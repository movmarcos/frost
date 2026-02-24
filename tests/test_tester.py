"""Tests for frost.tester – YAML-driven CSV data-quality testing."""

import shutil
import textwrap
from pathlib import Path

import pytest

from frost.tester import DataTester, TestCase, TestResult, _load_csv
from helpers import DATA_DIR


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture()
def data_dir():
    """Return the tests/data directory that contains tester_* CSV fixtures."""
    return DATA_DIR


@pytest.fixture()
def tester(data_dir):
    """A DataTester pointed at tests/data."""
    return DataTester(data_folder=str(data_dir))


def _make_data_dir(tmp_path: Path, csvs: dict[str, str] | None = None) -> Path:
    """Create a temp data folder, copying tester CSVs from tests/data."""
    d = tmp_path / "data"
    d.mkdir()
    # Copy all tester_* CSVs into the temp data folder
    for csv_file in DATA_DIR.glob("tester_*.csv"):
        shutil.copy2(csv_file, d / csv_file.name)
    if csvs:
        for name, content in csvs.items():
            (d / name).write_text(content, encoding="utf-8")
    return d


def _write_sidecar(folder: Path, stem: str, content: str) -> Path:
    """Write a YAML sidecar into *folder* as <stem>.yml."""
    p = folder / f"{stem}.yml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return p


def _case(**kw) -> TestCase:
    """Shorthand factory for TestCase with sensible defaults."""
    defaults = dict(name="t", source="tester_users.csv", test="unique")
    defaults.update(kw)
    return TestCase(**defaults)


# =====================================================================
# _load_csv helper
# =====================================================================


class TestLoadCsv:
    def test_returns_columns_and_rows(self, data_dir):
        cols, rows = _load_csv(data_dir / "tester_users.csv")
        assert cols == ["id", "name", "status"]
        assert len(rows) == 3

    def test_file_not_found(self, data_dir):
        with pytest.raises(FileNotFoundError):
            _load_csv(data_dir / "no_such_file.csv")


# =====================================================================
# YAML loading
# =====================================================================


class TestLoadTests:
    def test_loads_basic_yaml(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: t1
            column: id
            test: unique
          - name: t2
            column: name
            test: not_null
        """)
        dt = DataTester(data_folder=str(d))
        cases = dt.load_tests()
        # should find tests from tester_users.yml
        assert any(c.name == "t1" for c in cases)
        assert any(c.name == "t2" and c.test == "not_null" for c in cases)

    def test_source_defaults_to_csv(self, tmp_path):
        """When source is omitted it should default to <stem>.csv."""
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: auto_src
            column: id
            test: unique
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        cases = dt.load_tests()
        assert cases[0].source == "tester_users.csv"

    def test_source_without_extension(self, tmp_path):
        """source: tester_users  should resolve to tester_users.csv."""
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: bare_src
            source: tester_users
            column: id
            test: unique
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        cases = dt.load_tests()
        assert cases[0].source == "tester_users.csv"

    def test_target_filter(self, tmp_path):
        """Only load tests from the targeted YAML."""
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: from_users
            column: id
            test: unique
        """)
        _write_sidecar(d, "tester_orders", """\
        tests:
          - name: from_orders
            column: order_id
            test: unique
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        cases = dt.load_tests()
        assert len(cases) == 1
        assert cases[0].name == "from_users"

    def test_missing_target_returns_empty(self, tmp_path):
        d = _make_data_dir(tmp_path)
        dt = DataTester(data_folder=str(d), target="nonexistent")
        assert dt.load_tests() == []

    def test_no_tests_key_skipped(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        columns:
          id: NUMBER
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        assert dt.load_tests() == []

    def test_accepted_values_parsed(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: av
            column: status
            test: accepted_values
            values: [ACTIVE, INACTIVE]
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        tc = dt.load_tests()[0]
        assert tc.values == ["ACTIVE", "INACTIVE"]

    def test_row_count_fields(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: rc
            test: row_count
            min: 1
            max: 100
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        tc = dt.load_tests()[0]
        assert tc.min == 1
        assert tc.max == 100

    def test_relationship_to_without_extension(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_orders", """\
        tests:
          - name: rel
            column: customer_id
            test: relationship
            to: tester_customers
            to_column: id
        """)
        dt = DataTester(data_folder=str(d), target="tester_orders")
        tc = dt.load_tests()[0]
        assert tc.to == "tester_customers.csv"
        assert tc.to_column == "id"

    def test_expression_field(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: ex
            test: expression
            expression: "int(id) > 0"
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        tc = dt.load_tests()[0]
        assert tc.expression == "int(id) > 0"

    def test_missing_data_folder_returns_empty(self, tmp_path):
        dt = DataTester(data_folder=str(tmp_path / "no_such_dir"))
        assert dt.load_tests() == []


# =====================================================================
# unique test
# =====================================================================


class TestUnique:
    def test_pass(self, tester):
        r = tester.run([_case(test="unique", column="id")])
        assert r[0].passed is True

    def test_fail_duplicates(self, tester):
        r = tester.run([_case(source="tester_users_dupes.csv", test="unique", column="id")])
        assert r[0].passed is False
        assert "duplicate" in r[0].message.lower()
        assert len(r[0].failing_rows) >= 1

    def test_missing_column(self, tester):
        r = tester.run([_case(test="unique", column="nonexistent")])
        assert r[0].passed is False
        assert "not found" in r[0].message.lower()

    def test_no_column_raises(self, tester):
        r = tester.run([_case(test="unique", column=None)])
        assert r[0].passed is False
        assert "requires" in r[0].message.lower()


# =====================================================================
# not_null test
# =====================================================================


class TestNotNull:
    def test_pass(self, tester):
        r = tester.run([_case(test="not_null", column="id")])
        assert r[0].passed is True

    def test_fail_empty(self, tester):
        r = tester.run([_case(source="tester_users_nulls.csv", test="not_null", column="id")])
        assert r[0].passed is False
        assert "null" in r[0].message.lower() or "empty" in r[0].message.lower()

    def test_fail_null_string(self, tester):
        r = tester.run([_case(source="tester_users_nulls.csv", test="not_null", column="status")])
        assert r[0].passed is False

    def test_fail_empty_name(self, tester):
        r = tester.run([_case(source="tester_users_nulls.csv", test="not_null", column="name")])
        assert r[0].passed is False

    def test_missing_column(self, tester):
        r = tester.run([_case(test="not_null", column="zzz")])
        assert r[0].passed is False


# =====================================================================
# accepted_values test
# =====================================================================


class TestAcceptedValues:
    def test_pass(self, tester):
        r = tester.run([_case(test="accepted_values", column="status",
                               values=["ACTIVE", "INACTIVE"])])
        assert r[0].passed is True

    def test_fail(self, tester):
        r = tester.run([_case(test="accepted_values", column="status",
                               values=["ACTIVE"])])
        assert r[0].passed is False
        assert "outside accepted" in r[0].message.lower() or "not in" in r[0].message.lower()

    def test_no_values_list(self, tester):
        r = tester.run([_case(test="accepted_values", column="status", values=None)])
        assert r[0].passed is False
        assert "requires" in r[0].message.lower()

    def test_missing_column(self, tester):
        r = tester.run([_case(test="accepted_values", column="nope",
                               values=["A"])])
        assert r[0].passed is False


# =====================================================================
# row_count test
# =====================================================================


class TestRowCount:
    def test_pass_no_bounds(self, tester):
        r = tester.run([_case(test="row_count")])
        assert r[0].passed is True

    def test_pass_min(self, tester):
        tc = _case(test="row_count")
        tc.min = 1
        r = tester.run([tc])
        assert r[0].passed is True

    def test_fail_min(self, tester):
        tc = _case(test="row_count")
        tc.min = 999
        r = tester.run([tc])
        assert r[0].passed is False
        assert "below" in r[0].message.lower()

    def test_pass_max(self, tester):
        tc = _case(test="row_count")
        tc.max = 100
        r = tester.run([tc])
        assert r[0].passed is True

    def test_fail_max(self, tester):
        tc = _case(test="row_count")
        tc.max = 1
        r = tester.run([tc])
        assert r[0].passed is False
        assert "exceeds" in r[0].message.lower()

    def test_empty_file(self, tester):
        tc = _case(source="tester_empty.csv", test="row_count")
        tc.min = 1
        r = tester.run([tc])
        assert r[0].passed is False


# =====================================================================
# relationship test
# =====================================================================


class TestRelationship:
    def test_pass(self, tester):
        tc = _case(source="tester_orders.csv", test="relationship",
                    column="customer_id")
        tc.to = "tester_customers.csv"
        tc.to_column = "id"
        # orders have customer_id 1,2,3,99  -- 99 is an orphan
        r = tester.run([tc])
        assert r[0].passed is False
        assert "orphan" in r[0].message.lower()

    def test_all_match(self, tester):
        # use customers referencing themselves (id -> id always matches)
        tc = _case(source="tester_customers.csv", test="relationship",
                    column="id")
        tc.to = "tester_customers.csv"
        tc.to_column = "id"
        r = tester.run([tc])
        assert r[0].passed is True

    def test_missing_to(self, tester):
        tc = _case(test="relationship", column="id")
        tc.to = None
        tc.to_column = None
        r = tester.run([tc])
        assert r[0].passed is False
        assert "requires" in r[0].message.lower()

    def test_target_file_missing(self, tester):
        tc = _case(test="relationship", column="id")
        tc.to = "no_such.csv"
        tc.to_column = "id"
        r = tester.run([tc])
        assert r[0].passed is False

    def test_source_column_missing(self, tester):
        tc = _case(source="tester_orders.csv", test="relationship",
                    column="nope")
        tc.to = "tester_customers.csv"
        tc.to_column = "id"
        r = tester.run([tc])
        assert r[0].passed is False

    def test_target_column_missing(self, tester):
        tc = _case(source="tester_orders.csv", test="relationship",
                    column="customer_id")
        tc.to = "tester_customers.csv"
        tc.to_column = "nope"
        r = tester.run([tc])
        assert r[0].passed is False


# =====================================================================
# expression test
# =====================================================================


class TestExpression:
    def test_pass(self, tester):
        tc = _case(test="expression")
        tc.expression = "int(id) > 0"
        r = tester.run([tc])
        assert r[0].passed is True

    def test_fail(self, tester):
        tc = _case(test="expression")
        tc.expression = "int(id) > 2"
        r = tester.run([tc])
        assert r[0].passed is False
        assert "failed expression" in r[0].message.lower()

    def test_no_expression(self, tester):
        tc = _case(test="expression")
        tc.expression = None
        r = tester.run([tc])
        assert r[0].passed is False
        assert "requires" in r[0].message.lower()

    def test_bad_expression(self, tester):
        tc = _case(test="expression")
        tc.expression = "import os"
        r = tester.run([tc])
        assert r[0].passed is False


# =====================================================================
# Unknown test type
# =====================================================================


class TestUnknownType:
    def test_unknown(self, tester):
        r = tester.run([_case(test="banana")])
        assert r[0].passed is False
        assert "unknown" in r[0].message.lower()


# =====================================================================
# CSV caching
# =====================================================================


class TestCaching:
    def test_csv_cached(self, tester):
        tester._get_csv("tester_users.csv")
        assert "tester_users.csv" in tester._csv_cache
        # second call returns same object
        a = tester._get_csv("tester_users.csv")
        b = tester._get_csv("tester_users.csv")
        assert a is b

    def test_csv_not_found(self, tester):
        with pytest.raises(FileNotFoundError):
            tester._get_csv("no_such.csv")


# =====================================================================
# Source file not found
# =====================================================================


class TestMissingSourceFile:
    def test_unique_missing_file(self, tester):
        r = tester.run([_case(source="missing.csv", test="unique", column="id")])
        assert r[0].passed is False
        assert "error" in r[0].message.lower() or "not found" in r[0].message.lower()


# =====================================================================
# Integration: run full YAML config
# =====================================================================


class TestIntegration:
    def test_full_yaml_run(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: id_unique
            column: id
            test: unique
          - name: id_not_null
            column: id
            test: not_null
          - name: status_values
            column: status
            test: accepted_values
            values: [ACTIVE, INACTIVE]
          - name: has_rows
            test: row_count
            min: 1
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        results = dt.run()
        assert len(results) == 4
        assert all(r.passed for r in results)

    def test_mixed_pass_fail(self, tmp_path):
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: ok_test
            column: id
            test: unique
          - name: bad_test
            column: status
            test: accepted_values
            values: [ACTIVE]
        """)
        dt = DataTester(data_folder=str(d), target="tester_users")
        results = dt.run()
        assert results[0].passed is True
        assert results[1].passed is False

    def test_discover_all_files(self, tmp_path):
        """When no target is set, tests from all YAMLs are collected."""
        d = _make_data_dir(tmp_path)
        _write_sidecar(d, "tester_users", """\
        tests:
          - name: from_users
            column: id
            test: unique
        """)
        _write_sidecar(d, "tester_orders", """\
        tests:
          - name: from_orders
            column: order_id
            test: unique
        """)
        dt = DataTester(data_folder=str(d))
        results = dt.run()
        names = {r.test_case.name for r in results}
        assert "from_users" in names
        assert "from_orders" in names


# =====================================================================
# _ensure_csv_ext
# =====================================================================


class TestEnsureCsvExt:
    def test_bare_name(self):
        assert DataTester._ensure_csv_ext("countries") == "countries.csv"

    def test_already_csv(self):
        assert DataTester._ensure_csv_ext("countries.csv") == "countries.csv"

    def test_other_extension(self):
        assert DataTester._ensure_csv_ext("countries.tsv") == "countries.tsv"


# =====================================================================
# validate_unique_basenames
# =====================================================================


class TestValidateUniqueBasenames:
    def test_no_duplicates(self, tmp_path):
        d = _make_data_dir(tmp_path)
        dt = DataTester(data_folder=str(d))
        assert dt.validate_unique_basenames() == []

    def test_duplicate_csv(self, tmp_path):
        """Two .csv files with same stem should fail."""
        d = tmp_path / "dup"
        d.mkdir()
        (d / "foo.csv").write_text("a\n1\n")
        (d / "foo.tsv").write_text("a\n1\n")
        dt = DataTester(data_folder=str(d))
        errors = dt.validate_unique_basenames()
        assert len(errors) == 1
        assert "foo" in errors[0]

    def test_csv_yml_pair_is_ok(self, tmp_path):
        """A .csv + .yml pair with the same stem is expected."""
        d = tmp_path / "ok"
        d.mkdir()
        (d / "foo.csv").write_text("a\n1\n")
        (d / "foo.yml").write_text("columns:\n  a: NUMBER\n")
        dt = DataTester(data_folder=str(d))
        assert dt.validate_unique_basenames() == []

    def test_missing_folder(self, tmp_path):
        dt = DataTester(data_folder=str(tmp_path / "nope"))
        errors = dt.validate_unique_basenames()
        assert len(errors) == 1
        assert "not found" in errors[0].lower()


# =====================================================================
# report_test_results
# =====================================================================


class TestReportTestResults:
    def test_all_pass(self, tester):
        from frost.reporter import report_test_results

        results = tester.run([
            _case(test="unique", column="id"),
            _case(test="not_null", column="name"),
        ])
        report = report_test_results(results)
        assert "ALL PASSED" in report
        assert "PASS" in report

    def test_with_failures(self, tester):
        from frost.reporter import report_test_results

        results = tester.run([
            _case(test="unique", column="id"),
            _case(source="tester_users_dupes.csv", test="unique", column="id"),
        ])
        report = report_test_results(results)
        assert "FAIL" in report
        assert "FAILURES" in report

    def test_empty_results(self):
        from frost.reporter import report_test_results

        report = report_test_results([])
        assert "ALL PASSED" in report
        assert "Total tests:" in report
