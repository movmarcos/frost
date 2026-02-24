"""Tests for frost.lineage -- procedure lineage YAML sidecar scanning."""

import textwrap
from pathlib import Path

import pytest

from frost.lineage import LineageEntry, LineageScanner, merge_lineage_with_graph


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _write(base: Path, relative: str, content: str) -> Path:
    """Write a file under *base* and return its path."""
    p = base / relative
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


# ------------------------------------------------------------------
# LineageEntry dataclass
# ------------------------------------------------------------------

class TestLineageEntry:
    def test_defaults(self):
        e = LineageEntry(object_fqn="PUBLIC.MY_PROC", file_path="proc.sql")
        assert e.sources == []
        assert e.targets == []
        assert e.description == ""

    def test_with_values(self):
        e = LineageEntry(
            object_fqn="PUBLIC.MY_PROC",
            file_path="proc.sql",
            sources=["PUBLIC.T1", "PUBLIC.T2"],
            targets=["PUBLIC.SUMMARY"],
            description="Aggregates data",
        )
        assert len(e.sources) == 2
        assert len(e.targets) == 1
        assert e.description == "Aggregates data"


# ------------------------------------------------------------------
# LineageScanner._normalise_fqn
# ------------------------------------------------------------------

class TestNormaliseFqn:
    def test_upper_case(self):
        assert LineageScanner._normalise_fqn("public.orders") == "PUBLIC.ORDERS"

    def test_strips_whitespace(self):
        assert LineageScanner._normalise_fqn("  PUBLIC.T1  ") == "PUBLIC.T1"

    def test_mixed_case(self):
        assert LineageScanner._normalise_fqn("Public.My_Table") == "PUBLIC.MY_TABLE"


# ------------------------------------------------------------------
# LineageScanner._fqn_from_path
# ------------------------------------------------------------------

class TestFqnFromPath:
    def test_simple_stem(self):
        scanner = LineageScanner("/tmp/objects")
        assert scanner._fqn_from_path(Path("/tmp/objects/procedures/refresh_summary.sql")) == "REFRESH_SUMMARY"

    def test_lower_case_stem(self):
        scanner = LineageScanner("/tmp/objects")
        assert scanner._fqn_from_path(Path("my_proc.sql")) == "MY_PROC"


# ------------------------------------------------------------------
# LineageScanner.scan
# ------------------------------------------------------------------

class TestScan:
    def test_empty_folder(self, tmp_path):
        folder = tmp_path / "objects"
        folder.mkdir()
        scanner = LineageScanner(str(folder))
        assert scanner.scan() == []

    def test_missing_folder(self, tmp_path):
        scanner = LineageScanner(str(tmp_path / "nonexistent"))
        assert scanner.scan() == []

    def test_yml_without_sql_ignored(self, tmp_path):
        """A .yml with no matching .sql should be skipped."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/orphan.yml", """\
            sources:
              - PUBLIC.T1
        """)
        scanner = LineageScanner(str(folder))
        assert scanner.scan() == []

    def test_simple_sidecar(self, tmp_path):
        folder = tmp_path / "objects"
        _write(folder, "procedures/refresh.sql", """\
            CREATE OR ALTER PROCEDURE PUBLIC.REFRESH()
            RETURNS VARCHAR
            LANGUAGE SQL
            AS 'SELECT 1';
        """)
        _write(folder, "procedures/refresh.yml", """\
            sources:
              - PUBLIC.ORDERS
              - PUBLIC.CUSTOMERS
            targets:
              - PUBLIC.SUMMARY
            description: Aggregates stuff
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert len(entries) == 1

        e = entries[0]
        assert e.object_fqn == "REFRESH"
        assert e.sources == ["PUBLIC.ORDERS", "PUBLIC.CUSTOMERS"]
        assert e.targets == ["PUBLIC.SUMMARY"]
        assert e.description == "Aggregates stuff"
        assert e.file_path.endswith("refresh.sql")

    def test_yaml_extension(self, tmp_path):
        """Should also pick up .yaml files."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/proc.sql", "CREATE OR ALTER PROCEDURE P() RETURNS VARCHAR LANGUAGE SQL AS 'X';")
        _write(folder, "procedures/proc.yaml", """\
            sources:
              - PUBLIC.T1
            targets:
              - PUBLIC.T2
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert len(entries) == 1

    def test_empty_sources_and_targets_skipped(self, tmp_path):
        """A sidecar with neither sources nor targets should be ignored."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/noop.sql", "CREATE OR ALTER PROCEDURE NOOP() RETURNS VARCHAR LANGUAGE SQL AS '1';")
        _write(folder, "procedures/noop.yml", """\
            description: Does nothing
        """)
        scanner = LineageScanner(str(folder))
        assert scanner.scan() == []

    def test_empty_yaml_skipped(self, tmp_path):
        """An empty YAML file should be ignored."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/empty.sql", "SELECT 1;")
        (folder / "procedures" / "empty.yml").write_text("")
        scanner = LineageScanner(str(folder))
        assert scanner.scan() == []

    def test_sources_only(self, tmp_path):
        """Sidecar with only sources (no targets) is valid."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/reader.sql", "SELECT 1;")
        _write(folder, "procedures/reader.yml", """\
            sources:
              - PUBLIC.T1
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert len(entries) == 1
        assert entries[0].targets == []

    def test_targets_only(self, tmp_path):
        """Sidecar with only targets (no sources) is valid."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/writer.sql", "SELECT 1;")
        _write(folder, "procedures/writer.yml", """\
            targets:
              - PUBLIC.OUTPUT
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert len(entries) == 1
        assert entries[0].sources == []

    def test_multiple_sidecars(self, tmp_path):
        """Multiple sidecars should all be returned."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/a.sql", "SELECT 1;")
        _write(folder, "procedures/a.yml", """\
            sources:
              - PUBLIC.T1
        """)
        _write(folder, "procedures/b.sql", "SELECT 2;")
        _write(folder, "procedures/b.yml", """\
            targets:
              - PUBLIC.T2
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert len(entries) == 2

    def test_fqn_normalisation(self, tmp_path):
        """Source/target names should be upper-cased."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/x.sql", "SELECT 1;")
        _write(folder, "procedures/x.yml", """\
            sources:
              - public.orders
            targets:
              - mydb.myschema.summary
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert entries[0].sources == ["PUBLIC.ORDERS"]
        assert entries[0].targets == ["MYDB.MYSCHEMA.SUMMARY"]

    def test_nested_subfolder(self, tmp_path):
        """Sidecars in nested subfolders should be discovered."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/etl/load.sql", "SELECT 1;")
        _write(folder, "procedures/etl/load.yml", """\
            sources:
              - RAW.EVENTS
            targets:
              - ANALYTICS.FACT_EVENTS
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()
        assert len(entries) == 1
        assert entries[0].sources == ["RAW.EVENTS"]


# ------------------------------------------------------------------
# merge_lineage_with_graph
# ------------------------------------------------------------------

class TestMergeLineageWithGraph:
    def test_resolves_fqn(self):
        entry = LineageEntry(
            object_fqn="REFRESH_SUMMARY",
            file_path="/app/objects/procedures/refresh_summary.sql",
            sources=["PUBLIC.ORDERS"],
        )
        parsed_fqns = {
            "/app/objects/procedures/refresh_summary.sql": "DEV.PUBLIC.REFRESH_SUMMARY"
        }
        resolved = merge_lineage_with_graph([entry], parsed_fqns)
        assert len(resolved) == 1
        assert resolved[0].object_fqn == "DEV.PUBLIC.REFRESH_SUMMARY"

    def test_keeps_original_when_no_match(self):
        entry = LineageEntry(
            object_fqn="UNKNOWN_PROC",
            file_path="/app/objects/procedures/unknown.sql",
            sources=["PUBLIC.T1"],
        )
        resolved = merge_lineage_with_graph([entry], {})
        assert resolved[0].object_fqn == "UNKNOWN_PROC"

    def test_multiple_entries(self):
        entries = [
            LineageEntry(object_fqn="A", file_path="a.sql", sources=["T1"]),
            LineageEntry(object_fqn="B", file_path="b.sql", targets=["T2"]),
        ]
        parsed = {"a.sql": "DEV.PUBLIC.A"}
        resolved = merge_lineage_with_graph(entries, parsed)
        assert resolved[0].object_fqn == "DEV.PUBLIC.A"
        assert resolved[1].object_fqn == "B"  # not in parsed_fqns

    def test_empty_list(self):
        resolved = merge_lineage_with_graph([], {"a.sql": "X"})
        assert resolved == []
