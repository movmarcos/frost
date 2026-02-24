"""Tests for frost.lineage -- auto-detected and YAML-declared procedure lineage."""

import textwrap
from pathlib import Path

import pytest

from frost.lineage import (
    LineageEntry,
    LineageScanner,
    ProcedureBodyAnalyzer,
    merge_lineage_with_graph,
)
from frost.parser import ObjectDefinition


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _write(base: Path, relative: str, content: str) -> Path:
    """Write a file under *base* and return its path."""
    p = base / relative
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(content))
    return p


def _proc(fqn: str, sql: str, file_path: str = "proc.sql") -> ObjectDefinition:
    """Build a minimal PROCEDURE ObjectDefinition."""
    parts = fqn.split(".")
    name = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    db = parts[-3] if len(parts) >= 3 else None
    return ObjectDefinition(
        file_path=file_path,
        object_type="PROCEDURE",
        database=db,
        schema=schema,
        name=name,
        raw_sql=textwrap.dedent(sql),
        resolved_sql=textwrap.dedent(sql),
    )


def _func(fqn: str, sql: str, file_path: str = "func.sql") -> ObjectDefinition:
    """Build a minimal FUNCTION ObjectDefinition."""
    parts = fqn.split(".")
    name = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    db = parts[-3] if len(parts) >= 3 else None
    return ObjectDefinition(
        file_path=file_path,
        object_type="FUNCTION",
        database=db,
        schema=schema,
        name=name,
        raw_sql=textwrap.dedent(sql),
        resolved_sql=textwrap.dedent(sql),
    )


def _table(fqn: str, file_path: str = "table.sql") -> ObjectDefinition:
    """Build a minimal TABLE ObjectDefinition."""
    parts = fqn.split(".")
    name = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    db = parts[-3] if len(parts) >= 3 else None
    return ObjectDefinition(
        file_path=file_path,
        object_type="TABLE",
        database=db,
        schema=schema,
        name=name,
        raw_sql=f"CREATE OR ALTER TABLE {fqn} (id INT);",
        resolved_sql=f"CREATE OR ALTER TABLE {fqn} (id INT);",
    )


# ==================================================================
# LineageEntry dataclass
# ==================================================================

class TestLineageEntry:
    def test_defaults(self):
        e = LineageEntry(object_fqn="PUBLIC.MY_PROC", file_path="proc.sql")
        assert e.sources == []
        assert e.targets == []
        assert e.description == ""
        assert e.auto_detected is False

    def test_with_values(self):
        e = LineageEntry(
            object_fqn="PUBLIC.MY_PROC",
            file_path="proc.sql",
            sources=["PUBLIC.T1", "PUBLIC.T2"],
            targets=["PUBLIC.SUMMARY"],
            description="Aggregates data",
            auto_detected=True,
        )
        assert len(e.sources) == 2
        assert len(e.targets) == 1
        assert e.auto_detected is True


# ==================================================================
# ProcedureBodyAnalyzer -- body extraction
# ==================================================================

class TestExtractBody:
    analyzer = ProcedureBodyAnalyzer()

    def test_dollar_quoted(self):
        sql = """
        CREATE OR REPLACE PROCEDURE P()
        RETURNS VARCHAR LANGUAGE SQL AS
        $$
        INSERT INTO PUBLIC.T1 SELECT * FROM PUBLIC.T2;
        $$;
        """
        body = self.analyzer._extract_body(sql)
        assert body is not None
        assert "INSERT INTO PUBLIC.T1" in body

    def test_dollar_tagged(self):
        sql = """
        CREATE OR REPLACE PROCEDURE P()
        RETURNS VARCHAR LANGUAGE SQL AS
        $body$
        INSERT INTO PUBLIC.T1 SELECT * FROM PUBLIC.T2;
        $body$;
        """
        body = self.analyzer._extract_body(sql)
        assert body is not None
        assert "INSERT INTO PUBLIC.T1" in body

    def test_single_quoted(self):
        sql = """
        CREATE OR REPLACE PROCEDURE P()
        RETURNS VARCHAR LANGUAGE SQL
        AS 'INSERT INTO PUBLIC.T1 SELECT * FROM PUBLIC.T2';
        """
        body = self.analyzer._extract_body(sql)
        assert body is not None
        assert "INSERT INTO PUBLIC.T1" in body

    def test_begin_end(self):
        sql = """
        CREATE OR ALTER PROCEDURE P()
        RETURNS VARCHAR LANGUAGE SQL
        AS
        BEGIN
            INSERT INTO PUBLIC.T1 SELECT * FROM PUBLIC.T2;
        END;
        """
        body = self.analyzer._extract_body(sql)
        assert body is not None
        assert "INSERT INTO PUBLIC.T1" in body

    def test_no_body(self):
        sql = "CREATE OR ALTER TABLE PUBLIC.T1 (id INT);"
        body = self.analyzer._extract_body(sql)
        assert body is None

    def test_escaped_single_quotes(self):
        sql = "CREATE OR REPLACE PROCEDURE P() RETURNS VARCHAR LANGUAGE SQL AS 'SELECT ''hello'' FROM PUBLIC.T1';"
        body = self.analyzer._extract_body(sql)
        assert body is not None
        assert "PUBLIC.T1" in body


# ==================================================================
# ProcedureBodyAnalyzer -- analyze()
# ==================================================================

class TestAnalyze:
    analyzer = ProcedureBodyAnalyzer()

    def test_skips_non_procedures(self):
        obj = _table("PUBLIC.T1")
        assert self.analyzer.analyze(obj) is None

    def test_detects_insert_and_from(self):
        obj = _proc("PUBLIC.MY_PROC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.MY_PROC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.SUMMARY
            SELECT * FROM PUBLIC.ORDERS o
            JOIN PUBLIC.CUSTOMERS c ON o.cid = c.id;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert entry.auto_detected is True
        assert "PUBLIC.ORDERS" in entry.sources
        assert "PUBLIC.CUSTOMERS" in entry.sources
        assert "PUBLIC.SUMMARY" in entry.targets

    def test_detects_update(self):
        obj = _proc("PUBLIC.UPD", """
            CREATE OR REPLACE PROCEDURE PUBLIC.UPD()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            UPDATE PUBLIC.ORDERS SET status = 'DONE'
            WHERE id IN (SELECT id FROM PUBLIC.PENDING);
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.ORDERS" in entry.targets
        assert "PUBLIC.PENDING" in entry.sources

    def test_detects_delete(self):
        obj = _proc("PUBLIC.DEL", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DEL()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            DELETE FROM PUBLIC.STAGING WHERE processed = TRUE;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.STAGING" in entry.targets

    def test_detects_merge(self):
        obj = _proc("PUBLIC.MRG", """
            CREATE OR REPLACE PROCEDURE PUBLIC.MRG()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            MERGE INTO PUBLIC.TARGET t
            USING PUBLIC.SOURCE s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val);
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.TARGET" in entry.targets
        # USING is matched by FROM pattern after MERGE body analysis
        # The source table is detected via JOIN/FROM-like references

    def test_detects_truncate(self):
        obj = _proc("PUBLIC.TRUNC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.TRUNC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            TRUNCATE TABLE PUBLIC.OLD_DATA;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.OLD_DATA" in entry.targets

    def test_detects_ctas(self):
        obj = _proc("PUBLIC.CTAS_PROC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.CTAS_PROC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            CREATE TABLE PUBLIC.SNAPSHOT AS
            SELECT * FROM PUBLIC.LIVE_DATA;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.SNAPSHOT" in entry.targets
        assert "PUBLIC.LIVE_DATA" in entry.sources

    def test_detects_copy_into(self):
        obj = _proc("PUBLIC.COPY_PROC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.COPY_PROC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            COPY INTO PUBLIC.RAW_DATA
            FROM @PUBLIC.MY_STAGE/data.csv;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.RAW_DATA" in entry.targets

    def test_three_part_names(self):
        obj = _proc("DB.SCHEMA.PROC", """
            CREATE OR REPLACE PROCEDURE DB.SCHEMA.PROC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO DB.ANALYTICS.SUMMARY
            SELECT * FROM DB.RAW.EVENTS;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "DB.ANALYTICS.SUMMARY" in entry.targets
        assert "DB.RAW.EVENTS" in entry.sources

    def test_no_refs_returns_none(self):
        obj = _proc("PUBLIC.NOOP", """
            CREATE OR REPLACE PROCEDURE PUBLIC.NOOP()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            RETURN 'OK';
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is None

    def test_skips_own_fqn(self):
        """Auto-detection should not list the procedure itself as a source/target."""
        obj = _proc("PUBLIC.SELF_REF", """
            CREATE OR REPLACE PROCEDURE PUBLIC.SELF_REF()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.SELF_REF SELECT * FROM PUBLIC.T1;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.SELF_REF" not in entry.sources
        assert "PUBLIC.SELF_REF" not in entry.targets
        assert "PUBLIC.T1" in entry.sources

    def test_functions_analysed(self):
        obj = _func("PUBLIC.MY_FUNC", """
            CREATE OR REPLACE FUNCTION PUBLIC.MY_FUNC(x INT)
            RETURNS INT LANGUAGE SQL AS
            $$
            SELECT COUNT(*) FROM PUBLIC.EVENTS WHERE id = x
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.EVENTS" in entry.sources
        assert entry.targets == []

    def test_comments_in_body_ignored(self):
        obj = _proc("PUBLIC.CMT", """
            CREATE OR REPLACE PROCEDURE PUBLIC.CMT()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            -- SELECT * FROM PUBLIC.COMMENTED_OUT;
            /* INSERT INTO PUBLIC.ALSO_COMMENTED; */
            SELECT * FROM PUBLIC.REAL_TABLE;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.REAL_TABLE" in entry.sources
        assert "PUBLIC.COMMENTED_OUT" not in entry.sources
        assert "PUBLIC.ALSO_COMMENTED" not in entry.targets

    def test_deduplication(self):
        """Same table referenced twice should appear once."""
        obj = _proc("PUBLIC.DUP", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DUP()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.OUT SELECT * FROM PUBLIC.SRC;
            INSERT INTO PUBLIC.OUT SELECT * FROM PUBLIC.SRC;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert entry.sources.count("PUBLIC.SRC") == 1
        assert entry.targets.count("PUBLIC.OUT") == 1

    def test_begin_end_body(self):
        obj = _proc("PUBLIC.SCRIPTING", """
            CREATE OR ALTER PROCEDURE PUBLIC.SCRIPTING()
            RETURNS VARCHAR LANGUAGE SQL
            AS
            BEGIN
                INSERT INTO PUBLIC.DEST SELECT * FROM PUBLIC.SRC;
                RETURN 'done';
            END;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.SRC" in entry.sources
        assert "PUBLIC.DEST" in entry.targets

    def test_insert_overwrite(self):
        obj = _proc("PUBLIC.OVERWRITE", """
            CREATE OR REPLACE PROCEDURE PUBLIC.OVERWRITE()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT OVERWRITE INTO PUBLIC.TARGET
            SELECT * FROM PUBLIC.SOURCE;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.TARGET" in entry.targets
        assert "PUBLIC.SOURCE" in entry.sources

    def test_multiple_joins(self):
        obj = _proc("PUBLIC.MULTI", """
            CREATE OR REPLACE PROCEDURE PUBLIC.MULTI()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.RESULT
            SELECT a.*, b.val, c.name
            FROM PUBLIC.TABLE_A a
            JOIN PUBLIC.TABLE_B b ON a.id = b.id
            LEFT JOIN PUBLIC.TABLE_C c ON a.id = c.id;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.TABLE_A" in entry.sources
        assert "PUBLIC.TABLE_B" in entry.sources
        assert "PUBLIC.TABLE_C" in entry.sources
        assert "PUBLIC.RESULT" in entry.targets


# ==================================================================
# Dynamic SQL detection -- analyzer should skip these
# ==================================================================

class TestDynamicSqlSkip:
    """When a procedure body contains dynamic SQL markers, auto-detection
    should return None because regex matching is unreliable."""

    analyzer = ProcedureBodyAnalyzer()

    def test_execute_immediate(self):
        obj = _proc("PUBLIC.DYN", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DYN()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            LET sql := 'INSERT INTO PUBLIC.TARGET SELECT * FROM PUBLIC.SOURCE';
            EXECUTE IMMEDIATE :sql;
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_identifier_function(self):
        obj = _proc("PUBLIC.DYN2", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DYN2(TBL VARCHAR)
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO IDENTIFIER(:TBL) SELECT * FROM PUBLIC.SOURCE;
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_resultset_cursor(self):
        obj = _proc("PUBLIC.DYN3", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DYN3()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            LET rs RESULTSET := (SELECT col FROM PUBLIC.CONFIG);
            LET cur CURSOR FOR rs;
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_string_concat_query(self):
        obj = _proc("PUBLIC.DYN4", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DYN4(SCHEMA_NAME VARCHAR)
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            LET sql := 'SELECT * FROM ' || :SCHEMA_NAME || '.MY_TABLE';
            EXECUTE IMMEDIATE :sql;
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_system_query_reference(self):
        obj = _proc("PUBLIC.DYN5", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DYN5()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            LET ref := SYSTEM$QUERY_REFERENCE('SELECT 1');
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_static_sql_not_skipped(self):
        """Procedures with only static SQL should still be analysed."""
        obj = _proc("PUBLIC.STATIC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.STATIC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.OUT SELECT * FROM PUBLIC.IN_TABLE;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.OUT" in entry.targets

    def test_yaml_still_works_for_dynamic(self, tmp_path):
        """When auto-detect skips a dynamic proc, a YAML sidecar should
        still provide lineage."""
        folder = tmp_path / "objects"
        sql_path = _write(folder, "procedures/dyn.sql", """
            CREATE OR REPLACE PROCEDURE PUBLIC.DYN()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            EXECUTE IMMEDIATE 'INSERT INTO PUBLIC.TARGET SELECT 1';
            $$;
        """)

        _write(folder, "procedures/dyn.yml", """\
            sources:
              - PUBLIC.CONFIG
            targets:
              - PUBLIC.TARGET
        """)

        obj = _proc("PUBLIC.DYN", sql_path.read_text(), file_path=str(sql_path))
        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.DYN": obj})
        assert len(entries) == 1
        assert entries[0].auto_detected is False
        assert "PUBLIC.CONFIG" in entries[0].sources
        assert "PUBLIC.TARGET" in entries[0].targets


# ==================================================================
# LineageScanner -- YAML sidecar scanning (still supported)
# ==================================================================

class TestYamlSidecars:
    def test_empty_folder(self, tmp_path):
        folder = tmp_path / "objects"
        folder.mkdir()
        scanner = LineageScanner(str(folder))
        assert scanner.scan() == []

    def test_missing_folder(self, tmp_path):
        scanner = LineageScanner(str(tmp_path / "nonexistent"))
        assert scanner.scan() == []

    def test_yml_without_sql_ignored(self, tmp_path):
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
            RETURNS VARCHAR LANGUAGE SQL AS 'SELECT 1';
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
        assert e.sources == ["PUBLIC.ORDERS", "PUBLIC.CUSTOMERS"]
        assert e.targets == ["PUBLIC.SUMMARY"]
        assert e.description == "Aggregates stuff"
        assert e.auto_detected is False

    def test_yaml_extension(self, tmp_path):
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
        folder = tmp_path / "objects"
        _write(folder, "procedures/noop.sql", "CREATE OR ALTER PROCEDURE NOOP() RETURNS VARCHAR LANGUAGE SQL AS '1';")
        _write(folder, "procedures/noop.yml", """\
            description: Does nothing
        """)
        scanner = LineageScanner(str(folder))
        assert scanner.scan() == []

    def test_fqn_normalisation(self, tmp_path):
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


# ==================================================================
# LineageScanner -- auto-detect + YAML override
# ==================================================================

class TestAutoDetectWithOverride:
    def test_auto_detect_from_objects(self, tmp_path):
        """Scanner should auto-detect from parsed ObjectDefinitions."""
        obj = _proc("PUBLIC.MY_PROC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.MY_PROC()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.OUT SELECT * FROM PUBLIC.IN;
            $$;
        """, file_path=str(tmp_path / "proc.sql"))

        folder = tmp_path / "objects"
        folder.mkdir()
        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.MY_PROC": obj})
        assert len(entries) == 1
        assert entries[0].auto_detected is True
        assert "PUBLIC.IN" in entries[0].sources
        assert "PUBLIC.OUT" in entries[0].targets

    def test_yaml_overrides_auto_detected_sources(self, tmp_path):
        """YAML sources should replace auto-detected sources."""
        folder = tmp_path / "objects"
        sql_path = _write(folder, "procedures/p.sql", """
            CREATE OR REPLACE PROCEDURE PUBLIC.P()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.OUT SELECT * FROM PUBLIC.AUTO_SRC;
            $$;
        """)

        _write(folder, "procedures/p.yml", """\
            sources:
              - PUBLIC.MANUAL_SRC
        """)

        obj = _proc("PUBLIC.P", sql_path.read_text(), file_path=str(sql_path))
        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.P": obj})

        assert len(entries) == 1
        e = entries[0]
        # Sources overridden by YAML
        assert e.sources == ["PUBLIC.MANUAL_SRC"]
        # Targets kept from auto-detection
        assert "PUBLIC.OUT" in e.targets
        assert e.auto_detected is False

    def test_yaml_overrides_auto_detected_targets(self, tmp_path):
        """YAML targets should replace auto-detected targets."""
        folder = tmp_path / "objects"
        sql_path = _write(folder, "procedures/q.sql", """
            CREATE OR REPLACE PROCEDURE PUBLIC.Q()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.AUTO_TGT SELECT * FROM PUBLIC.SRC;
            $$;
        """)

        _write(folder, "procedures/q.yml", """\
            targets:
              - PUBLIC.MANUAL_TGT
        """)

        obj = _proc("PUBLIC.Q", sql_path.read_text(), file_path=str(sql_path))
        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.Q": obj})

        assert len(entries) == 1
        e = entries[0]
        assert "PUBLIC.SRC" in e.sources  # kept from auto-detect
        assert e.targets == ["PUBLIC.MANUAL_TGT"]  # overridden

    def test_yaml_adds_description_to_auto(self, tmp_path):
        """YAML description supplements auto-detected lineage."""
        folder = tmp_path / "objects"
        sql_path = _write(folder, "procedures/r.sql", """
            CREATE OR REPLACE PROCEDURE PUBLIC.R()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.OUT SELECT * FROM PUBLIC.IN;
            $$;
        """)

        _write(folder, "procedures/r.yml", """\
            description: My manual description
        """)

        obj = _proc("PUBLIC.R", sql_path.read_text(), file_path=str(sql_path))
        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.R": obj})

        # YAML without sources/targets does NOT create an override entry,
        # so auto-detection stands alone
        assert len(entries) == 1
        assert entries[0].auto_detected is True

    def test_yaml_only_no_objects(self, tmp_path):
        """When no parsed_objects, only YAML sidecars are used."""
        folder = tmp_path / "objects"
        _write(folder, "procedures/s.sql", "SELECT 1;")
        _write(folder, "procedures/s.yml", """\
            sources:
              - PUBLIC.T1
        """)
        scanner = LineageScanner(str(folder))
        entries = scanner.scan()  # no parsed_objects
        assert len(entries) == 1
        assert entries[0].auto_detected is False

    def test_auto_skips_tables(self, tmp_path):
        """TABLE objects should not be analysed for lineage."""
        folder = tmp_path / "objects"
        folder.mkdir()
        obj = _table("PUBLIC.T1", file_path=str(tmp_path / "t1.sql"))
        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.T1": obj})
        assert entries == []

    def test_mixed_auto_and_yaml(self, tmp_path):
        """Auto-detected procs + separate YAML-only procs."""
        folder = tmp_path / "objects"

        # Proc 1: auto-detected (no YAML)
        auto_sql = _write(folder, "procedures/auto.sql", """
            CREATE OR REPLACE PROCEDURE PUBLIC.AUTO()
            RETURNS VARCHAR LANGUAGE SQL AS
            $$
            INSERT INTO PUBLIC.OUT1 SELECT * FROM PUBLIC.IN1;
            $$;
        """)
        auto_obj = _proc("PUBLIC.AUTO", auto_sql.read_text(), file_path=str(auto_sql))

        # Proc 2: YAML only (SQL has no detectable patterns)
        yaml_sql = _write(folder, "procedures/manual.sql", """
            CREATE OR REPLACE PROCEDURE PUBLIC.MANUAL()
            RETURNS VARCHAR LANGUAGE JAVASCRIPT AS
            $$
            var x = 1;
            $$;
        """)
        _write(folder, "procedures/manual.yml", """\
            sources:
              - PUBLIC.IN2
            targets:
              - PUBLIC.OUT2
        """)

        scanner = LineageScanner(str(folder))
        entries = scanner.scan(parsed_objects={"PUBLIC.AUTO": auto_obj})

        assert len(entries) == 2
        fqns = {e.object_fqn for e in entries}
        auto_flags = {e.object_fqn: e.auto_detected for e in entries}
        assert "PUBLIC.AUTO" in fqns or "AUTO" in fqns  # auto obj has FQN
        # YAML entry has stem-based FQN
        yaml_entry = [e for e in entries if not e.auto_detected][0]
        assert "PUBLIC.IN2" in yaml_entry.sources


# ==================================================================
# merge_lineage_with_graph
# ==================================================================

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
        assert resolved[1].object_fqn == "B"

    def test_empty_list(self):
        resolved = merge_lineage_with_graph([], {"a.sql": "X"})
        assert resolved == []


# ==================================================================
# _normalise_fqn
# ==================================================================

class TestNormaliseFqn:
    def test_upper_case(self):
        assert LineageScanner._normalise_fqn("public.orders") == "PUBLIC.ORDERS"

    def test_strips_whitespace(self):
        assert LineageScanner._normalise_fqn("  PUBLIC.T1  ") == "PUBLIC.T1"

    def test_mixed_case(self):
        assert LineageScanner._normalise_fqn("Public.My_Table") == "PUBLIC.MY_TABLE"


# ==================================================================
# _fqn_from_path
# ==================================================================

class TestFqnFromPath:
    def test_simple_stem(self):
        scanner = LineageScanner("/tmp/objects")
        assert scanner._fqn_from_path(Path("/tmp/objects/procedures/refresh.sql")) == "REFRESH"

    def test_lower_case_stem(self):
        scanner = LineageScanner("/tmp/objects")
        assert scanner._fqn_from_path(Path("my_proc.sql")) == "MY_PROC"
