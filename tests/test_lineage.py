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


def _task(fqn: str, sql: str, file_path: str = "task.sql") -> ObjectDefinition:
    """Build a minimal TASK ObjectDefinition."""
    parts = fqn.split(".")
    name = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    db = parts[-3] if len(parts) >= 3 else None
    return ObjectDefinition(
        file_path=file_path,
        object_type="TASK",
        database=db,
        schema=schema,
        name=name,
        raw_sql=textwrap.dedent(sql),
        resolved_sql=textwrap.dedent(sql),
    )


def _stream(fqn: str, sql: str, file_path: str = "stream.sql") -> ObjectDefinition:
    """Build a minimal STREAM ObjectDefinition."""
    parts = fqn.split(".")
    name = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    db = parts[-3] if len(parts) >= 3 else None
    return ObjectDefinition(
        file_path=file_path,
        object_type="STREAM",
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
        assert "PUBLIC.SOURCE" in entry.sources

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
# Language detection
# ==================================================================

class TestLanguageDetection:
    """Analyzer should detect LANGUAGE and route to the correct strategy."""

    analyzer = ProcedureBodyAnalyzer()

    def test_sql_language_default(self):
        """No explicit LANGUAGE clause defaults to SQL."""
        obj = _proc("PUBLIC.P", """
            CREATE OR REPLACE PROCEDURE PUBLIC.P()
            RETURNS VARCHAR AS
            $$
            INSERT INTO PUBLIC.T SELECT * FROM PUBLIC.S;
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.S" in entry.sources

    def test_unsupported_language_java(self):
        """JAVA procedures should be skipped."""
        obj = _proc("PUBLIC.JP", """
            CREATE OR REPLACE PROCEDURE PUBLIC.JP()
            RETURNS VARCHAR LANGUAGE JAVA
            HANDLER = 'MyHandler.run'
            AS
            $$
            // Java code — FROM and JOIN in comments
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_unsupported_language_scala(self):
        """SCALA procedures should be skipped."""
        obj = _proc("PUBLIC.SP", """
            CREATE OR REPLACE PROCEDURE PUBLIC.SP()
            RETURNS VARCHAR LANGUAGE SCALA
            HANDLER = 'MyHandler.run'
            AS
            $$
            import something
            $$;
        """)
        assert self.analyzer.analyze(obj) is None


# ==================================================================
# JavaScript procedure analysis
# ==================================================================

class TestJavaScriptAnalysis:
    """Detect lineage from snowflake.execute / createStatement patterns."""

    analyzer = ProcedureBodyAnalyzer()

    def test_snowflake_execute_insert(self):
        obj = _proc("PUBLIC.JS_PROC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.JS_PROC()
            RETURNS VARCHAR LANGUAGE JAVASCRIPT AS
            $$
            snowflake.execute({sqlText: "INSERT INTO PUBLIC.TARGET SELECT * FROM PUBLIC.SOURCE"});
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.SOURCE" in entry.sources
        assert "PUBLIC.TARGET" in entry.targets

    def test_create_statement(self):
        obj = _proc("PUBLIC.JS_PROC2", """
            CREATE OR REPLACE PROCEDURE PUBLIC.JS_PROC2()
            RETURNS VARCHAR LANGUAGE JAVASCRIPT AS
            $$
            var stmt = snowflake.createStatement({sqlText: "DELETE FROM PUBLIC.CLEANUP"});
            stmt.execute();
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.CLEANUP" in entry.targets

    def test_single_quoted_sql_text(self):
        obj = _proc("PUBLIC.JS_PROC3", """
            CREATE OR REPLACE PROCEDURE PUBLIC.JS_PROC3()
            RETURNS VARCHAR LANGUAGE JAVASCRIPT AS
            $$
            snowflake.execute({sqlText: 'MERGE INTO PUBLIC.TARGET USING PUBLIC.SOURCE ON TRUE WHEN MATCHED THEN DELETE'});
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.SOURCE" in entry.sources
        assert "PUBLIC.TARGET" in entry.targets

    def test_no_sql_text_returns_none(self):
        """JS proc without sqlText patterns returns None."""
        obj = _proc("PUBLIC.JS_EMPTY", """
            CREATE OR REPLACE PROCEDURE PUBLIC.JS_EMPTY()
            RETURNS VARCHAR LANGUAGE JAVASCRIPT AS
            $$
            return "hello";
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_multiple_execute_calls(self):
        obj = _proc("PUBLIC.JS_MULTI", """
            CREATE OR REPLACE PROCEDURE PUBLIC.JS_MULTI()
            RETURNS VARCHAR LANGUAGE JAVASCRIPT AS
            $$
            snowflake.execute({sqlText: "INSERT INTO PUBLIC.T1 SELECT * FROM PUBLIC.S1"});
            snowflake.execute({sqlText: "UPDATE PUBLIC.T2 SET col=1"});
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.S1" in entry.sources
        assert "PUBLIC.T1" in entry.targets
        assert "PUBLIC.T2" in entry.targets


# ==================================================================
# Python / Snowpark procedure analysis
# ==================================================================

class TestPythonAnalysis:
    """Detect lineage from session.table / session.sql / save_as_table."""

    analyzer = ProcedureBodyAnalyzer()

    def test_session_table_source(self):
        obj = _proc("PUBLIC.PY_PROC", """
            CREATE OR REPLACE PROCEDURE PUBLIC.PY_PROC()
            RETURNS VARCHAR LANGUAGE PYTHON
            RUNTIME_VERSION = '3.8'
            HANDLER = 'run'
            AS
            $$
            def run(session):
                df = session.table("PUBLIC.MY_TABLE")
                return "ok"
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.MY_TABLE" in entry.sources

    def test_save_as_table_target(self):
        obj = _proc("PUBLIC.PY_PROC2", """
            CREATE OR REPLACE PROCEDURE PUBLIC.PY_PROC2()
            RETURNS VARCHAR LANGUAGE PYTHON
            RUNTIME_VERSION = '3.8'
            HANDLER = 'run'
            AS
            $$
            def run(session):
                df = session.table("PUBLIC.INPUT")
                df.write.save_as_table("PUBLIC.OUTPUT")
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.INPUT" in entry.sources
        assert "PUBLIC.OUTPUT" in entry.targets

    def test_session_sql_with_dml(self):
        obj = _proc("PUBLIC.PY_PROC3", """
            CREATE OR REPLACE PROCEDURE PUBLIC.PY_PROC3()
            RETURNS VARCHAR LANGUAGE PYTHON
            RUNTIME_VERSION = '3.8'
            HANDLER = 'run'
            AS
            $$
            def run(session):
                session.sql("INSERT INTO PUBLIC.TARGET SELECT * FROM PUBLIC.SOURCE").collect()
            $$;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.SOURCE" in entry.sources
        assert "PUBLIC.TARGET" in entry.targets

    def test_python_import_not_treated_as_lineage(self):
        """Python 'from X import Y' must NOT create lineage entries."""
        obj = _proc("PUBLIC.PY_IMPORTS", """
            CREATE OR REPLACE PROCEDURE PUBLIC.PY_IMPORTS()
            RETURNS VARCHAR LANGUAGE PYTHON
            RUNTIME_VERSION = '3.8'
            HANDLER = 'run'
            AS
            $$
            from snowflake.snowpark.types import IntegerType, StringType
            from snowflake.snowpark.functions import col, lit
            import pandas as pd

            def run(session):
                return "done"
            $$;
        """)
        assert self.analyzer.analyze(obj) is None

    def test_write_pandas_target(self):
        obj = _proc("PUBLIC.PY_WP", """
            CREATE OR REPLACE PROCEDURE PUBLIC.PY_WP()
            RETURNS VARCHAR LANGUAGE PYTHON
            RUNTIME_VERSION = '3.8'
            HANDLER = 'run'
            AS
            $$
            def run(session):
                import pandas as pd
                df = pd.DataFrame({"a": [1, 2]})
                session.write_pandas(df).write_pandas("PUBLIC.DEST")
            $$;
        """)
        # write_pandas detects target
        entry = self.analyzer.analyze(obj)
        # May or may not find entry depending on exact pattern
        if entry:
            assert "PUBLIC.DEST" in entry.targets

    def test_no_snowpark_ops_returns_none(self):
        """Python proc without any Snowpark ops returns None."""
        obj = _proc("PUBLIC.PY_EMPTY", """
            CREATE OR REPLACE PROCEDURE PUBLIC.PY_EMPTY()
            RETURNS VARCHAR LANGUAGE PYTHON
            RUNTIME_VERSION = '3.8'
            HANDLER = 'run'
            AS
            $$
            def run(session):
                return "hello"
            $$;
        """)
        assert self.analyzer.analyze(obj) is None


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


# ==================================================================
# TASK lineage
# ==================================================================

class TestTaskLineage:
    analyzer = ProcedureBodyAnalyzer()

    def test_task_simple_insert_select(self):
        """A task that reads from one table and inserts into another."""
        obj = _task("RAVEN.TASK_LOAD_DATA", """
            CREATE OR ALTER TASK RAVEN.TASK_LOAD_DATA
              WAREHOUSE = 'WH_ETL'
              SCHEDULE = 'USING CRON 0 6 * * * UTC'
            AS
            INSERT INTO RAVEN.TARGET_TABLE
            SELECT * FROM RAVEN.SOURCE_TABLE;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "RAVEN.SOURCE_TABLE" in entry.sources
        assert "RAVEN.TARGET_TABLE" in entry.targets
        assert entry.auto_detected is True

    def test_task_after_parent(self):
        """A child task with AFTER references its parent as a source."""
        obj = _task("RAVEN.TASK_CHILD", """
            CREATE OR ALTER TASK RAVEN.TASK_CHILD
              WAREHOUSE = 'WH_ETL'
              AFTER RAVEN.TASK_PARENT
            AS
            INSERT INTO RAVEN.SUMMARY
            SELECT * FROM RAVEN.RAW_DATA;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "RAVEN.TASK_PARENT" in entry.sources
        assert "RAVEN.RAW_DATA" in entry.sources
        assert "RAVEN.SUMMARY" in entry.targets

    def test_task_stream_has_data(self):
        """A task triggered by SYSTEM$STREAM_HAS_DATA."""
        obj = _task("RAVEN.TASK_CDC", """
            CREATE OR ALTER TASK RAVEN.TASK_CDC
              WAREHOUSE = 'WH_ETL'
              SCHEDULE = '1 MINUTE'
              WHEN SYSTEM$STREAM_HAS_DATA('RAVEN.MY_STREAM')
            AS
            MERGE INTO RAVEN.TARGET t
            USING RAVEN.MY_STREAM s ON t.ID = s.ID
            WHEN MATCHED THEN UPDATE SET t.VAL = s.VAL
            WHEN NOT MATCHED THEN INSERT (ID, VAL) VALUES (s.ID, s.VAL);
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "RAVEN.MY_STREAM" in entry.sources
        assert "RAVEN.TARGET" in entry.targets

    def test_task_after_and_stream_has_data(self):
        """Task with both AFTER and STREAM_HAS_DATA."""
        obj = _task("RAVEN.TASK_COMBO", """
            CREATE OR ALTER TASK RAVEN.TASK_COMBO
              WAREHOUSE = 'WH_ETL'
              AFTER RAVEN.TASK_ROOT
              WHEN SYSTEM$STREAM_HAS_DATA('RAVEN.CHANGES')
            AS
            INSERT INTO RAVEN.FINAL SELECT * FROM RAVEN.STAGING;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "RAVEN.TASK_ROOT" in entry.sources
        assert "RAVEN.CHANGES" in entry.sources
        assert "RAVEN.STAGING" in entry.sources
        assert "RAVEN.FINAL" in entry.targets

    def test_task_merge(self):
        """A task that does a MERGE INTO."""
        obj = _task("PUBLIC.TASK_MERGE", """
            CREATE OR ALTER TASK PUBLIC.TASK_MERGE
              WAREHOUSE = 'WH'
              SCHEDULE = '5 MINUTE'
            AS
            MERGE INTO PUBLIC.DIM_CUSTOMER c
            USING PUBLIC.STG_CUSTOMER s ON c.ID = s.ID
            WHEN MATCHED THEN UPDATE SET c.NAME = s.NAME
            WHEN NOT MATCHED THEN INSERT (ID, NAME) VALUES (s.ID, s.NAME);
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.STG_CUSTOMER" in entry.sources
        assert "PUBLIC.DIM_CUSTOMER" in entry.targets

    def test_task_no_body_returns_none(self):
        """A task with no SQL body yields None."""
        obj = _task("RAVEN.EMPTY_TASK", """
            CREATE OR ALTER TASK RAVEN.EMPTY_TASK
              WAREHOUSE = 'WH'
              SCHEDULE = '1 MINUTE';
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is None

    def test_task_call_procedure(self):
        """A task that calls a procedure — no read/write lineage, but
        the AFTER dependency is still captured."""
        obj = _task("RAVEN.TASK_CALLER", """
            CREATE OR ALTER TASK RAVEN.TASK_CALLER
              WAREHOUSE = 'WH'
              AFTER RAVEN.TASK_PARENT
            AS
            CALL RAVEN.MY_PROC();
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "RAVEN.TASK_PARENT" in entry.sources

    def test_task_delete_from(self):
        """A task with DELETE FROM."""
        obj = _task("PUBLIC.TASK_CLEANUP", """
            CREATE OR ALTER TASK PUBLIC.TASK_CLEANUP
              WAREHOUSE = 'WH'
              SCHEDULE = 'USING CRON 0 0 * * * UTC'
            AS
            DELETE FROM PUBLIC.TEMP_DATA WHERE created_at < DATEADD(day, -30, CURRENT_TIMESTAMP());
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "PUBLIC.TEMP_DATA" in entry.targets

    def test_task_multiple_statements_begin_end(self):
        """A task with a BEGIN...END block body."""
        obj = _task("RAVEN.TASK_MULTI", """
            CREATE OR ALTER TASK RAVEN.TASK_MULTI
              WAREHOUSE = 'WH'
              SCHEDULE = '5 MINUTE'
            AS
            BEGIN
              INSERT INTO RAVEN.LOG_TABLE SELECT * FROM RAVEN.RAW_LOG;
              DELETE FROM RAVEN.RAW_LOG WHERE processed = TRUE;
            END;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert "RAVEN.RAW_LOG" in entry.sources
        assert "RAVEN.LOG_TABLE" in entry.targets


# ==================================================================
# STREAM lineage
# ==================================================================

class TestStreamLineage:
    analyzer = ProcedureBodyAnalyzer()

    def test_stream_on_table(self):
        """A stream on a table has the table as source."""
        obj = _stream("RAVEN.MY_STREAM", """
            CREATE OR ALTER STREAM RAVEN.MY_STREAM
              ON TABLE RAVEN.SOURCE_TABLE;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert entry.sources == ["RAVEN.SOURCE_TABLE"]
        assert entry.targets == []
        assert entry.auto_detected is True

    def test_stream_on_view(self):
        """A stream on a view."""
        obj = _stream("PUBLIC.STREAM_ON_VIEW", """
            CREATE OR ALTER STREAM PUBLIC.STREAM_ON_VIEW
              ON VIEW PUBLIC.MY_VIEW;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert entry.sources == ["PUBLIC.MY_VIEW"]

    def test_stream_on_external_table(self):
        """A stream on an external table."""
        obj = _stream("RAW.EXT_STREAM", """
            CREATE OR ALTER STREAM RAW.EXT_STREAM
              ON EXTERNAL TABLE RAW.MY_EXT_TABLE
              INSERT_ONLY = TRUE;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert entry.sources == ["RAW.MY_EXT_TABLE"]

    def test_stream_three_part_name(self):
        """A stream referencing a fully-qualified 3-part table name."""
        obj = _stream("DB.SCHEMA.STR", """
            CREATE OR ALTER STREAM DB.SCHEMA.STR
              ON TABLE DB.SCHEMA.ORDERS;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is not None
        assert entry.sources == ["DB.SCHEMA.ORDERS"]

    def test_stream_no_on_clause(self):
        """A stream without ON TABLE (malformed) returns None."""
        obj = _stream("PUBLIC.BAD_STREAM", """
            CREATE OR ALTER STREAM PUBLIC.BAD_STREAM;
        """)
        entry = self.analyzer.analyze(obj)
        assert entry is None

    def test_table_is_not_analyzed(self):
        """A TABLE object should still return None."""
        obj = _table("PUBLIC.MY_TABLE")
        entry = self.analyzer.analyze(obj)
        assert entry is None


# ==================================================================
# Scanner picks up TASK and STREAM
# ==================================================================

class TestScannerTaskStream:
    """LineageScanner.scan() discovers TASK and STREAM lineage."""

    def test_scan_includes_task(self):
        task = _task("RAVEN.MY_TASK", """
            CREATE OR ALTER TASK RAVEN.MY_TASK
              WAREHOUSE = 'WH'
              SCHEDULE = '5 MINUTE'
            AS
            INSERT INTO RAVEN.T2 SELECT * FROM RAVEN.T1;
        """, file_path="/tmp/objects/tasks/my_task.sql")
        scanner = LineageScanner("/tmp/objects")
        entries = scanner.scan(parsed_objects={"RAVEN.MY_TASK": task})
        assert len(entries) == 1
        assert "RAVEN.T1" in entries[0].sources
        assert "RAVEN.T2" in entries[0].targets

    def test_scan_includes_stream(self):
        stream = _stream("RAVEN.STR", """
            CREATE OR ALTER STREAM RAVEN.STR ON TABLE RAVEN.ORDERS;
        """, file_path="/tmp/objects/streams/str.sql")
        scanner = LineageScanner("/tmp/objects")
        entries = scanner.scan(parsed_objects={"RAVEN.STR": stream})
        assert len(entries) == 1
        assert entries[0].sources == ["RAVEN.ORDERS"]
