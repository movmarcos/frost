"""Tests for frost.connector -- statement splitter.

Includes CSV-driven tests from tests/data/splitter_cases.csv.
The actual Snowflake connection is not tested (requires credentials).
We test the _split_statements static method which is pure logic.
"""

import pytest

from frost.connector import SnowflakeConnector
from helpers import load_csv


# ------------------------------------------------------------------
# CSV-driven: splitter_cases.csv
# ------------------------------------------------------------------

_SPLITTER_CASES = load_csv("splitter_cases.csv")


@pytest.mark.csv
@pytest.mark.parametrize(
    "row",
    _SPLITTER_CASES,
    ids=[r["description"] for r in _SPLITTER_CASES],
)
def test_splitter_csv_cases(row):
    """Each row in splitter_cases.csv declares expected statement count."""
    stmts = SnowflakeConnector._split_statements(row["sql"])
    # Filter out empty statements
    stmts = [s for s in stmts if s.strip()]
    expected = int(row["expected_count"])
    assert len(stmts) == expected, (
        f"Expected {expected} statements, got {len(stmts)}: {stmts}"
    )


# ------------------------------------------------------------------
# Unit tests: _split_statements
# ------------------------------------------------------------------

def test_split_empty():
    assert SnowflakeConnector._split_statements("") == []


def test_split_single_no_semicolon():
    stmts = SnowflakeConnector._split_statements("SELECT 1")
    assert len(stmts) == 1
    assert stmts[0].strip() == "SELECT 1"


def test_split_respects_single_quotes():
    stmts = SnowflakeConnector._split_statements("SELECT 'a;b'; SELECT 2;")
    stmts = [s for s in stmts if s.strip()]
    assert len(stmts) == 2


def test_split_respects_dollar_quotes():
    sql = "CREATE PROCEDURE P() AS $$x := 1; y := 2;$$; SELECT 1;"
    stmts = SnowflakeConnector._split_statements(sql)
    stmts = [s for s in stmts if s.strip()]
    assert len(stmts) == 2


def test_split_respects_tagged_dollar_quotes():
    sql = "CREATE PROCEDURE P() AS $body$x; y;$body$; SELECT 1;"
    stmts = SnowflakeConnector._split_statements(sql)
    stmts = [s for s in stmts if s.strip()]
    assert len(stmts) == 2


def test_split_respects_line_comments():
    sql = "-- comment with ; inside\nSELECT 1;"
    stmts = SnowflakeConnector._split_statements(sql)
    stmts = [s for s in stmts if s.strip()]
    assert len(stmts) == 1


def test_split_respects_block_comments():
    sql = "/* comment with ; inside */ SELECT 1;"
    stmts = SnowflakeConnector._split_statements(sql)
    stmts = [s for s in stmts if s.strip()]
    assert len(stmts) == 1


def test_split_escaped_quote():
    """Escaped single quote ('') should not end the string prematurely."""
    sql = "SELECT 'it''s ok'; SELECT 2;"
    stmts = SnowflakeConnector._split_statements(sql)
    stmts = [s for s in stmts if s.strip()]
    assert len(stmts) == 2


# ------------------------------------------------------------------
# get_existing_objects_in_schema
# ------------------------------------------------------------------

from unittest.mock import MagicMock, patch
from frost.connector import _SHOW_CMD_MAP


def test_existing_objects_unsupported_type():
    """Unsupported types (SCRIPT, DATABASE, etc.) return None."""
    conn = SnowflakeConnector.__new__(SnowflakeConnector)
    conn._conn = MagicMock()
    assert conn.get_existing_objects_in_schema("PUBLIC", "SCRIPT") is None
    assert conn.get_existing_objects_in_schema("PUBLIC", "DATABASE") is None


def test_existing_objects_returns_names():
    """SHOW results are parsed into upper-cased name set."""
    conn = SnowflakeConnector.__new__(SnowflakeConnector)
    conn._conn = MagicMock()
    # Simulate SHOW TABLES result: (created_on, name, ...)
    conn._conn.cursor.return_value.__enter__ = MagicMock()
    conn._conn.cursor.return_value.__exit__ = MagicMock()

    with patch.object(conn, "execute_single", return_value=[
        ("2024-01-01", "T1", "DEV", "PUBLIC"),
        ("2024-01-01", "T2", "DEV", "PUBLIC"),
    ]):
        result = conn.get_existing_objects_in_schema("PUBLIC", "TABLE")
    assert result == {"T1", "T2"}


def test_existing_objects_strips_proc_signature():
    """Procedure names with signatures (e.g. MY_PROC(VARCHAR)) are cleaned."""
    conn = SnowflakeConnector.__new__(SnowflakeConnector)
    conn._conn = MagicMock()
    with patch.object(conn, "execute_single", return_value=[
        ("2024-01-01", "MY_PROC(VARCHAR, NUMBER)", "PUBLIC"),
        ("2024-01-01", "SIMPLE_PROC()", "PUBLIC"),
    ]):
        result = conn.get_existing_objects_in_schema("PUBLIC", "PROCEDURE")
    assert result == {"MY_PROC", "SIMPLE_PROC"}


def test_existing_objects_show_failure_returns_empty():
    """If the SHOW command fails, return empty set (objects assumed missing)."""
    conn = SnowflakeConnector.__new__(SnowflakeConnector)
    conn._conn = MagicMock()
    with patch.object(conn, "execute_single", side_effect=Exception("Schema not found")):
        result = conn.get_existing_objects_in_schema("NONEXISTENT", "TABLE")
    assert result == set()


def test_show_cmd_map_covers_common_types():
    """Verify that all common Snowflake DDL types have SHOW mappings."""
    for t in ("TABLE", "VIEW", "PROCEDURE", "FUNCTION", "STAGE",
              "FILE FORMAT", "TASK", "TAG", "DYNAMIC TABLE"):
        assert t in _SHOW_CMD_MAP, f"{t} missing from _SHOW_CMD_MAP"
