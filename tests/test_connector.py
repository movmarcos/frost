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
