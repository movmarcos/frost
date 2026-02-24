"""Tests for frost.data_loader -- CSV scanning, loading, escaping.

Includes CSV-driven tests from tests/data/escape_cases.csv.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, call

from frost.data_loader import DataLoader, DataFile, _escape, _chunked
from helpers import load_csv, DATA_DIR


# ------------------------------------------------------------------
# CSV-driven: escape_cases.csv
# ------------------------------------------------------------------

_ESCAPE_CASES = load_csv("escape_cases.csv")


@pytest.mark.csv
@pytest.mark.parametrize(
    "row",
    _ESCAPE_CASES,
    ids=[r["description"] for r in _ESCAPE_CASES],
)
def test_escape_csv_cases(row):
    """Each row in escape_cases.csv declares input -> expected escaped output."""
    assert _escape(row["value"]) == row["expected"]


# ------------------------------------------------------------------
# Unit tests: _escape
# ------------------------------------------------------------------

def test_escape_null():
    assert _escape("NULL") == "NULL"
    assert _escape("null") == "NULL"


def test_escape_empty():
    assert _escape("") == "NULL"


def test_escape_normal():
    assert _escape("hello") == "'hello'"


def test_escape_quotes():
    assert _escape("it's") == "'it''s'"


# ------------------------------------------------------------------
# Unit tests: _chunked
# ------------------------------------------------------------------

def test_chunked_exact():
    result = list(_chunked([1, 2, 3, 4], 2))
    assert result == [[1, 2], [3, 4]]


def test_chunked_remainder():
    result = list(_chunked([1, 2, 3], 2))
    assert result == [[1, 2], [3]]


def test_chunked_empty():
    result = list(_chunked([], 5))
    assert result == []


# ------------------------------------------------------------------
# Unit tests: DataLoader.scan()
# ------------------------------------------------------------------

def test_scan_finds_csv(tmp_path):
    """scan() should find CSV files in the data folder."""
    csv_content = "id,name\n1,Alice\n2,Bob\n"
    (tmp_path / "users.csv").write_text(csv_content)

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    files = loader.scan()

    assert len(files) == 1
    assert files[0].table_name == "USERS"
    assert files[0].columns == ["id", "name"]
    assert len(files[0].rows) == 2


def test_scan_empty_folder(tmp_path):
    """scan() on a folder with no CSV files should return empty list."""
    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    assert loader.scan() == []


def test_scan_missing_folder(tmp_path):
    """scan() on a non-existent folder should return empty list."""
    loader = DataLoader(data_folder=str(tmp_path / "nope"), schema="PUBLIC")
    assert loader.scan() == []


def test_scan_with_sidecar(tmp_path):
    """scan() should pick up column type overrides from YAML sidecar."""
    (tmp_path / "orders.csv").write_text("id,amount\n1,100\n")
    (tmp_path / "orders.yml").write_text("columns:\n  id: NUMBER\n  amount: DECIMAL(12,2)\n")

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    files = loader.scan()

    assert files[0].column_types == {"ID": "NUMBER", "AMOUNT": "DECIMAL(12,2)"}


# ------------------------------------------------------------------
# Unit tests: DataLoader.load()
# ------------------------------------------------------------------

def test_load_creates_table_and_inserts(tmp_path, mock_connector):
    """load() should call execute_single for CREATE and INSERT."""
    # Write the file BEFORE constructing DataFile (checksum in __post_init__)
    (tmp_path / "test.csv").write_text("id,name\n1,Alice\n2,Bob\n")
    df = DataFile(
        file_path=str(tmp_path / "test.csv"),
        table_name="TEST",
        schema="PUBLIC",
        columns=["id", "name"],
        rows=[["1", "Alice"], ["2", "Bob"]],
    )

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    loader.load(mock_connector, df)

    # Should have called execute_single at least twice (CREATE + INSERT)
    assert mock_connector.execute_single.call_count >= 2

    # First call should be the CREATE TABLE
    create_call = mock_connector.execute_single.call_args_list[0][0][0]
    assert "CREATE OR ALTER TABLE" in create_call
    assert "PUBLIC.TEST" in create_call

    # Second call should be INSERT
    insert_call = mock_connector.execute_single.call_args_list[1][0][0]
    assert "INSERT INTO" in insert_call


def test_load_dry_run(tmp_path, mock_connector):
    """load() with dry_run=True should not execute anything."""
    (tmp_path / "test.csv").write_text("id,name\n1,Alice\n")
    df = DataFile(
        file_path=str(tmp_path / "test.csv"),
        table_name="TEST",
        schema="PUBLIC",
        columns=["id", "name"],
        rows=[["1", "Alice"]],
    )

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    loader.load(mock_connector, df, dry_run=True)

    mock_connector.execute_single.assert_not_called()


def test_load_with_type_overrides(tmp_path, mock_connector):
    """load() should use column type overrides in CREATE TABLE."""
    (tmp_path / "test.csv").write_text("id,amount\n1,100\n")
    df = DataFile(
        file_path=str(tmp_path / "test.csv"),
        table_name="TEST",
        schema="PUBLIC",
        columns=["id", "amount"],
        column_types={"ID": "NUMBER", "AMOUNT": "DECIMAL(12,2)"},
        rows=[["1", "100"]],
    )

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    loader.load(mock_connector, df)

    create_call = mock_connector.execute_single.call_args_list[0][0][0]
    assert "NUMBER" in create_call
    assert "DECIMAL(12,2)" in create_call


# ------------------------------------------------------------------
# Integration: scan real test CSV
# ------------------------------------------------------------------

def test_scan_test_data_csv():
    """Scan the tests/data/sample_load.csv with its YAML sidecar."""
    loader = DataLoader(data_folder=str(DATA_DIR), schema="PUBLIC")
    files = loader.scan()

    # Should find at least sample_load.csv (there may be other csvs in data/)
    sample = [f for f in files if f.table_name == "SAMPLE_LOAD"]
    assert len(sample) == 1

    df = sample[0]
    assert df.columns == ["id", "name", "status", "amount"]
    assert len(df.rows) == 3
    assert df.column_types.get("ID") == "NUMBER"
    assert df.column_types.get("AMOUNT") == "DECIMAL(12,2)"


# ------------------------------------------------------------------
# DataFile properties
# ------------------------------------------------------------------

def test_datafile_fqn():
    """DataFile.fqn should combine schema and table_name."""
    df = DataFile.__new__(DataFile)
    df.schema = "PUBLIC"
    df.table_name = "USERS"
    df.checksum = "abc"
    assert df.fqn == "PUBLIC.USERS"


def test_datafile_object_type():
    """DataFile.object_type should always be 'DATA'."""
    df = DataFile.__new__(DataFile)
    df.checksum = "abc"
    assert df.object_type == "DATA"


# ------------------------------------------------------------------
# Row padding / truncation (trailing empty columns)
# ------------------------------------------------------------------

def test_scan_pads_short_rows(tmp_path):
    """Rows with fewer values than headers get padded with empty strings."""
    # Trailing comma means csv.reader gives 4 values (last one empty),
    # but no trailing comma means only 3 -- both should produce 4 columns.
    csv_text = "a,b,c,d\n1,2,3,\n4,5,6\n"
    (tmp_path / "data.csv").write_text(csv_text)

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    files = loader.scan()

    assert len(files) == 1
    df = files[0]
    assert df.columns == ["a", "b", "c", "d"]
    # Row 1: trailing comma -> csv.reader gives ["1","2","3",""]  -> 4 values
    assert len(df.rows[0]) == 4
    # Row 2: no trailing comma -> csv.reader gives ["4","5","6"] -> padded to 4
    assert len(df.rows[1]) == 4
    assert df.rows[1] == ["4", "5", "6", ""]


def test_scan_truncates_extra_columns(tmp_path):
    """Rows with more values than headers get truncated with a warning."""
    csv_text = "a,b\n1,2,EXTRA\n"
    (tmp_path / "wide.csv").write_text(csv_text)

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    files = loader.scan()

    assert len(files) == 1
    assert len(files[0].rows[0]) == 2
    assert files[0].rows[0] == ["1", "2"]


def test_load_short_row_generates_null(tmp_path, mock_connector):
    """A padded empty value should become NULL in the INSERT statement."""
    (tmp_path / "t.csv").write_text("a,b,c\n1,2,\n")
    df = DataFile(
        file_path=str(tmp_path / "t.csv"),
        table_name="T",
        schema="PUBLIC",
        columns=["a", "b", "c"],
        rows=[["1", "2", ""]],
    )

    loader = DataLoader(data_folder=str(tmp_path), schema="PUBLIC")
    loader.load(mock_connector, df)

    insert_call = mock_connector.execute_single.call_args_list[1][0][0]
    assert "NULL" in insert_call
    # Should have 3 values in the VALUES clause
    assert "'1', '2', NULL" in insert_call
