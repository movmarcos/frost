"""Shared fixtures and CSV data-loading helpers for frost tests."""

import csv
import os
import tempfile
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock

import pytest


# ------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------

TESTS_DIR = Path(__file__).parent
DATA_DIR = TESTS_DIR / "data"


# ------------------------------------------------------------------
# CSV data-driven test helper
# ------------------------------------------------------------------

def load_csv(filename: str) -> List[Dict[str, str]]:
    """Load a CSV file from tests/data/ and return a list of row dicts."""
    path = DATA_DIR / filename
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture()
def tmp_dir(tmp_path):
    """Provide a temporary directory with automatic cleanup."""
    return tmp_path


@pytest.fixture()
def sql_file(tmp_path):
    """Factory fixture: write SQL content to a temp .sql file and return its path."""
    def _make(content: str, name: str = "test.sql") -> str:
        p = tmp_path / name
        p.write_text(content, encoding="utf-8")
        return str(p)
    return _make


@pytest.fixture()
def mock_connector():
    """A MagicMock standing in for SnowflakeConnector."""
    conn = MagicMock()
    conn.execute.return_value = []
    conn.execute_single.return_value = []
    conn.execute_params.return_value = []
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure Snowflake / frost env vars don't leak between tests."""
    for key in list(os.environ):
        if key.startswith(("SNOWFLAKE_", "FROST_")):
            monkeypatch.delenv(key, raising=False)
