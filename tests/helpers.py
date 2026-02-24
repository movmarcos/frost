"""Shared helpers for frost tests -- importable from test modules."""

import csv
from pathlib import Path
from typing import Dict, List

TESTS_DIR = Path(__file__).parent
DATA_DIR = TESTS_DIR / "data"


def load_csv(filename: str) -> List[Dict[str, str]]:
    """Load a CSV file from tests/data/ and return a list of row dicts."""
    path = DATA_DIR / filename
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))
