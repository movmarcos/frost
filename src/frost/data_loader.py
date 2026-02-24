"""Data loader -- load CSV files into Snowflake tables.

Scans a ``data/`` folder for ``.csv`` files.  Each CSV becomes a table
whose name is derived from the filename (e.g. ``countries.csv`` ->
``COUNTRIES``).  The first row must be a header.

Behaviour:
  1. ``CREATE OR ALTER TABLE`` with columns typed as ``VARCHAR`` by
     default.  An optional ``<name>.yml`` sidecar can override column
     types.
  2. ``INSERT INTO ... VALUES (...)`` in batches.

CSV tables participate in the dependency graph: they are always created
*after* the schema they belong to, and other objects (views, procedures)
can reference them normally.
"""

import csv
import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from frost.connector import SnowflakeConnector

log = logging.getLogger("frost")

_BATCH_SIZE = 1_000  # rows per INSERT ... VALUES batch


# ----------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------

@dataclass
class DataFile:
    """A CSV file that represents a loadable table."""

    file_path: str
    table_name: str
    schema: Optional[str]
    columns: List[str]                              # header names
    column_types: Dict[str, str] = field(default_factory=dict)  # overrides
    rows: List[List[str]] = field(default_factory=list)
    checksum: str = ""

    def __post_init__(self):
        if not self.checksum:
            raw = Path(self.file_path).read_bytes()
            self.checksum = hashlib.md5(raw).hexdigest()

    @property
    def fqn(self) -> str:
        parts = [p for p in (self.schema, self.table_name) if p]
        return ".".join(parts).upper()

    @property
    def object_type(self) -> str:
        return "DATA"


# ----------------------------------------------------------------------
# Loader
# ----------------------------------------------------------------------

class DataLoader:
    """Scan CSV files and load them into Snowflake."""

    def __init__(
        self,
        data_folder: str,
        schema: Optional[str] = None,
    ):
        self.data_folder = Path(data_folder)
        self.schema = schema

    # -- scanning ------------------------------------------------------

    def scan(self) -> List[DataFile]:
        """Parse all CSV files in the data folder."""
        if not self.data_folder.is_dir():
            log.debug("Data folder not found: %s -- skipping", self.data_folder)
            return []

        csv_files = sorted(self.data_folder.rglob("*.csv"))
        log.info("Scanning %d CSV files in '%s'", len(csv_files), self.data_folder)

        data_files: List[DataFile] = []
        for path in csv_files:
            try:
                df = self._parse_csv(path)
                data_files.append(df)
            except Exception as exc:
                log.error("Failed to parse %s: %s", path, exc)

        return data_files

    # -- loading -------------------------------------------------------

    def load(
        self,
        connector: SnowflakeConnector,
        data_file: DataFile,
        *,
        dry_run: bool = False,
    ) -> None:
        """Create the table and insert all rows from a DataFile."""
        fqn = data_file.fqn

        # Build column definitions
        col_defs = []
        for col in data_file.columns:
            col_upper = col.upper()
            dtype = data_file.column_types.get(col_upper, "VARCHAR(16777216)")
            col_defs.append(f"    {col_upper} {dtype}")
        col_block = ",\n".join(col_defs)

        create_sql = (
            f"CREATE OR ALTER TABLE {fqn} (\n{col_block}\n)"
        )

        if dry_run:
            log.info("  DRY RUN: %s -> %d columns, %d rows", fqn, len(data_file.columns), len(data_file.rows))
            return

        log.info("LOAD  [DATA]  %s  (%d rows)", fqn, len(data_file.rows))

        # Create table
        connector.execute_single(create_sql)

        # Insert in batches
        if data_file.rows:
            col_names = ", ".join(c.upper() for c in data_file.columns)
            total = 0
            for batch in _chunked(data_file.rows, _BATCH_SIZE):
                values_clauses = []
                for row in batch:
                    escaped = [_escape(v) for v in row]
                    values_clauses.append(f"({', '.join(escaped)})")
                insert_sql = (
                    f"INSERT INTO {fqn} ({col_names})\nVALUES\n"
                    + ",\n".join(values_clauses)
                )
                connector.execute_single(insert_sql)
                total += len(batch)

            log.info("  %d rows loaded", total)

    # -- internal helpers ----------------------------------------------

    def _parse_csv(self, path: Path) -> DataFile:
        """Read a CSV file + optional YAML sidecar for column types."""
        table_name = path.stem.upper()

        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            headers = next(reader)
            raw_rows = list(reader)

        # Pad rows that have fewer values than headers (e.g. trailing
        # empty columns like ``a,b,c,`` where the last value is blank).
        n_cols = len(headers)
        rows: list[list[str]] = []
        for idx, row in enumerate(raw_rows):
            if len(row) < n_cols:
                row = row + [""] * (n_cols - len(row))
            elif len(row) > n_cols:
                log.warning(
                    "%s row %d: expected %d columns but got %d -- extra values truncated",
                    path.name, idx + 2, n_cols, len(row),
                )
                row = row[:n_cols]
            rows.append(row)

        # Optional sidecar: countries.yml alongside countries.csv
        column_types = self._load_sidecar(path)

        return DataFile(
            file_path=str(path),
            table_name=table_name,
            schema=self.schema,
            columns=headers,
            column_types=column_types,
            rows=rows,
        )

    def _load_sidecar(self, csv_path: Path) -> Dict[str, str]:
        """Load optional YAML sidecar for column type overrides.

        Expected format:
            columns:
              id: NUMBER
              amount: DECIMAL(12,2)
              created_at: TIMESTAMP_NTZ
        """
        for ext in (".yml", ".yaml"):
            sidecar = csv_path.with_suffix(ext)
            if sidecar.is_file():
                with open(sidecar) as fh:
                    data = yaml.safe_load(fh) or {}
                raw = data.get("columns", {})
                return {k.upper(): v for k, v in raw.items()} if raw else {}
        return {}


# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------

def _escape(value: str) -> str:
    """Escape a value for a SQL VALUES clause."""
    if value == "" or value.upper() == "NULL":
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _chunked(lst: list, size: int):
    """Yield successive chunks of *size* from *lst*."""
    for i in range(0, len(lst), size):
        yield lst[i : i + size]
