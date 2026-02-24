"""Data tester -- run YAML-defined data quality tests against CSV files.

Reads one or more YAML test-config files and validates CSV data locally
(no Snowflake connection required).

Supported test types
--------------------
* **unique**          -- column values are unique (no duplicates)
* **not_null**        -- column has no empty / NULL values
* **accepted_values** -- every value is in an allowed set
* **row_count**       -- file has between ``min`` and ``max`` rows
* **relationship**    -- every value in *column* exists in another CSV's column
* **expression**      -- a Python expression evaluated per row (truthy = pass)
"""

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import yaml

log = logging.getLogger("frost")


# ------------------------------------------------------------------
# Data models
# ------------------------------------------------------------------

@dataclass
class TestCase:
    """A single data quality test loaded from YAML."""

    name: str
    source: str                        # CSV filename (relative to data folder)
    test: str                          # test type keyword
    description: str = ""
    column: Optional[str] = None       # target column (most tests)
    values: Optional[List[str]] = None  # accepted_values list
    min: Optional[int] = None          # row_count lower bound
    max: Optional[int] = None          # row_count upper bound
    to: Optional[str] = None           # relationship target CSV
    to_column: Optional[str] = None    # relationship target column
    expression: Optional[str] = None   # Python expression for 'expression' test


@dataclass
class TestResult:
    """The outcome of a single test execution."""

    test_case: TestCase
    passed: bool
    message: str
    failing_rows: List[str] = field(default_factory=list)  # sample violations


# ------------------------------------------------------------------
# CSV loader helper
# ------------------------------------------------------------------

def _load_csv(path: Path) -> tuple:
    """Load a CSV file.  Returns (columns, rows) where rows is a list of dicts."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        columns = reader.fieldnames or []
        rows = list(reader)
    return columns, rows


# ------------------------------------------------------------------
# Test runner
# ------------------------------------------------------------------

_VALID_TESTS = {"unique", "not_null", "accepted_values", "row_count",
                "relationship", "expression"}


class DataTester:
    """Load YAML test configs and run data-quality checks against CSV files."""

    def __init__(self, data_folder: str, test_config: str = "frost-tests.yml"):
        self.data_folder = Path(data_folder)
        self.test_config = Path(test_config)
        self._csv_cache: Dict[str, tuple] = {}  # filename -> (cols, rows)

    # -- public API ----------------------------------------------------

    def load_tests(self) -> List[TestCase]:
        """Parse the YAML test config and return a list of TestCase objects."""
        if not self.test_config.exists():
            log.warning("Test config not found: %s", self.test_config)
            return []

        raw = yaml.safe_load(self.test_config.read_text(encoding="utf-8"))
        if not raw or "tests" not in raw:
            log.warning("No 'tests' key found in %s", self.test_config)
            return []

        cases: List[TestCase] = []
        for entry in raw["tests"]:
            tc = TestCase(
                name=entry.get("name", "unnamed"),
                source=entry["source"],
                test=entry["test"],
                description=entry.get("description", ""),
                column=entry.get("column"),
                values=[str(v) for v in entry["values"]] if "values" in entry else None,
                min=entry.get("min"),
                max=entry.get("max"),
                to=entry.get("to"),
                to_column=entry.get("to_column"),
                expression=entry.get("expression"),
            )
            cases.append(tc)
        return cases

    def run(self, cases: Optional[List[TestCase]] = None) -> List[TestResult]:
        """Execute all tests and return results."""
        if cases is None:
            cases = self.load_tests()

        results: List[TestResult] = []
        for tc in cases:
            try:
                result = self._run_one(tc)
            except Exception as exc:
                result = TestResult(
                    test_case=tc,
                    passed=False,
                    message=f"Error running test: {exc}",
                )
            results.append(result)
        return results

    # -- internal dispatch ---------------------------------------------

    def _run_one(self, tc: TestCase) -> TestResult:
        if tc.test not in _VALID_TESTS:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Unknown test type: '{tc.test}'.  "
                        f"Valid types: {', '.join(sorted(_VALID_TESTS))}",
            )

        handler = getattr(self, f"_test_{tc.test}")
        return handler(tc)

    # -- CSV access with caching ---------------------------------------

    def _get_csv(self, filename: str) -> tuple:
        """Return (columns, rows) for a CSV file; results are cached."""
        if filename not in self._csv_cache:
            path = self.data_folder / filename
            if not path.exists():
                raise FileNotFoundError(
                    f"CSV file not found: {path}"
                )
            self._csv_cache[filename] = _load_csv(path)
        return self._csv_cache[filename]

    def _require_column(self, tc: TestCase) -> None:
        """Raise ValueError if the test case has no column specified."""
        if not tc.column:
            raise ValueError(
                f"Test '{tc.name}' requires a 'column' field"
            )

    # -- Test implementations ------------------------------------------

    def _test_unique(self, tc: TestCase) -> TestResult:
        self._require_column(tc)
        columns, rows = self._get_csv(tc.source)
        col = tc.column

        if col not in columns:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Column '{col}' not found in {tc.source}.  "
                        f"Available: {', '.join(columns)}",
            )

        seen: Dict[str, int] = {}
        duplicates: List[str] = []
        for i, row in enumerate(rows, 2):  # row 1 is the header
            val = row.get(col, "")
            if val in seen:
                duplicates.append(
                    f"row {i}: '{val}' (first seen row {seen[val]})"
                )
            else:
                seen[val] = i

        if duplicates:
            return TestResult(
                test_case=tc, passed=False,
                message=f"{len(duplicates)} duplicate value(s) in column '{col}'",
                failing_rows=duplicates[:20],
            )
        return TestResult(
            test_case=tc, passed=True,
            message=f"All {len(rows)} values in '{col}' are unique",
        )

    def _test_not_null(self, tc: TestCase) -> TestResult:
        self._require_column(tc)
        columns, rows = self._get_csv(tc.source)
        col = tc.column

        if col not in columns:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Column '{col}' not found in {tc.source}.  "
                        f"Available: {', '.join(columns)}",
            )

        nulls: List[str] = []
        for i, row in enumerate(rows, 2):
            val = row.get(col, "")
            if val.strip() == "" or val.upper() == "NULL":
                nulls.append(f"row {i}: value is {'NULL' if val.upper() == 'NULL' else 'empty'}")

        if nulls:
            return TestResult(
                test_case=tc, passed=False,
                message=f"{len(nulls)} null/empty value(s) in column '{col}'",
                failing_rows=nulls[:20],
            )
        return TestResult(
            test_case=tc, passed=True,
            message=f"All {len(rows)} values in '{col}' are non-null",
        )

    def _test_accepted_values(self, tc: TestCase) -> TestResult:
        self._require_column(tc)
        if not tc.values:
            return TestResult(
                test_case=tc, passed=False,
                message="Test 'accepted_values' requires a 'values' list",
            )

        columns, rows = self._get_csv(tc.source)
        col = tc.column
        allowed = set(tc.values)

        if col not in columns:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Column '{col}' not found in {tc.source}.  "
                        f"Available: {', '.join(columns)}",
            )

        bad: List[str] = []
        for i, row in enumerate(rows, 2):
            val = row.get(col, "")
            if val not in allowed:
                bad.append(f"row {i}: '{val}' not in {sorted(allowed)}")

        if bad:
            return TestResult(
                test_case=tc, passed=False,
                message=f"{len(bad)} value(s) in '{col}' outside accepted set",
                failing_rows=bad[:20],
            )
        return TestResult(
            test_case=tc, passed=True,
            message=f"All {len(rows)} values in '{col}' are in accepted set",
        )

    def _test_row_count(self, tc: TestCase) -> TestResult:
        _, rows = self._get_csv(tc.source)
        count = len(rows)
        lo = tc.min if tc.min is not None else 0
        hi = tc.max

        if count < lo:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Row count {count} is below minimum {lo}",
            )
        if hi is not None and count > hi:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Row count {count} exceeds maximum {hi}",
            )
        return TestResult(
            test_case=tc, passed=True,
            message=f"Row count {count} is within expected range"
                    f" [{lo}..{'∞' if hi is None else hi}]",
        )

    def _test_relationship(self, tc: TestCase) -> TestResult:
        self._require_column(tc)
        if not tc.to or not tc.to_column:
            return TestResult(
                test_case=tc, passed=False,
                message="Test 'relationship' requires 'to' and 'to_column' fields",
            )

        src_cols, src_rows = self._get_csv(tc.source)
        tgt_cols, tgt_rows = self._get_csv(tc.to)

        if tc.column not in src_cols:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Column '{tc.column}' not found in {tc.source}",
            )
        if tc.to_column not in tgt_cols:
            return TestResult(
                test_case=tc, passed=False,
                message=f"Column '{tc.to_column}' not found in {tc.to}",
            )

        target_values = {row[tc.to_column] for row in tgt_rows}
        orphans: List[str] = []
        for i, row in enumerate(src_rows, 2):
            val = row.get(tc.column, "")
            if val and val not in target_values:
                orphans.append(f"row {i}: '{val}' not found in {tc.to}.{tc.to_column}")

        if orphans:
            return TestResult(
                test_case=tc, passed=False,
                message=f"{len(orphans)} orphan value(s) in '{tc.column}'",
                failing_rows=orphans[:20],
            )
        return TestResult(
            test_case=tc, passed=True,
            message=f"All values in '{tc.column}' exist in {tc.to}.{tc.to_column}",
        )

    def _test_expression(self, tc: TestCase) -> TestResult:
        if not tc.expression:
            return TestResult(
                test_case=tc, passed=False,
                message="Test 'expression' requires an 'expression' field",
            )

        columns, rows = self._get_csv(tc.source)
        failures: List[str] = []

        # Allow a limited set of safe builtins inside expressions
        _safe_builtins = {
            "int": int, "float": float, "str": str, "bool": bool,
            "len": len, "abs": abs, "min": min, "max": max,
            "round": round, "sum": sum, "sorted": sorted,
            "True": True, "False": False, "None": None,
        }

        for i, row in enumerate(rows, 2):
            try:
                result = eval(tc.expression, {"__builtins__": _safe_builtins}, dict(row))  # noqa: S307
                if not result:
                    failures.append(f"row {i}: expression evaluated to {result!r}")
            except Exception as exc:
                failures.append(f"row {i}: expression error: {exc}")

        if failures:
            return TestResult(
                test_case=tc, passed=False,
                message=f"{len(failures)} row(s) failed expression check",
                failing_rows=failures[:20],
            )
        return TestResult(
            test_case=tc, passed=True,
            message=f"All {len(rows)} rows satisfy expression",
        )
