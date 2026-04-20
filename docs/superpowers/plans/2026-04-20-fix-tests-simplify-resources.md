# Fix lineage-JSON tests + simplify resources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the 5 failing tests in `tests/test_cli_lineage_json.py` to green and apply small simplifications to `src/frost/resources.py` (modernize typing, localize column-index fragility).

**Architecture:** Two independent edits in different files. (1) Test helper `_run_cli` gets an explicit `PYTHONPATH=<repo>/src` env so the spawned `python -m frost` subprocess can import the package without depending on editable-install state. (2) `resources.py` drops `Dict`/`List` from typing imports and extracts the per-row dict construction into a `_parse_show_row()` helper with a docstring noting the column-index heuristic.

**Tech Stack:** Python 3.10+, pytest, stdlib `subprocess`/`os`/`pathlib`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-04-20-fix-tests-simplify-resources-design.md`

---

## File Structure

**Modified files (2):**
- `tests/test_cli_lineage_json.py` — `_run_cli` helper gains an `env=` argument with `PYTHONPATH` set; module-level `SRC_DIR` constant added; `import os` added
- `src/frost/resources.py` — typing modernized; `_parse_show_row` helper extracted from inner loop

**Untouched:**
- `src/frost/cli.py` (`_cmd_resources` already follows project pattern)
- `src/frost/connector.py` (no Connector Protocol in this scope)
- `tests/conftest.py` (no fixture changes needed)
- `tests/test_resources.py` (serves as the safety net; must remain green untouched)

---

## Task 1: Fix `_run_cli` to pass PYTHONPATH to subprocess

**Files:**
- Modify: `tests/test_cli_lineage_json.py:1-20`

**Why first:** This is the failing-test fix. Doing it first proves the diagnosis and gives us a green baseline before any `resources.py` work.

- [ ] **Step 1: Confirm the current failure mode**

Run: `python3 -m pytest tests/test_cli_lineage_json.py -v`
Expected: 5 failures, each with `No module named frost` in stderr. This is the baseline.

- [ ] **Step 2: Apply the fix**

Edit `tests/test_cli_lineage_json.py`. Change the imports block and the `_run_cli` helper.

Current (lines 1-20):

```python
"""Tests for `frost lineage --json` subgraph and full-graph CLI branches."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "data" / "lineage_fixture"


def _run_cli(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Run `python -m frost ...` and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "frost", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )
```

Replace with:

```python
"""Tests for `frost lineage --json` subgraph and full-graph CLI branches."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "data" / "lineage_fixture"
SRC_DIR = Path(__file__).resolve().parents[1] / "src"


def _run_cli(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Run `python -m frost ...` and return the result.

    Passes PYTHONPATH=<repo>/src so the subprocess can import frost
    without requiring `pip install -e .` in the active interpreter.
    """
    env = {**os.environ, "PYTHONPATH": str(SRC_DIR)}
    return subprocess.run(
        [sys.executable, "-m", "frost", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        env=env,
    )
```

- [ ] **Step 3: Verify the 5 tests now pass**

Run: `python3 -m pytest tests/test_cli_lineage_json.py -v`
Expected: 5 passed, 0 failed.

- [ ] **Step 4: Verify no other tests regressed**

Run: `python3 -m pytest tests/ -v`
Expected: 475 passed, 0 failed (was 470 passed / 5 failed). If any new failure appears, stop and investigate before proceeding.

- [ ] **Step 5: Commit**

```bash
git add tests/test_cli_lineage_json.py
git commit -m "$(cat <<'EOF'
fix(tests): pass PYTHONPATH to lineage CLI subprocess

The _run_cli helper shells out via `python -m frost`. The subprocess
does not inherit pytest's pyproject `pythonpath` setting, so the import
fails unless frost is editable-installed in the active interpreter.
Passing env with PYTHONPATH=<repo>/src removes the install-state
dependency. Restores 5 tests to green.
EOF
)"
```

---

## Task 2: Modernize typing imports in `resources.py`

**Files:**
- Modify: `src/frost/resources.py:1-26` (imports + RESOURCE_QUERIES annotation)
- Modify: `src/frost/resources.py:31-93` (function signature + body annotations)

**Why second:** Pure mechanical change. Makes Task 3 easier (don't have to keep mixing `Dict`/`List` with new `dict`/`list`).

- [ ] **Step 1: Read the current file once for context**

Open `src/frost/resources.py` and confirm the imports/annotations match what's described below. If the file has drifted, stop and reconcile before editing.

- [ ] **Step 2: Update imports and annotations**

Edit `src/frost/resources.py`. Three edits:

**Edit A — line 3:** change

```python
from typing import Any, Dict, List
```

to

```python
from typing import Any
```

**Edit B — line 10:** change

```python
RESOURCE_QUERIES: Dict[str, str] = {
```

to

```python
RESOURCE_QUERIES: dict[str, str] = {
```

**Edit C — function signature and locals at lines 31-41:** change

```python
def fetch_resources(connector: Any, database: str) -> Dict[str, Any]:
    """Query Snowflake for all schema-scoped resources in *database*.

    Returns a dict matching the ``frost resources --json`` output schema:
    ``{"database": ..., "resources": [...], "warnings": [...]}``.

    Individual SHOW queries that fail (e.g. insufficient privileges) are
    logged and added to ``warnings`` — they do not abort the operation.
    """
    resources: List[Dict[str, Any]] = []
    warnings: List[str] = []
```

to

```python
def fetch_resources(connector: Any, database: str) -> dict[str, Any]:
    """Query Snowflake for all schema-scoped resources in *database*.

    Returns a dict matching the ``frost resources --json`` output schema:
    ``{"database": ..., "resources": [...], "warnings": [...]}``.

    Individual SHOW queries that fail (e.g. insufficient privileges) are
    logged and added to ``warnings`` — they do not abort the operation.
    """
    resources: list[dict[str, Any]] = []
    warnings: list[str] = []
```

No other changes in this task.

- [ ] **Step 3: Verify resources tests still pass**

Run: `python3 -m pytest tests/test_resources.py -v`
Expected: all green. Behavior is unchanged; only annotations differ.

- [ ] **Step 4: Verify the full suite still passes**

Run: `python3 -m pytest tests/ -v`
Expected: 475 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add src/frost/resources.py
git commit -m "$(cat <<'EOF'
refactor(resources): modernize typing imports

Drop Dict/List in favor of PEP 585 builtin generics (dict/list).
Project requires Python >=3.10. Behavior unchanged.
EOF
)"
```

---

## Task 3: Extract `_parse_show_row` helper

**Files:**
- Modify: `src/frost/resources.py:57-91` (extract per-row dict construction)

**Why last:** Behavior-preserving refactor. Test_resources.py is the contract; if its mocked rows still produce identical output, the extraction is correct.

- [ ] **Step 1: Add the helper**

Edit `src/frost/resources.py`. Insert the new helper above `fetch_resources` (after the `_SKIP_SCHEMAS` constant, before line 31):

```python
def _parse_show_row(
    row: tuple, schema: str, rtype: str
) -> dict[str, Any]:
    """Build one resource record from a Snowflake SHOW result row.

    Snowflake SHOW commands return rows with positional columns; the
    layout varies by object type and Snowflake version. The owner/comment
    indices used here are heuristic:

    - Owner: column 4 for most SHOW commands.
    - Comment: column 17 for SHOW TABLES; column 8 for several others;
      otherwise empty. Last-non-empty fallbacks were considered fragile
      and dropped.

    A future fix should switch to name-based parsing via
    ``cursor.description``; that work is intentionally deferred so this
    one helper is the only place to change.
    """
    name = row[1] if len(row) > 1 else ""
    if isinstance(name, str):
        name = name.split("(")[0].strip().upper()
    created_on = str(row[0]) if row else ""
    owner = str(row[4]) if len(row) > 4 else ""
    comment = ""
    if len(row) > 17 and isinstance(row[17], str):
        comment = row[17]
    elif len(row) > 8 and isinstance(row[8], str):
        comment = row[8]
    return {
        "schema": schema,
        "type": rtype,
        "name": name,
        "fqn": f"{schema}.{name}",
        "created_on": created_on,
        "owner": owner,
        "comment": comment,
    }
```

- [ ] **Step 2: Replace the inner loop body**

In `fetch_resources`, replace lines 68-91 (the body inside `for row in rows:`) so the inner loop becomes:

```python
            for row in rows:
                resources.append(_parse_show_row(row, schema, rtype))
```

The full inner section after this edit should read:

```python
    for schema in schemas:
        fq_schema = f"{database}.{schema}"
        for rtype, query_tpl in RESOURCE_QUERIES.items():
            try:
                rows = connector.execute_single(query_tpl.format(schema=fq_schema))
            except Exception as exc:
                msg = f"Could not list {rtype} in schema {schema}: {exc}"
                log.debug(msg)
                warnings.append(msg)
                continue

            for row in rows:
                resources.append(_parse_show_row(row, schema, rtype))

    return {"database": database, "resources": resources, "warnings": warnings}
```

- [ ] **Step 3: Verify behavior is byte-identical via the existing test suite**

Run: `python3 -m pytest tests/test_resources.py -v`
Expected: all green. `test_resources.py` exercises `fetch_resources` with mocked SHOW rows and asserts on the produced dicts; if these pass, the extraction preserves output exactly.

- [ ] **Step 4: Verify the full suite is still green**

Run: `python3 -m pytest tests/ -v`
Expected: 475 passed, 0 failed.

- [ ] **Step 5: Commit**

```bash
git add src/frost/resources.py
git commit -m "$(cat <<'EOF'
refactor(resources): extract _parse_show_row helper

Move per-row SHOW-result parsing out of the nested loop in
fetch_resources into a module-private helper. Docstring documents
the column-index heuristic (owner @ 4, comment @ 17 or 8) so a
future name-based fix has a single chokepoint. Behavior unchanged.
EOF
)"
```

---

## Final verification

After all three tasks are complete:

- [ ] **Run the full test suite one more time**

Run: `python3 -m pytest tests/ -v --tb=short`
Expected: 475 passed, 0 failed.

- [ ] **Confirm the `resources.py` diff is small and contained**

Run: `git diff main..HEAD -- src/frost/resources.py | wc -l`
Expected: roughly 50-70 lines of diff. If significantly larger, review for scope creep.

- [ ] **Confirm no other source files were touched**

Run: `git diff --stat main..HEAD`
Expected: only `tests/test_cli_lineage_json.py` and `src/frost/resources.py` (plus the design spec already committed).

---

## Out of scope (do not implement)

These were considered in brainstorming and explicitly deferred:

- **Name-based row parsing** via `cursor.description` (Approach B). Would kill the heuristic in `_parse_show_row` but requires verifying `connector.execute_single` plumbing. Separate plan when needed.
- **Connector Protocol typing** to replace `connector: Any` (Approach C). Cross-cutting type change touching multiple modules.
- **Splitting `cli.py` (993 LOC) or `visualizer.py` (1006 LOC).** Big structural work; needs its own brainstorm.
- **Any change to `_cmd_resources` in `cli.py`** — already conforms to the existing `_cmd_*` pattern.
