# Design: Fix lineage-JSON CLI tests + simplify resources module

**Date:** 2026-04-20
**Scope:** Python CLI (`src/frost/`) — recently-touched code only
**Approach:** A (minimal) of three considered

## Background

Two pieces of work, bundled because they share a session and verification surface:

1. **5 failing tests** in `tests/test_cli_lineage_json.py` — all fail with `No module named frost`. Memory tagged them as "pre-existing failures, not regressions." Investigation confirms they are environment plumbing, not code defects.
2. **Recently-added `src/frost/resources.py`** (93 LOC, shipped with the new `frost resources --json` subcommand) — small surface, but contains dated typing imports and a self-admitted fragile column-index heuristic worth localizing.

Both are in scope for the `simplify` skill: review changed code for reuse, quality, and efficiency, then fix issues found.

## Root cause: failing tests

`pyproject.toml` sets `pythonpath = ["src", "tests"]` for pytest discovery. When `_run_cli` shells out via `subprocess.run([sys.executable, "-m", "frost", ...])`, the **subprocess does not inherit pytest's pythonpath** — it gets a clean `PYTHONPATH` from `os.environ`. If frost is not `pip install -e .`'d into the active interpreter, `python -m frost` fails to import.

The fix is at the test-helper level: pass an `env` mapping to `subprocess.run` that includes `PYTHONPATH` pointing at the repo's `src/` directory. This matches what pytest itself does in-process and removes the install-state dependency.

## Changes

### 1. `tests/test_cli_lineage_json.py:13-20`

Modify `_run_cli` to pass `env`:

- Compute repo `src/` path: `SRC_DIR = Path(__file__).resolve().parents[1] / "src"` (module-level constant)
- Build env via `{**os.environ, "PYTHONPATH": str(SRC_DIR)}` — overwrites any inherited PYTHONPATH (acceptable; tests should not depend on outer PYTHONPATH state)
- Pass `env=...` to `subprocess.run`
- Add `import os` at the top of the file

No other test changes. No fixture changes. No conftest changes.

### 2. `src/frost/resources.py`

Three localized edits:

- **Line 3 (`from typing import Any, Dict, List`)** — drop `Dict` and `List`, keep `Any`. Use `dict[str, Any]` / `list[...]` inline. Project requires Python ≥3.10 so PEP 585 generics are available.
- **Lines 68-91 (per-row dict construction inside the inner loop)** — extract into `_parse_show_row(row, schema, rtype) -> dict[str, Any]`. Module-private helper. Docstring explicitly states that owner/comment column indices are heuristic and Snowflake-version-sensitive, so a future name-based parsing fix has a single chokepoint.
- **`except Exception` at lines 48 and 62** — keep both. Per-resource SHOW failures legitimately want to be warnings (insufficient privileges, etc.). Schema-discovery failure is also a connector-level error that should surface as a single warning, not crash the JSON output. Documented behavior.

No signature changes. No changes to `RESOURCE_QUERIES` or `_SKIP_SCHEMAS`.

### 3. Out of scope (explicitly deferred)

- Name-based row parsing via `cursor.description` (Approach B from brainstorm)
- `Connector` Protocol typing to replace `connector: Any` (Approach C)
- Splitting `cli.py` (993 LOC) or `visualizer.py` (1006 LOC)
- Any changes to `_cmd_resources` in `cli.py` — already follows existing `_cmd_*` pattern

## Verification

Three commands, in order:

1. `pytest tests/test_cli_lineage_json.py -v` → 5 pass / 0 fail
2. `pytest tests/test_resources.py -v` → still green (untouched behavior; `_parse_show_row` must produce identical output)
3. `pytest tests/ -v` → 475 pass / 0 fail (was 470 / 5)

Behavior preservation for `_parse_show_row` is the load-bearing claim; `test_resources.py` is the safety net.

## Risk

Very low.

- Test fix is mechanical; only affects subprocess invocation env.
- Resources edits are internal refactor with byte-identical output expected.
- No public API changes. No new dependencies. No connector behavior change.

## Success criteria

- All 5 previously-failing tests pass.
- Full test suite green.
- `resources.py` no longer imports `Dict`/`List` from typing.
- Column-index parsing localized to one helper with a docstring explaining the fragility.
- No other files touched.
