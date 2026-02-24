"""Rich terminal error reporting for frost.

Inspired by the Rust compiler and Elm error style -- shows source
context, highlights the offending line, and suggests the exact fix.
Designed to look polished in any terminal that supports ANSI colours.
"""

from __future__ import annotations

import os
import re
import sys
import textwrap
from dataclasses import dataclass, field
from typing import List, Optional, Sequence


# ------------------------------------------------------------------
# ANSI colour helpers (disabled when NO_COLOR is set or not a TTY)
# ------------------------------------------------------------------

def _supports_colour() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FROST_COLOR", "").lower() in ("1", "true", "yes"):
        return True
    return hasattr(sys.stderr, "isatty") and sys.stderr.isatty()


_USE_COLOUR = _supports_colour()


def _ansi(code: str) -> str:
    return f"\033[{code}m" if _USE_COLOUR else ""


# Palette
_RESET   = _ansi("0")
_BOLD    = _ansi("1")
_DIM     = _ansi("2")
_RED     = _ansi("31")
_GREEN   = _ansi("32")
_YELLOW  = _ansi("33")
_CYAN    = _ansi("36")
_WHITE   = _ansi("37")
_BG_RED  = _ansi("41")
_BRED    = _ansi("1;31")
_BGREEN  = _ansi("1;32")
_BYELLOW = _ansi("1;33")
_BCYAN   = _ansi("1;36")
_BWHITE  = _ansi("1;37")
_MAGENTA = _ansi("35")
_BMAGENTA = _ansi("1;35")


# ------------------------------------------------------------------
# Violation data model
# ------------------------------------------------------------------

@dataclass
class Violation:
    """A single CREATE OR ALTER policy violation."""

    file_path: str
    object_type: str
    fqn: str
    found_form: str        # e.g. "CREATE OR REPLACE" / "CREATE"
    suggested_form: str    # e.g. "CREATE OR ALTER"
    source_line: str       # the raw SQL line containing the CREATE
    line_number: int       # 1-based line number in the file


class PolicyError(Exception):
    """Raised when one or more SQL files violate frost policies."""

    def __init__(self, violations: List[Violation]):
        self.violations = violations
        count = len(violations)
        super().__init__(
            f"{count} policy violation{'s' if count != 1 else ''} found"
        )


# ------------------------------------------------------------------
# Reporter
# ------------------------------------------------------------------

_BOX_TL = "+"
_BOX_TR = "+"
_BOX_BL = "+"
_BOX_BR = "+"
_BOX_H  = "-"
_BOX_V  = "|"
_ARROW  = "-->"
_POINTER = "^^^"


def _gutter(n: int, width: int = 4) -> str:
    """Format a line-number gutter."""
    return f"{_DIM}{str(n).rjust(width)} {_BOX_V}{_RESET} "


def _empty_gutter(width: int = 4) -> str:
    return f"{_DIM}{' ' * width} {_BOX_V}{_RESET} "


def report_violations(violations: Sequence[Violation]) -> str:
    """Build a rich, multi-violation error report string."""
    lines: List[str] = []
    count = len(violations)

    # -- Header banner ------------------------------------------------
    banner_text = f" FROST  //  {count} violation{'s' if count != 1 else ''} found "
    banner_width = max(64, len(banner_text) + 4)
    pad = banner_width - len(banner_text) - 2
    lpad = pad // 2
    rpad = pad - lpad

    lines.append("")
    lines.append(
        f"{_BRED}{_BOX_TL}{_BOX_H * (banner_width - 2)}{_BOX_TR}{_RESET}"
    )
    lines.append(
        f"{_BRED}{_BOX_V}{_RESET}"
        f"{_BWHITE}{' ' * lpad}{banner_text}{' ' * rpad}{_RESET}"
        f"{_BRED}{_BOX_V}{_RESET}"
    )
    lines.append(
        f"{_BRED}{_BOX_BL}{_BOX_H * (banner_width - 2)}{_BOX_BR}{_RESET}"
    )
    lines.append("")

    for i, v in enumerate(violations, 1):
        lines.extend(_format_violation(i, v, count))
        if i < count:
            lines.append("")

    # -- Footer summary -----------------------------------------------
    lines.append("")
    lines.append(
        f"{_BRED}error:{_RESET} "
        f"{_BWHITE}{count} file{'s' if count != 1 else ''} "
        f"must be updated to use CREATE OR ALTER{_RESET}"
    )
    lines.append(
        f"{_DIM}help:{_RESET}  Snowflake supports CREATE OR ALTER for these types."
    )
    lines.append(
        f"{_DIM}help:{_RESET}  Replace the highlighted statement in each file."
    )
    lines.append(
        f"{_DIM}docs:{_RESET}  {_CYAN}https://docs.snowflake.com/en/sql-reference/sql/create-or-alter{_RESET}"
    )
    lines.append("")

    return "\n".join(lines)


def _format_violation(index: int, v: Violation, total: int) -> List[str]:
    """Format a single violation with source context and suggested fix."""
    lines: List[str] = []

    # -- Title line ---------------------------------------------------
    tag = f"[{index}/{total}]"
    lines.append(
        f"{_BRED}error{_RESET}{_DIM}{tag}{_RESET}: "
        f"{_BWHITE}{v.object_type} must use CREATE OR ALTER{_RESET}"
    )

    # -- File location ------------------------------------------------
    lines.append(
        f"  {_BCYAN}{_ARROW}{_RESET} {_CYAN}{v.file_path}:{v.line_number}{_RESET}"
    )

    # -- Source context -----------------------------------------------
    gw = max(4, len(str(v.line_number)) + 1)

    # Empty gutter line (top border)
    lines.append(f"{_DIM}{' ' * gw} {_BOX_V}{_RESET}")

    # The offending source line in red
    lines.append(
        f"{_RED}{str(v.line_number).rjust(gw)} {_BOX_V}{_RESET} "
        f"{_RED}{v.source_line.rstrip()}{_RESET}"
    )

    # Pointer line -- underline the "CREATE OR REPLACE" / "CREATE" part
    stripped = v.source_line.rstrip()
    upper = stripped.upper()
    start = upper.find(v.found_form.upper())
    if start >= 0:
        pointer_line = " " * start + "^" * len(v.found_form)
        lines.append(
            f"{' ' * gw} {_DIM}{_BOX_V}{_RESET} "
            f"{_BRED}{pointer_line} use {v.suggested_form} here{_RESET}"
        )
    else:
        lines.append(
            f"{' ' * gw} {_DIM}{_BOX_V}{_RESET} "
            f"{_BRED}{_POINTER} use {v.suggested_form}{_RESET}"
        )

    # -- Suggested fix in green ---------------------------------------
    fixed_line = stripped.replace(
        v.found_form, v.suggested_form
    )
    # If the original didn't have OR REPLACE (just CREATE TABLE), inject OR ALTER
    if fixed_line == stripped:
        fixed_line = stripped.upper().replace(
            f"CREATE {v.object_type}",
            f"CREATE OR ALTER {v.object_type}",
        )
        # Preserve original casing for the rest
        if fixed_line == stripped.upper():
            fixed_line = stripped  # fallback

    lines.append(f"{_DIM}{' ' * gw} {_BOX_V}{_RESET}")
    lines.append(
        f"{_BGREEN}{'fix'.rjust(gw)} {_BOX_V}{_RESET} "
        f"{_GREEN}{fixed_line.rstrip()}{_RESET}"
    )
    lines.append(f"{_DIM}{' ' * gw} {_BOX_V}{_RESET}")

    # -- Object info --------------------------------------------------
    lines.append(
        f"{_DIM}{' ' * gw} = object:{_RESET} {_YELLOW}{v.fqn}{_RESET}"
    )

    return lines


# ==================================================================
# Snowflake deployment error reporting
# ==================================================================

@dataclass
class DeployError:
    """A structured Snowflake deployment error."""

    fqn: str
    object_type: str
    file_path: str
    sql: str                        # the SQL that was executed
    error_message: str              # raw Snowflake error message
    error_code: Optional[str] = None  # Snowflake SQL error code
    blocked: List[str] = field(default_factory=list)  # FQNs blocked by this failure
    ai_suggestion: Optional[str] = None  # Cortex AI fix suggestion


# -- Snowflake error code lookup -----------------------------------

_SF_ERROR_HINTS = {
    "000904": "Invalid identifier -- check column/object names and schema context.",
    "001003": "SQL compilation error -- review the SQL syntax.",
    "001038": "Object not found -- it may not exist yet or the schema/database context may be wrong.",
    "001304": "Table already has a primary key -- remove the duplicate PK definition.",
    "002003": "SQL execution error -- a runtime error occurred during SQL execution.",
    "002014": "Function/procedure not found -- check the name, argument types, and schema.",
    "002043": "SQL compilation error on expression -- check computed column definitions.",
    "090106": "Permission denied -- the current role lacks the required privilege.",
    "002002": "Numeric value out of range -- check data types and precision.",
    "100035": "Stage not found -- verify the stage name and schema.",
    "100038": "File format not found -- check the file format reference.",
    "091002": "Role does not exist -- verify the role name in GRANT statements.",
}


# Lines produced by the Snowflake Python connector internals that
# should never appear in user-facing output.
_INTERNAL_NOISE = re.compile(
    r"(?:"
    r"handed_over\s*="
    r"|Error\.hand_to_other_handler"
    r"|InterfaceError"
    r"|File\s+\""
    r"|Traceback\s*\("
    r"|^\s+\^"
    r"|cursor\.execute"
    r"|snowflake\.connector"
    r"|_exec_with_retry"
    r"|raise_prog_error"
    r")",
    re.IGNORECASE,
)


def _parse_snowflake_error(raw: str) -> tuple:
    """Extract (error_code, clean_message) from a Snowflake exception string.

    Strips Python connector internal lines (``handed_over = ...``,
    traceback fragments, etc.) so only the Snowflake error text remains.
    """
    # 1. Strip connector internal noise line-by-line
    cleaned_lines = [
        ln for ln in raw.splitlines()
        if not _INTERNAL_NOISE.search(ln)
    ]
    cleaned = "\n".join(cleaned_lines).strip()
    if not cleaned:
        cleaned = raw.strip()  # fallback -- keep original

    # 2. Try to extract the 6-digit error code + message
    # Format: "000904 (42000): SQL compilation error:\n..."
    m = re.match(r"(\d{6})\s*\([^)]*\):\s*(.*)", cleaned, re.DOTALL)
    if m:
        return m.group(1), m.group(2).strip()

    # 3. Code might be at the start without parenthesised SQL state
    m2 = re.match(r"(\d{6}):\s*(.*)", cleaned, re.DOTALL)
    if m2:
        return m2.group(1), m2.group(2).strip()

    # 4. No code found -- return the cleaned text as-is
    return None, cleaned


def _sql_preview(sql: str, max_lines: int = 10) -> List[str]:
    """Return numbered SQL lines for display (trimmed to max_lines)."""
    raw_lines = sql.strip().splitlines()
    show = raw_lines[:max_lines]
    result = []
    gw = len(str(len(show))) + 1
    for i, line in enumerate(show, 1):
        result.append(f"{_DIM}{str(i).rjust(gw)} {_BOX_V}{_RESET} {line.rstrip()}")
    if len(raw_lines) > max_lines:
        remaining = len(raw_lines) - max_lines
        result.append(
            f"{_DIM}{' ' * gw} {_BOX_V}  ... {remaining} more line{'s' if remaining != 1 else ''}{_RESET}"
        )
    return result


def report_deploy_errors(errors: Sequence[DeployError]) -> str:
    """Build a rich error report for Snowflake deployment failures."""
    lines: List[str] = []
    count = len(errors)

    # -- Header banner ------------------------------------------------
    banner_text = f" FROST  //  {count} deployment failure{'s' if count != 1 else ''} "
    banner_width = max(64, len(banner_text) + 4)
    pad = banner_width - len(banner_text) - 2
    lpad = pad // 2
    rpad = pad - lpad

    lines.append("")
    lines.append(f"{_BRED}{_BOX_TL}{_BOX_H * (banner_width - 2)}{_BOX_TR}{_RESET}")
    lines.append(
        f"{_BRED}{_BOX_V}{_RESET}"
        f"{_BWHITE}{' ' * lpad}{banner_text}{' ' * rpad}{_RESET}"
        f"{_BRED}{_BOX_V}{_RESET}"
    )
    lines.append(f"{_BRED}{_BOX_BL}{_BOX_H * (banner_width - 2)}{_BOX_BR}{_RESET}")
    lines.append("")

    for i, err in enumerate(errors, 1):
        lines.extend(_format_deploy_error(i, err, count))
        if i < count:
            lines.append("")

    # -- Footer -------------------------------------------------------
    lines.append("")
    lines.append(
        f"{_BRED}error:{_RESET} "
        f"{_BWHITE}{count} object{'s' if count != 1 else ''} "
        f"failed to deploy{_RESET}"
    )

    total_blocked = sum(len(e.blocked) for e in errors)
    if total_blocked:
        lines.append(
            f"{_BYELLOW}warn:{_RESET}  "
            f"{total_blocked} downstream object{'s' if total_blocked != 1 else ''} "
            f"skipped due to failed dependencies"
        )

    lines.append(
        f"{_DIM}help:{_RESET}  Fix the SQL files above and run {_CYAN}frost deploy{_RESET} again."
    )
    lines.append("")

    return "\n".join(lines)


def _format_deploy_error(index: int, err: DeployError, total: int) -> List[str]:
    """Format a single deployment error with SQL context and Snowflake details."""
    lines: List[str] = []

    code, message = _parse_snowflake_error(err.error_message)
    if err.error_code:
        code = err.error_code

    # -- Title --------------------------------------------------------
    tag = f"[{index}/{total}]"
    code_str = f" ({code})" if code else ""
    lines.append(
        f"{_BRED}error{_RESET}{_DIM}{tag}{_RESET}: "
        f"{_BWHITE}failed to deploy {err.object_type} {err.fqn}{code_str}{_RESET}"
    )

    # -- File location ------------------------------------------------
    lines.append(
        f"  {_BCYAN}{_ARROW}{_RESET} {_CYAN}{err.file_path}{_RESET}"
    )

    # -- Snowflake error message --------------------------------------
    lines.append(f"{_DIM}     {_BOX_V}{_RESET}")

    # Wrap long messages nicely
    msg_lines = message.splitlines()
    for ml in msg_lines[:6]:
        lines.append(
            f"{_DIM}     {_BOX_V}{_RESET} {_RED}{ml.strip()}{_RESET}"
        )
    if len(msg_lines) > 6:
        lines.append(
            f"{_DIM}     {_BOX_V}  ... {len(msg_lines) - 6} more lines{_RESET}"
        )

    # -- Error hint ---------------------------------------------------
    if code and code in _SF_ERROR_HINTS:
        lines.append(f"{_DIM}     {_BOX_V}{_RESET}")
        lines.append(
            f"{_BGREEN}hint {_BOX_V}{_RESET} "
            f"{_GREEN}{_SF_ERROR_HINTS[code]}{_RESET}"
        )

    # -- SQL preview --------------------------------------------------
    lines.append(f"{_DIM}     {_BOX_V}{_RESET}")
    lines.append(
        f"{_DIM}     {_BOX_V}{_RESET} {_DIM}SQL sent to Snowflake:{_RESET}"
    )
    lines.extend(f"     {l}" for l in _sql_preview(err.sql))

    # -- Blocked dependents -------------------------------------------
    if err.blocked:
        lines.append(f"{_DIM}     {_BOX_V}{_RESET}")
        lines.append(
            f"{_BYELLOW}     {_BOX_V}{_RESET} "
            f"{_YELLOW}Blocked {len(err.blocked)} dependent{'s' if len(err.blocked) != 1 else ''}:{_RESET}"
        )
        for b in err.blocked[:8]:
            lines.append(
                f"{_DIM}     {_BOX_V}{_RESET}   {_YELLOW}{b}{_RESET}"
            )
        if len(err.blocked) > 8:
            lines.append(
                f"{_DIM}     {_BOX_V}   ... and {len(err.blocked) - 8} more{_RESET}"
            )

    # -- Cortex AI suggestion -----------------------------------------
    if err.ai_suggestion:
        lines.append(f"{_DIM}     {_BOX_V}{_RESET}")
        lines.append(
            f"{_BMAGENTA}  ai {_BOX_V}{_RESET} "
            f"{_MAGENTA}Cortex suggestion:{_RESET}"
        )
        for sl in textwrap.wrap(err.ai_suggestion, width=72):
            lines.append(
                f"{_DIM}     {_BOX_V}{_RESET}   {_MAGENTA}{sl}{_RESET}"
            )

    lines.append(f"{_DIM}     {_BOX_V}{_RESET}")

    return lines


# ==================================================================
# Deployment summary
# ==================================================================

def report_deploy_summary(
    total: int,
    deployed: int,
    skipped: int,
    failed: int,
    elapsed: float,
) -> str:
    """Build a branded deployment summary banner."""
    lines: List[str] = []

    if failed == 0:
        status = f"{_BGREEN}SUCCESS{_RESET}"
        border_col = _BGREEN
    else:
        status = f"{_BRED}FAILED{_RESET}"
        border_col = _BRED

    width = 52

    lines.append("")
    lines.append(f"{border_col}{_BOX_TL}{_BOX_H * (width - 2)}{_BOX_TR}{_RESET}")
    lines.append(
        f"{border_col}{_BOX_V}{_RESET}  "
        f"FROST  //  Deployment {status}"
        f"{' ' * 2}{border_col}{_BOX_V}{_RESET}"
    )
    lines.append(f"{border_col}{_BOX_V}{_BOX_H * (width - 2)}{_BOX_V}{_RESET}")

    def _row(label: str, value: str, col: str = "") -> str:
        content = f"  {label:<20} {col}{value}{_RESET if col else ''}"
        # Pad to fill box width (accounting for ANSI codes)
        return f"{border_col}{_BOX_V}{_RESET}{content:<{width - 2 + (len(col) + len(_RESET) if col else 0)}}{border_col}{_BOX_V}{_RESET}"

    lines.append(_row("Total objects:", str(total)))
    lines.append(_row("Deployed:", str(deployed), _BGREEN if deployed else ""))
    lines.append(_row("Skipped:", str(skipped), _DIM if skipped else ""))
    lines.append(_row("Failed:", str(failed), _BRED if failed else ""))
    lines.append(_row("Elapsed:", f"{elapsed:.1f}s"))

    lines.append(f"{border_col}{_BOX_BL}{_BOX_H * (width - 2)}{_BOX_BR}{_RESET}")
    lines.append("")

    return "\n".join(lines)


# ==================================================================
# Data load summary
# ==================================================================

def report_load_summary(
    total: int,
    loaded: int,
    failed: int,
) -> str:
    """Build a branded data loading summary banner."""
    lines: List[str] = []

    if failed == 0:
        status = f"{_BGREEN}SUCCESS{_RESET}"
        border_col = _BGREEN
    else:
        status = f"{_BRED}FAILED{_RESET}"
        border_col = _BRED

    width = 52

    lines.append("")
    lines.append(f"{border_col}{_BOX_TL}{_BOX_H * (width - 2)}{_BOX_TR}{_RESET}")
    lines.append(
        f"{border_col}{_BOX_V}{_RESET}  "
        f"FROST  //  Data Load {status}"
        f"{' ' * 3}{border_col}{_BOX_V}{_RESET}"
    )
    lines.append(f"{border_col}{_BOX_V}{_BOX_H * (width - 2)}{_BOX_V}{_RESET}")

    def _row(label: str, value: str, col: str = "") -> str:
        content = f"  {label:<20} {col}{value}{_RESET if col else ''}"
        return f"{border_col}{_BOX_V}{_RESET}{content:<{width - 2 + (len(col) + len(_RESET) if col else 0)}}{border_col}{_BOX_V}{_RESET}"

    lines.append(_row("Total files:", str(total)))
    lines.append(_row("Loaded:", str(loaded), _BGREEN if loaded else ""))
    lines.append(_row("Failed:", str(failed), _BRED if failed else ""))

    lines.append(f"{border_col}{_BOX_BL}{_BOX_H * (width - 2)}{_BOX_BR}{_RESET}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Data-quality test report
# ---------------------------------------------------------------------------


def report_test_results(results: Sequence) -> str:
    """Build a rich ANSI report for data-quality test results.

    *results* is a sequence of ``frost.tester.TestResult`` objects.
    """
    from frost.tester import TestResult  # local import to avoid circular dep

    lines: list[str] = []

    passed_count = sum(1 for r in results if r.passed)
    failed_count = len(results) - passed_count

    # --- per-test lines ---
    for r in results:
        tc = r.test_case
        tag = f"{_BGREEN}PASS{_RESET}" if r.passed else f"{_BRED}FAIL{_RESET}"
        col_part = f" [{tc.column}]" if tc.column else ""
        lines.append(f"  {tag}  {tc.name}  ({tc.test} on {tc.source}{col_part})")
        if not r.passed:
            lines.append(f"        {_BYELLOW}{r.message}{_RESET}")
            if r.failing_rows:
                for detail in r.failing_rows[:5]:
                    lines.append(f"        {_DIM}· {detail}{_RESET}")
                if len(r.failing_rows) > 5:
                    lines.append(
                        f"        {_DIM}… and {len(r.failing_rows) - 5} more{_RESET}"
                    )

    lines.append("")

    # --- summary banner ---
    all_ok = failed_count == 0
    width = 44
    border_col = _BGREEN if all_ok else _BRED
    status = f"{_BGREEN}ALL PASSED{_RESET}" if all_ok else f"{_BRED}FAILURES{_RESET}"

    lines.append(f"{border_col}{_BOX_TL}{_BOX_H * (width - 2)}{_BOX_TR}{_RESET}")
    lines.append(
        f"{border_col}{_BOX_V}{_RESET}  "
        f"FROST  //  Data Tests {status}"
        f"{' ' * 2}{border_col}{_BOX_V}{_RESET}"
    )
    lines.append(f"{border_col}{_BOX_V}{_BOX_H * (width - 2)}{_BOX_V}{_RESET}")

    def _trow(label: str, value: str, col: str = "") -> str:
        content = f"  {label:<20} {col}{value}{_RESET if col else ''}"
        return (
            f"{border_col}{_BOX_V}{_RESET}"
            f"{content:<{width - 2 + (len(col) + len(_RESET) if col else 0)}}"
            f"{border_col}{_BOX_V}{_RESET}"
        )

    lines.append(_trow("Total tests:", str(len(results))))
    lines.append(_trow("Passed:", str(passed_count), _BGREEN if passed_count else ""))
    lines.append(_trow("Failed:", str(failed_count), _BRED if failed_count else ""))

    lines.append(f"{border_col}{_BOX_BL}{_BOX_H * (width - 2)}{_BOX_BR}{_RESET}")
    lines.append("")

    return "\n".join(lines)
