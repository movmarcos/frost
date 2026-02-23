"""Rich terminal error reporting for frost.

Inspired by the Rust compiler and Elm error style -- shows source
context, highlights the offending line, and suggests the exact fix.
Designed to look polished in any terminal that supports ANSI colours.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
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
