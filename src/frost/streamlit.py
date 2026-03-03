"""Streamlit app discovery and deployment via Snowflake CLI (snow).

frost discovers Streamlit apps by scanning for ``snowflake.yml`` project
definition files.  Deployment is delegated to ``snow streamlit deploy``
from the Snowflake CLI, which handles file staging and the
``CREATE STREAMLIT`` statement.

Usage from frost CLI::

    frost streamlit list          # show discovered apps
    frost streamlit deploy        # deploy all apps
    frost streamlit deploy myapp  # deploy a single app
    frost streamlit teardown myapp
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

log = logging.getLogger("frost")


# ── Data model ───────────────────────────────────────────────

@dataclass
class StreamlitApp:
    """Metadata about a discovered Streamlit app."""

    name: str
    """Logical app name (from snowflake.yml or directory name)."""

    directory: str
    """Absolute path to the app directory (contains snowflake.yml)."""

    main_file: str
    """Entry-point Python file (default: streamlit_app.py)."""

    stage: str
    """Stage used for deployment."""

    schema: str
    """Target Snowflake schema."""

    warehouse: str
    """Warehouse to run the Streamlit app."""

    query_warehouse: str
    """Warehouse for queries executed by the app."""

    title: str
    """Human-readable title displayed in Snowflake."""

    definition_file: str
    """Absolute path to snowflake.yml."""

    comment: str = ""
    """Optional comment / description."""

    external_access_integrations: List[str] = field(default_factory=list)
    """External access integrations for the app."""

    imports: List[str] = field(default_factory=list)
    """Additional stage imports."""

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "directory": self.directory,
            "main_file": self.main_file,
            "stage": self.stage,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "query_warehouse": self.query_warehouse,
            "title": self.title,
            "definition_file": self.definition_file,
            "comment": self.comment,
            "external_access_integrations": self.external_access_integrations,
            "imports": self.imports,
        }


@dataclass
class StreamlitDeployResult:
    """Result of a Streamlit deployment."""

    name: str
    success: bool
    message: str
    url: str = ""


# ── Discovery ────────────────────────────────────────────────

def discover_apps(
    root: str,
    max_depth: int = 5,
) -> List[StreamlitApp]:
    """Walk *root* looking for ``snowflake.yml`` files that define
    Streamlit apps.  Returns a list of :class:`StreamlitApp`.

    The Snowflake CLI project definition format stores Streamlit config
    under the ``streamlit`` key (v1) or under ``entities`` with
    ``type: streamlit`` (v2).
    """
    apps: List[StreamlitApp] = []
    root_path = Path(root).resolve()

    for yml_path in _find_snowflake_ymls(root_path, max_depth):
        try:
            parsed = _parse_snowflake_yml(yml_path)
            apps.extend(parsed)
        except Exception as exc:
            log.warning("Skipping %s: %s", yml_path, exc)

    # Sort by name for deterministic output
    apps.sort(key=lambda a: a.name)
    return apps


def _find_snowflake_ymls(root: Path, max_depth: int) -> List[Path]:
    """Recursively find snowflake.yml / snowflake.yaml files."""
    results: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Respect max depth
        depth = len(Path(dirpath).relative_to(root).parts)
        if depth > max_depth:
            dirnames.clear()
            continue
        # Skip hidden dirs and common non-project dirs
        dirnames[:] = [
            d for d in dirnames
            if not d.startswith(".")
            and d not in ("node_modules", "__pycache__", ".venv", "venv")
        ]
        for fname in filenames:
            if fname in ("snowflake.yml", "snowflake.yaml"):
                results.append(Path(dirpath) / fname)
    return results


def _parse_snowflake_yml(yml_path: Path) -> List[StreamlitApp]:
    """Parse a ``snowflake.yml`` and extract Streamlit app definitions."""
    with open(yml_path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    app_dir = str(yml_path.parent)
    apps: List[StreamlitApp] = []

    # ── V2 format: definition_version: 2, entities ──────────
    if data.get("definition_version") and data.get("entities"):
        for entity_name, entity in data["entities"].items():
            if entity.get("type", "").lower() != "streamlit":
                continue
            st = entity
            apps.append(StreamlitApp(
                name=entity_name,
                directory=app_dir,
                main_file=st.get("main_file", "streamlit_app.py"),
                stage=st.get("stage", f"@{entity_name}_stage"),
                schema=st.get("schema", "PUBLIC"),
                warehouse=st.get("warehouse", ""),
                query_warehouse=st.get("query_warehouse", ""),
                title=st.get("title", entity_name),
                definition_file=str(yml_path),
                comment=st.get("comment", ""),
                external_access_integrations=st.get(
                    "external_access_integrations", []
                ),
                imports=st.get("imports", []),
            ))

    # ── V1 format: streamlit top-level key ──────────────────
    elif "streamlit" in data:
        items = data["streamlit"]
        # Could be a single dict or a list
        if isinstance(items, dict):
            items = [items]
        for i, st in enumerate(items):
            name = st.get("name", yml_path.parent.name)
            apps.append(StreamlitApp(
                name=name,
                directory=app_dir,
                main_file=st.get("main_file", "streamlit_app.py"),
                stage=st.get("stage", f"@{name}_stage"),
                schema=st.get("schema", "PUBLIC"),
                warehouse=st.get("warehouse", ""),
                query_warehouse=st.get("query_warehouse", ""),
                title=st.get("title", name),
                definition_file=str(yml_path),
                comment=st.get("comment", ""),
                external_access_integrations=st.get(
                    "external_access_integrations", []
                ),
                imports=st.get("imports", []),
            ))

    return apps


# ── Snow CLI integration ─────────────────────────────────────

def find_snow_cli() -> Optional[str]:
    """Return the path to ``snow`` if installed, else ``None``."""
    return shutil.which("snow")


def deploy_app(
    app: StreamlitApp,
    snow_path: Optional[str] = None,
    *,
    replace: bool = True,
    open_browser: bool = False,
    connection: Optional[str] = None,
    account: Optional[str] = None,
    database: Optional[str] = None,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
) -> StreamlitDeployResult:
    """Deploy a Streamlit app using ``snow streamlit deploy``.

    Parameters
    ----------
    app : StreamlitApp
        The app to deploy (must have a valid *directory*).
    snow_path : str, optional
        Path to the ``snow`` binary.  Auto-detected if ``None``.
    replace : bool
        If ``True`` (default), pass ``--replace`` to recreate the app.
    open_browser : bool
        If ``True``, pass ``--open`` to open the app after deploy.
    connection : str, optional
        Named Snow CLI connection to use (``--connection``).
    account, database, role, warehouse : str, optional
        Override Snowflake connection parameters.
    """
    snow = snow_path or find_snow_cli()
    if not snow:
        return StreamlitDeployResult(
            name=app.name,
            success=False,
            message=(
                "Snowflake CLI (snow) not found. "
                "Install it: pip install snowflake-cli-labs   or   "
                "brew install snowflake-cli"
            ),
        )

    cmd = [snow, "streamlit", "deploy"]
    if replace:
        cmd.append("--replace")
    if open_browser:
        cmd.append("--open")
    if connection:
        cmd.extend(["--connection", connection])
    if account:
        cmd.extend(["--account", account])
    if database:
        cmd.extend(["--database", database])
    if role:
        cmd.extend(["--role", role])
    if warehouse:
        cmd.extend(["--warehouse", warehouse])

    log.info("Deploying Streamlit app '%s' from %s", app.name, app.directory)
    log.debug("Command: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            cwd=app.directory,
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = result.stdout + result.stderr
        if result.returncode == 0:
            url = _extract_url(output)
            return StreamlitDeployResult(
                name=app.name,
                success=True,
                message=output.strip(),
                url=url,
            )
        else:
            return StreamlitDeployResult(
                name=app.name,
                success=False,
                message=output.strip(),
            )
    except subprocess.TimeoutExpired:
        return StreamlitDeployResult(
            name=app.name,
            success=False,
            message="Deployment timed out after 120 seconds",
        )
    except Exception as exc:
        return StreamlitDeployResult(
            name=app.name,
            success=False,
            message=str(exc),
        )


def teardown_app(
    app: StreamlitApp,
    snow_path: Optional[str] = None,
    *,
    connection: Optional[str] = None,
) -> StreamlitDeployResult:
    """Tear down (drop) a Streamlit app using ``snow streamlit teardown``."""
    snow = snow_path or find_snow_cli()
    if not snow:
        return StreamlitDeployResult(
            name=app.name, success=False,
            message="Snowflake CLI (snow) not found.",
        )

    cmd = [snow, "streamlit", "teardown", app.name]
    if connection:
        cmd.extend(["--connection", connection])

    try:
        result = subprocess.run(
            cmd, cwd=app.directory, capture_output=True, text=True, timeout=60,
        )
        output = result.stdout + result.stderr
        return StreamlitDeployResult(
            name=app.name,
            success=result.returncode == 0,
            message=output.strip(),
        )
    except Exception as exc:
        return StreamlitDeployResult(
            name=app.name, success=False, message=str(exc),
        )


def get_app_url(
    app: StreamlitApp,
    snow_path: Optional[str] = None,
    *,
    connection: Optional[str] = None,
) -> Optional[str]:
    """Get the URL of a deployed Streamlit app."""
    snow = snow_path or find_snow_cli()
    if not snow:
        return None

    cmd = [snow, "streamlit", "get-url", app.name]
    if connection:
        cmd.extend(["--connection", connection])

    try:
        result = subprocess.run(
            cmd, cwd=app.directory, capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return None


# ── Helpers ──────────────────────────────────────────────────

def _extract_url(output: str) -> str:
    """Try to extract a Snowflake URL from snow CLI output."""
    import re
    match = re.search(r"https://\S+", output)
    return match.group(0) if match else ""
