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


@pytest.fixture()
def fixture_project(tmp_path: Path) -> Path:
    """Minimal frost project with 3 objects: VIEW_A -> TABLE_B."""
    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "table_b.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.TABLE_B (id INT);\n"
    )
    (objects / "view_a.sql").write_text(
        "CREATE OR ALTER VIEW PUBLIC.VIEW_A AS SELECT id FROM PUBLIC.TABLE_B;\n"
    )
    (tmp_path / "frost-config.yml").write_text(
        "account: x\nuser: x\nrole: x\nwarehouse: x\ndatabase: DB\n"
        "objects_folder: objects\n"
    )
    return tmp_path


def test_subgraph_json_focus(fixture_project: Path):
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--json",
         "--object", "PUBLIC.VIEW_A", "--depth", "1", "--direction", "both"],
        cwd=fixture_project,
    )
    assert result.returncode == 0, result.stderr
    # Strip any leading log lines before the first '{'.
    stdout = result.stdout
    stdout = stdout[stdout.index("{"):]
    payload = json.loads(stdout)
    assert payload["focus"] == "PUBLIC.VIEW_A"
    assert payload["depth"] == 1
    assert payload["direction"] == "both"
    fqns = {n["fqn"] for n in payload["nodes"]}
    assert fqns == {"PUBLIC.VIEW_A", "PUBLIC.TABLE_B"}


def test_subgraph_unknown_fqn_exits_2(fixture_project: Path):
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--json",
         "--object", "PUBLIC.NO_SUCH", "--depth", "1"],
        cwd=fixture_project,
    )
    assert result.returncode == 2
    stdout = result.stdout
    stdout = stdout[stdout.index("{"):]
    payload = json.loads(stdout)
    assert payload["error"] == "object not found"
    assert payload["fqn"] == "PUBLIC.NO_SUCH"


def test_full_graph_json(fixture_project: Path):
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--json"],
        cwd=fixture_project,
    )
    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    stdout = stdout[stdout.index("{"):]
    payload = json.loads(stdout)
    assert payload["focus"] is None
    assert payload["depth"] is None
    assert payload["direction"] is None
    fqns = {n["fqn"] for n in payload["nodes"]}
    assert fqns == {"PUBLIC.VIEW_A", "PUBLIC.TABLE_B"}


def test_html_output_still_works(fixture_project: Path, tmp_path: Path):
    """Existing `frost lineage --local --output X.html` behaviour preserved."""
    out = fixture_project / "lineage.html"
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--output", str(out)],
        cwd=fixture_project,
    )
    assert result.returncode == 0, result.stderr
    assert out.exists()
    html = out.read_text()
    assert "<html" in html.lower()
