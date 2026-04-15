"""Lockdown test: `frost graph --json` must not invoke LineageScanner.

The VSCode extension calls `frost graph --json` on activation for every
project. Today the call path goes Deployer.plan -> _build_graph ->
LineageScanner.scan, which scans every procedure / function / task /
stream body for regex matches — pure waste for the graph command, and a
significant contributor to activation memory pressure on 1700-object
workspaces. This test locks the fix in.
"""

from argparse import Namespace
from unittest.mock import patch

from frost import cli
from frost.config import FrostConfig


def _write_config(root, objects_folder):
    (root / "frost-config.yml").write_text(
        "account: x\nuser: x\nrole: x\nwarehouse: x\n"
        "database: DB\nobjects_folder: " + str(objects_folder) + "\n"
    )


def test_graph_command_does_not_invoke_lineage_scanner(tmp_path, capsys):
    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "t.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.T (id INT);\n"
    )
    _write_config(tmp_path, objects)

    cfg = FrostConfig(
        account="x", user="x", role="x", warehouse="x",
        database="DB", objects_folder=str(objects),
    )

    with patch("frost.deployer.LineageScanner") as scanner_cls:
        args = Namespace(json=True)
        cli._cmd_graph(cfg, args)
        scanner_cls.assert_not_called()


def test_build_graph_skip_lineage_produces_empty_lineage(tmp_path):
    """Calling _build_graph with include_lineage=False leaves _lineage empty."""
    from frost.deployer import Deployer

    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "t.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.T (id INT);\n"
    )
    cfg = FrostConfig(
        account="x", user="x", role="x", warehouse="x",
        database="DB", objects_folder=str(objects),
    )
    deployer = Deployer(cfg)
    deployer._scan_and_parse()
    deployer._build_graph(include_lineage=False)
    assert deployer._graph.lineage == {}


def test_graph_text_mode_still_invokes_lineage_scanner(tmp_path):
    """Text-mode `frost graph` (no --json) preserves the Procedure Lineage
    section in its human-readable output, so it must still run the scanner."""
    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "t.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.T (id INT);\n"
    )
    _write_config(tmp_path, objects)

    cfg = FrostConfig(
        account="x", user="x", role="x", warehouse="x",
        database="DB", objects_folder=str(objects),
    )

    with patch("frost.deployer.LineageScanner") as scanner_cls:
        args = Namespace(json=False)
        cli._cmd_graph(cfg, args)
        scanner_cls.assert_called_once()
