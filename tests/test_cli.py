"""Tests for frost.cli -- argument parsing and command dispatch.

We test:
  * _build_parser() produces valid argparse parser
  * Each sub-command parses expected arguments
  * main() dispatches to the correct internal function
  * --version flag
  * --vars JSON parsing
"""

import sys
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest

from frost.cli import main, _build_parser


# ------------------------------------------------------------------
# Argument parser structure
# ------------------------------------------------------------------

def test_parser_plan():
    p = _build_parser()
    args = p.parse_args(["plan"])
    assert args.command == "plan"


def test_parser_deploy():
    p = _build_parser()
    args = p.parse_args(["deploy"])
    assert args.command == "deploy"
    assert args.dry_run is False


def test_parser_deploy_dry_run():
    p = _build_parser()
    args = p.parse_args(["deploy", "--dry-run"])
    assert args.dry_run is True


def test_parser_deploy_no_cortex():
    p = _build_parser()
    args = p.parse_args(["deploy", "--no-cortex"])
    assert args.no_cortex is True


def test_parser_deploy_cortex_model():
    p = _build_parser()
    args = p.parse_args(["deploy", "--cortex-model", "llama3-70b"])
    assert args.cortex_model == "llama3-70b"


def test_parser_deploy_force():
    p = _build_parser()
    args = p.parse_args(["deploy", "--force"])
    assert args.force is True


def test_parser_deploy_force_default_false():
    p = _build_parser()
    args = p.parse_args(["deploy"])
    assert args.force is False


def test_parser_deploy_target():
    p = _build_parser()
    args = p.parse_args(["deploy", "--target", "PUBLIC.MY_VIEW"])
    assert args.target == "PUBLIC.MY_VIEW"


def test_parser_deploy_target_default_none():
    p = _build_parser()
    args = p.parse_args(["deploy"])
    assert args.target is None


def test_parser_load():
    p = _build_parser()
    args = p.parse_args(["load"])
    assert args.command == "load"


def test_parser_load_dry_run():
    p = _build_parser()
    args = p.parse_args(["load", "--dry-run"])
    assert args.dry_run is True


def test_parser_load_data_folder():
    p = _build_parser()
    args = p.parse_args(["load", "--data-folder", "/tmp/data"])
    assert args.data_folder == "/tmp/data"


def test_parser_load_data_schema():
    p = _build_parser()
    args = p.parse_args(["load", "--data-schema", "RAW"])
    assert args.data_schema == "RAW"


def test_parser_init_default_dir():
    p = _build_parser()
    args = p.parse_args(["init"])
    assert args.command == "init"
    assert args.directory == "."


def test_parser_init_custom_dir():
    p = _build_parser()
    args = p.parse_args(["init", "my_project"])
    assert args.directory == "my_project"


def test_parser_graph():
    p = _build_parser()
    args = p.parse_args(["graph"])
    assert args.command == "graph"


def test_parser_global_verbose():
    p = _build_parser()
    args = p.parse_args(["-v", "plan"])
    assert args.verbose is True


def test_parser_global_config():
    p = _build_parser()
    args = p.parse_args(["-c", "custom.yml", "plan"])
    assert args.config == "custom.yml"


def test_parser_global_objects_folder():
    p = _build_parser()
    args = p.parse_args(["-f", "/tmp/sql", "plan"])
    assert args.objects_folder == "/tmp/sql"


def test_parser_vars():
    p = _build_parser()
    args = p.parse_args(["--vars", '{"env":"prod"}', "plan"])
    assert args.vars == '{"env":"prod"}'


def test_parser_version(capsys):
    p = _build_parser()
    with pytest.raises(SystemExit) as exc_info:
        p.parse_args(["--version"])
    assert exc_info.value.code == 0


def test_parser_no_command(capsys):
    """Omitting the sub-command raises SystemExit(2)."""
    p = _build_parser()
    with pytest.raises(SystemExit) as exc_info:
        p.parse_args([])
    assert exc_info.value.code == 2


# ------------------------------------------------------------------
# main() dispatch smoke tests
# ------------------------------------------------------------------

@patch("frost.cli._cmd_plan")
@patch("frost.cli.load_config")
def test_main_plan_dispatches(mock_load_config, mock_cmd_plan):
    mock_load_config.return_value = MagicMock()
    main(["plan"])
    mock_cmd_plan.assert_called_once()


@patch("frost.cli._cmd_deploy")
@patch("frost.cli.load_config")
def test_main_deploy_dispatches(mock_load_config, mock_cmd_deploy):
    mock_load_config.return_value = MagicMock()
    main(["deploy"])
    mock_cmd_deploy.assert_called_once()


@patch("frost.cli._cmd_load")
@patch("frost.cli.load_config")
def test_main_load_dispatches(mock_load_config, mock_cmd_load):
    mock_load_config.return_value = MagicMock()
    main(["load"])
    mock_cmd_load.assert_called_once()


@patch("frost.cli._cmd_init")
def test_main_init_dispatches(mock_cmd_init):
    """init command should NOT require config."""
    main(["init"])
    mock_cmd_init.assert_called_once()


@patch("frost.cli.load_config")
def test_main_invalid_vars_exits(mock_load_config, capsys):
    """Invalid JSON in --vars should exit with code 1."""
    mock_load_config.return_value = MagicMock()
    with pytest.raises(SystemExit) as exc_info:
        main(["--vars", "not-json", "plan"])
    assert exc_info.value.code == 1
