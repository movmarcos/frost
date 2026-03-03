"""Tests for frost.streamlit – Streamlit app discovery and deployment.

We test:
  * discover_apps() with V1 snowflake.yml format
  * discover_apps() with V2 snowflake.yml format (entities)
  * discover_apps() with no apps
  * _parse_snowflake_yml() edge cases
  * find_snow_cli() behaviour
  * deploy_app() with mocked subprocess
  * teardown_app() with mocked subprocess
  * get_app_url() with mocked subprocess
  * _extract_url() helper
  * CLI argparse integration (frost streamlit list/deploy/teardown/get-url)
"""

import json
import os
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from frost.streamlit import (
    StreamlitApp,
    StreamlitDeployResult,
    discover_apps,
    _parse_snowflake_yml,
    _find_snowflake_ymls,
    find_snow_cli,
    deploy_app,
    teardown_app,
    get_app_url,
    _extract_url,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture
def v1_project(tmp_path):
    """Create a tmp project with a V1 snowflake.yml."""
    yml = tmp_path / "snowflake.yml"
    yml.write_text(textwrap.dedent("""\
        streamlit:
          name: my_dashboard
          main_file: app.py
          stage: "@my_stage"
          schema: ANALYTICS
          warehouse: COMPUTE_WH
          query_warehouse: QUERY_WH
          title: My Dashboard
          comment: A demo dashboard
    """))
    (tmp_path / "app.py").write_text("import streamlit as st\nst.title('Hello')\n")
    return tmp_path


@pytest.fixture
def v2_project(tmp_path):
    """Create a tmp project with a V2 snowflake.yml (entities)."""
    yml = tmp_path / "snowflake.yml"
    yml.write_text(textwrap.dedent("""\
        definition_version: 2
        entities:
          sales_report:
            type: streamlit
            main_file: report.py
            stage: "@sales_stage"
            schema: SALES
            warehouse: REPORT_WH
            title: Sales Report
          data_explorer:
            type: streamlit
            main_file: explorer.py
            schema: PUBLIC
    """))
    (tmp_path / "report.py").write_text("import streamlit as st\n")
    (tmp_path / "explorer.py").write_text("import streamlit as st\n")
    return tmp_path


@pytest.fixture
def empty_project(tmp_path):
    """Project with no snowflake.yml."""
    (tmp_path / "frost-config.yml").write_text("objects_folder: objects/\n")
    return tmp_path


@pytest.fixture
def multi_project(tmp_path):
    """Project with multiple snowflake.yml in subdirectories."""
    app1 = tmp_path / "apps" / "dashboard"
    app1.mkdir(parents=True)
    (app1 / "snowflake.yml").write_text(textwrap.dedent("""\
        streamlit:
          name: dashboard
          main_file: app.py
    """))

    app2 = tmp_path / "apps" / "monitor"
    app2.mkdir(parents=True)
    (app2 / "snowflake.yml").write_text(textwrap.dedent("""\
        definition_version: 2
        entities:
          monitor:
            type: streamlit
            main_file: monitor.py
    """))
    return tmp_path


@pytest.fixture
def sample_app():
    """A pre-built StreamlitApp for deploy/teardown tests."""
    return StreamlitApp(
        name="test_app",
        directory="/tmp/test_app",
        main_file="app.py",
        stage="@test_stage",
        schema="PUBLIC",
        warehouse="WH",
        query_warehouse="QWH",
        title="Test App",
        definition_file="/tmp/test_app/snowflake.yml",
    )


# ------------------------------------------------------------------
# Discovery: V1 format
# ------------------------------------------------------------------

class TestDiscoverV1:
    def test_discovers_v1_app(self, v1_project):
        apps = discover_apps(str(v1_project))
        assert len(apps) == 1
        app = apps[0]
        assert app.name == "my_dashboard"
        assert app.main_file == "app.py"
        assert app.stage == "@my_stage"
        assert app.schema == "ANALYTICS"
        assert app.warehouse == "COMPUTE_WH"
        assert app.query_warehouse == "QUERY_WH"
        assert app.title == "My Dashboard"
        assert app.comment == "A demo dashboard"
        assert app.definition_file.endswith("snowflake.yml")

    def test_v1_defaults(self, tmp_path):
        """V1 with minimal fields uses defaults."""
        yml = tmp_path / "snowflake.yml"
        yml.write_text("streamlit:\n  name: minimal\n")
        apps = discover_apps(str(tmp_path))
        assert len(apps) == 1
        assert apps[0].main_file == "streamlit_app.py"
        assert apps[0].schema == "PUBLIC"

    def test_v1_list(self, tmp_path):
        """V1 format with a list of streamlit apps."""
        yml = tmp_path / "snowflake.yml"
        yml.write_text(textwrap.dedent("""\
            streamlit:
              - name: app_a
                main_file: a.py
              - name: app_b
                main_file: b.py
        """))
        apps = discover_apps(str(tmp_path))
        assert len(apps) == 2
        names = {a.name for a in apps}
        assert names == {"app_a", "app_b"}


# ------------------------------------------------------------------
# Discovery: V2 format
# ------------------------------------------------------------------

class TestDiscoverV2:
    def test_discovers_v2_apps(self, v2_project):
        apps = discover_apps(str(v2_project))
        assert len(apps) == 2
        names = {a.name for a in apps}
        assert names == {"data_explorer", "sales_report"}

    def test_v2_fields(self, v2_project):
        apps = discover_apps(str(v2_project))
        sales = next(a for a in apps if a.name == "sales_report")
        assert sales.main_file == "report.py"
        assert sales.stage == "@sales_stage"
        assert sales.schema == "SALES"
        assert sales.warehouse == "REPORT_WH"
        assert sales.title == "Sales Report"

    def test_v2_ignores_non_streamlit(self, tmp_path):
        """V2 entities that are not type: streamlit are ignored."""
        yml = tmp_path / "snowflake.yml"
        yml.write_text(textwrap.dedent("""\
            definition_version: 2
            entities:
              my_func:
                type: function
                handler: handler.main
              my_app:
                type: streamlit
                main_file: app.py
        """))
        apps = discover_apps(str(tmp_path))
        assert len(apps) == 1
        assert apps[0].name == "my_app"


# ------------------------------------------------------------------
# Discovery: edge cases
# ------------------------------------------------------------------

class TestDiscoverEdgeCases:
    def test_empty_project(self, empty_project):
        apps = discover_apps(str(empty_project))
        assert apps == []

    def test_multi_project(self, multi_project):
        apps = discover_apps(str(multi_project))
        assert len(apps) == 2
        names = {a.name for a in apps}
        assert names == {"dashboard", "monitor"}

    def test_max_depth_respected(self, tmp_path):
        """Deeply nested snowflake.yml beyond max_depth is not found."""
        deep = tmp_path / "a" / "b" / "c" / "d" / "e" / "f"
        deep.mkdir(parents=True)
        (deep / "snowflake.yml").write_text("streamlit:\n  name: hidden\n")
        # max_depth=2 should not find it
        apps = discover_apps(str(tmp_path), max_depth=2)
        assert len(apps) == 0

    def test_skips_hidden_dirs(self, tmp_path):
        hidden = tmp_path / ".hidden"
        hidden.mkdir()
        (hidden / "snowflake.yml").write_text("streamlit:\n  name: secret\n")
        apps = discover_apps(str(tmp_path))
        assert len(apps) == 0

    def test_skips_venv(self, tmp_path):
        venv = tmp_path / ".venv"
        venv.mkdir()
        (venv / "snowflake.yml").write_text("streamlit:\n  name: venv_app\n")
        apps = discover_apps(str(tmp_path))
        assert len(apps) == 0

    def test_malformed_yaml(self, tmp_path):
        """Malformed YAML is skipped with a warning."""
        yml = tmp_path / "snowflake.yml"
        yml.write_text(": : invalid yaml {{{\n")
        apps = discover_apps(str(tmp_path))
        assert apps == []

    def test_yaml_without_streamlit_key(self, tmp_path):
        """snowflake.yml without streamlit or entities key returns empty."""
        yml = tmp_path / "snowflake.yml"
        yml.write_text("native_app:\n  name: not_streamlit\n")
        apps = discover_apps(str(tmp_path))
        assert apps == []

    def test_sorted_output(self, multi_project):
        """Apps are returned sorted by name."""
        apps = discover_apps(str(multi_project))
        names = [a.name for a in apps]
        assert names == sorted(names)


# ------------------------------------------------------------------
# to_dict
# ------------------------------------------------------------------

class TestStreamlitAppToDict:
    def test_to_dict(self, sample_app):
        d = sample_app.to_dict()
        assert d["name"] == "test_app"
        assert d["main_file"] == "app.py"
        assert d["schema"] == "PUBLIC"
        assert d["definition_file"] == "/tmp/test_app/snowflake.yml"
        assert isinstance(d["external_access_integrations"], list)
        assert isinstance(d["imports"], list)


# ------------------------------------------------------------------
# find_snow_cli
# ------------------------------------------------------------------

class TestFindSnowCli:
    @patch("shutil.which", return_value="/usr/local/bin/snow")
    def test_found(self, mock_which):
        assert find_snow_cli() == "/usr/local/bin/snow"

    @patch("shutil.which", return_value=None)
    def test_not_found(self, mock_which):
        assert find_snow_cli() is None


# ------------------------------------------------------------------
# deploy_app
# ------------------------------------------------------------------

class TestDeployApp:
    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_success(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="Streamlit app deployed: https://app.snowflake.com/my_app\n",
            stderr="",
        )
        result = deploy_app(sample_app)
        assert result.success is True
        assert result.name == "test_app"
        assert "https://app.snowflake.com/my_app" in result.url

    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_failure(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Error: permission denied",
        )
        result = deploy_app(sample_app)
        assert result.success is False
        assert "permission denied" in result.message

    def test_no_snow_cli(self, sample_app):
        with patch("frost.streamlit.find_snow_cli", return_value=None):
            result = deploy_app(sample_app)
            assert result.success is False
            assert "not found" in result.message.lower()

    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_replace_flag(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        deploy_app(sample_app, replace=True)
        cmd = mock_run.call_args[0][0]
        assert "--replace" in cmd

    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_connection_option(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(returncode=0, stdout="OK", stderr="")
        deploy_app(sample_app, connection="myconn")
        cmd = mock_run.call_args[0][0]
        assert "--connection" in cmd
        assert "myconn" in cmd

    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_timeout(self, mock_cli, mock_run, sample_app):
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="snow", timeout=120)
        result = deploy_app(sample_app)
        assert result.success is False
        assert "timed out" in result.message.lower()


# ------------------------------------------------------------------
# teardown_app
# ------------------------------------------------------------------

class TestTeardownApp:
    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_success(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="Dropped.", stderr=""
        )
        result = teardown_app(sample_app)
        assert result.success is True
        assert result.name == "test_app"

    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_failure(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Not found"
        )
        result = teardown_app(sample_app)
        assert result.success is False

    def test_no_snow_cli(self, sample_app):
        with patch("frost.streamlit.find_snow_cli", return_value=None):
            result = teardown_app(sample_app)
            assert result.success is False


# ------------------------------------------------------------------
# get_app_url
# ------------------------------------------------------------------

class TestGetAppUrl:
    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_success(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="https://app.snowflake.com/my_app\n",
            stderr="",
        )
        url = get_app_url(sample_app)
        assert url == "https://app.snowflake.com/my_app"

    @patch("frost.streamlit.subprocess.run")
    @patch("frost.streamlit.find_snow_cli", return_value="/usr/local/bin/snow")
    def test_failure(self, mock_cli, mock_run, sample_app):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
        url = get_app_url(sample_app)
        assert url is None

    def test_no_snow_cli(self, sample_app):
        with patch("frost.streamlit.find_snow_cli", return_value=None):
            url = get_app_url(sample_app)
            assert url is None


# ------------------------------------------------------------------
# _extract_url helper
# ------------------------------------------------------------------

class TestExtractUrl:
    def test_extracts_url(self):
        output = "Deploy done. URL: https://app.snowflake.com/org/account/#/streamlit/TEST.APP"
        assert _extract_url(output).startswith("https://")

    def test_no_url(self):
        assert _extract_url("No url here") == ""

    def test_multiline(self):
        output = "Line 1\nURL: https://example.com/app\nLine 3"
        assert _extract_url(output) == "https://example.com/app"


# ------------------------------------------------------------------
# CLI argparse integration
# ------------------------------------------------------------------

class TestCliArgparse:
    def test_streamlit_list(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "list"])
        assert args.command == "streamlit"
        assert args.action == "list"

    def test_streamlit_deploy(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "deploy"])
        assert args.command == "streamlit"
        assert args.action == "deploy"
        assert args.name is None

    def test_streamlit_deploy_specific(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "deploy", "my_app"])
        assert args.action == "deploy"
        assert args.name == "my_app"

    def test_streamlit_teardown(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "teardown", "my_app"])
        assert args.action == "teardown"
        assert args.name == "my_app"

    def test_streamlit_get_url(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "get-url", "my_app"])
        assert args.action == "get-url"
        assert args.name == "my_app"

    def test_streamlit_json_flag(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "list", "--json"])
        assert args.json is True

    def test_streamlit_connection_flag(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "deploy", "--connection", "myconn"])
        assert args.connection == "myconn"

    def test_streamlit_open_flag(self):
        from frost.cli import _build_parser
        p = _build_parser()
        args = p.parse_args(["streamlit", "deploy", "--open"])
        assert getattr(args, "open") is True
