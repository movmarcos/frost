"""Tests for frost.deployer -- the deployment orchestrator.

All Snowflake interactions are mocked; we focus on:
  * _scan_and_parse discovers SQL files
  * plan() returns a graph visualisation
  * deploy() orchestrates parse → graph → diff → execute
  * dry_run mode skips real SQL execution
  * Cycle errors handled gracefully
  * PolicyError raised when violations exist
  * Cascade logic re-deploys dependents of changed objects
"""

import os
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from frost.config import FrostConfig
from frost.deployer import Deployer, DeployResult
from frost.reporter import PolicyError


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _write_sql(base: Path, relative: str, sql: str) -> None:
    """Write a SQL file under *base* at the given relative path."""
    p = base / relative
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(sql))


def _make_config(objects_folder: str, **kwargs) -> FrostConfig:
    """Return a FrostConfig pointing at *objects_folder*."""
    defaults = dict(
        account="test_acct",
        user="test_user",
        role="SYSADMIN",
        warehouse="COMPUTE_WH",
        database="DEV",
        objects_folder=objects_folder,
        dry_run=False,
        cortex=False,
    )
    defaults.update(kwargs)
    return FrostConfig(**defaults)


# ------------------------------------------------------------------
# DeployResult model
# ------------------------------------------------------------------

def test_deploy_result_defaults():
    r = DeployResult()
    assert r.total_objects == 0
    assert r.deployed == 0
    assert r.skipped == 0
    assert r.failed == 0
    assert r.success is True
    assert r.execution_order == []


def test_deploy_result_success_when_no_failures():
    r = DeployResult(deployed=3, skipped=1, failed=0)
    assert r.success is True


def test_deploy_result_not_success_when_failures():
    r = DeployResult(deployed=2, failed=1)
    assert r.success is False


# ------------------------------------------------------------------
# _scan_and_parse & plan()
# ------------------------------------------------------------------

def test_plan_empty_folder(tmp_path):
    """plan() on an empty folder returns empty or header-only graph."""
    folder = tmp_path / "objects"
    folder.mkdir()
    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    plan = deployer.plan()
    # Plan should be a string (possibly empty or with a header)
    assert isinstance(plan, str)


def test_plan_single_object(tmp_path):
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (
            ID INT
        );
    """)
    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    plan = deployer.plan()
    assert "T1" in plan.upper()


def test_plan_with_dependencies(tmp_path):
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)
    _write_sql(folder, "views/v1.sql", """\
        CREATE OR ALTER VIEW DEV.PUBLIC.V1 AS
        SELECT * FROM DEV.PUBLIC.T1;
    """)
    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    plan = deployer.plan()
    assert "T1" in plan.upper()
    assert "V1" in plan.upper()


def test_plan_raises_policy_error_on_violations(tmp_path):
    """SQL with CREATE OR REPLACE TABLE should trigger PolicyError."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/bad.sql", """\
        CREATE OR REPLACE TABLE DEV.PUBLIC.BAD (ID INT);
    """)
    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    with pytest.raises(PolicyError):
        deployer.plan()


# ------------------------------------------------------------------
# deploy() -- dry_run
# ------------------------------------------------------------------

def test_deploy_dry_run(tmp_path):
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)
    cfg = _make_config(str(folder), dry_run=True)
    deployer = Deployer(cfg)
    result = deployer.deploy()
    assert result.deployed >= 1
    assert result.failed == 0


def test_deploy_empty_folder(tmp_path):
    folder = tmp_path / "objects"
    folder.mkdir()
    cfg = _make_config(str(folder), dry_run=True)
    deployer = Deployer(cfg)
    result = deployer.deploy()
    assert result.total_objects == 0


def test_deploy_missing_folder(tmp_path):
    cfg = _make_config(str(tmp_path / "nonexistent"), dry_run=True)
    deployer = Deployer(cfg)
    result = deployer.deploy()
    assert result.total_objects == 0


# ------------------------------------------------------------------
# deploy() -- with mocked Snowflake
# ------------------------------------------------------------------

@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_executes_changed_objects(MockTracker, MockConnector, tmp_path):
    """Changed objects should be deployed in order."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)

    # Set up mock connector
    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    # Set up mock tracker — everything changed
    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    mock_tracker.get_changed_fqns.return_value = {"DEV.PUBLIC.T1"}

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    assert result.deployed == 1
    assert result.failed == 0
    mock_conn.execute.assert_called_once()


@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_skips_unchanged(MockTracker, MockConnector, tmp_path):
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    # Object exists in database -- should stay skipped
    mock_conn.get_existing_objects_in_schema.return_value = {"T1"}

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    mock_tracker.get_changed_fqns.return_value = set()  # nothing changed

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    assert result.deployed == 0
    assert result.skipped >= 1
    mock_conn.execute.assert_not_called()


@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_records_failure(MockTracker, MockConnector, tmp_path):
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.side_effect = Exception("SQL compilation error")

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    mock_tracker.get_changed_fqns.return_value = {"DEV.PUBLIC.T1"}

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    assert result.failed == 1
    assert result.success is False
    assert len(result.deploy_errors) == 1
    assert "SQL compilation error" in result.errors[0]


# ------------------------------------------------------------------
# deploy() -- cycle detection
# ------------------------------------------------------------------

def test_deploy_cycle_error(tmp_path):
    """When objects form a cycle, deploy should report error, not crash."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "a.sql", """\
        -- @depends_on: DEV.PUBLIC.B
        CREATE OR ALTER TABLE DEV.PUBLIC.A (ID INT);
    """)
    _write_sql(folder, "b.sql", """\
        -- @depends_on: DEV.PUBLIC.A
        CREATE OR ALTER TABLE DEV.PUBLIC.B (ID INT);
    """)

    cfg = _make_config(str(folder), dry_run=True)
    deployer = Deployer(cfg)
    result = deployer.deploy()
    assert result.failed > 0 or len(result.errors) > 0


# ------------------------------------------------------------------
# deploy() -- --force mode
# ------------------------------------------------------------------

@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_force_redeploys_all(MockTracker, MockConnector, tmp_path):
    """--force should deploy ALL objects even when nothing changed."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)
    _write_sql(folder, "tables/t2.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T2 (NAME VARCHAR);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    # Nothing changed per checksum
    mock_tracker.get_changed_fqns.return_value = set()

    cfg = _make_config(str(folder), force=True)
    deployer = Deployer(cfg)
    result = deployer.deploy()

    # Both objects should be deployed despite no changes
    assert result.deployed == 2
    assert result.skipped == 0
    # get_changed_fqns should NOT have been called in force mode
    mock_tracker.get_changed_fqns.assert_not_called()


# ------------------------------------------------------------------
# deploy() -- --target mode
# ------------------------------------------------------------------

@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_target_specific_object(MockTracker, MockConnector, tmp_path):
    """--target should deploy only the named object and its dependents."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)
    _write_sql(folder, "views/v1.sql", """\
        CREATE OR ALTER VIEW DEV.PUBLIC.V1 AS
        SELECT * FROM DEV.PUBLIC.T1;
    """)
    _write_sql(folder, "tables/t2.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T2 (NAME VARCHAR);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    # Nothing changed per checksum
    mock_tracker.get_changed_fqns.return_value = set()

    # Target T1 -- should also bring V1 (dependent) but not T2
    cfg = _make_config(str(folder), target="DEV.PUBLIC.T1")
    deployer = Deployer(cfg)
    result = deployer.deploy()

    # T1 + V1 deployed, T2 skipped
    assert result.deployed == 2
    assert result.skipped == 1
    mock_tracker.get_changed_fqns.assert_not_called()


@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_target_not_found(MockTracker, MockConnector, tmp_path):
    """--target with a nonexistent FQN should fail gracefully."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}

    cfg = _make_config(str(folder), target="DEV.PUBLIC.DOES_NOT_EXIST")
    deployer = Deployer(cfg)
    result = deployer.deploy()

    assert result.failed == 1
    assert "not found" in result.errors[0].lower()


# ------------------------------------------------------------------
# deploy() -- auto-detect missing objects
# ------------------------------------------------------------------

@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_missing_object_auto_detected(MockTracker, MockConnector, tmp_path):
    """Unchanged object that doesn't exist in DB should be auto-deployed."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    # SHOW TABLES returns empty → T1 is missing from the database
    mock_conn.get_existing_objects_in_schema.return_value = set()

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    # Checksum matches → no change detected by tracker
    mock_tracker.get_changed_fqns.return_value = set()

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    # Object should be deployed because it's missing from the database
    assert result.deployed == 1
    assert result.skipped == 0


@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_existing_unchanged_still_skipped(MockTracker, MockConnector, tmp_path):
    """Unchanged object that exists in DB should remain skipped."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    # SHOW TABLES returns T1 → it exists, should stay skipped
    mock_conn.get_existing_objects_in_schema.return_value = {"T1"}

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    mock_tracker.get_changed_fqns.return_value = set()

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    assert result.deployed == 0
    assert result.skipped >= 1
    mock_conn.execute.assert_not_called()


@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_missing_cascades_dependents(MockTracker, MockConnector, tmp_path):
    """Missing object should also redeploy its dependents."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)
    _write_sql(folder, "views/v1.sql", """\
        CREATE OR ALTER VIEW DEV.PUBLIC.V1 AS
        SELECT * FROM DEV.PUBLIC.T1;
    """)
    _write_sql(folder, "tables/t2.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T2 (NAME VARCHAR);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    # T1 is missing, T2 and V1 exist
    mock_conn.get_existing_objects_in_schema.side_effect = (
        lambda schema, obj_type: {"T2"} if obj_type == "TABLE" else set()
    )

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    mock_tracker.get_changed_fqns.return_value = set()

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    # T1 (missing) + V1 (dependent of T1) deployed, T2 skipped
    assert result.deployed == 2
    assert result.skipped == 1


@patch("frost.deployer.SnowflakeConnector")
@patch("frost.deployer.ChangeTracker")
def test_deploy_missing_mixed_with_changed(MockTracker, MockConnector, tmp_path):
    """Changed + missing objects should both be deployed."""
    folder = tmp_path / "objects"
    folder.mkdir()
    _write_sql(folder, "tables/t1.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T1 (ID INT);
    """)
    _write_sql(folder, "tables/t2.sql", """\
        CREATE OR ALTER TABLE DEV.PUBLIC.T2 (NAME VARCHAR);
    """)

    mock_conn = MagicMock()
    MockConnector.return_value = mock_conn
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    # T2 is missing from the database
    mock_conn.get_existing_objects_in_schema.return_value = set()

    mock_tracker = MagicMock()
    MockTracker.return_value = mock_tracker
    mock_tracker.load_checksums.return_value = {}
    # T1 changed, T2 did not
    mock_tracker.get_changed_fqns.return_value = {"DEV.PUBLIC.T1"}

    cfg = _make_config(str(folder))
    deployer = Deployer(cfg)
    result = deployer.deploy()

    # T1 (changed) + T2 (missing) both deployed
    assert result.deployed == 2
    assert result.skipped == 0
