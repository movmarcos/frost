"""Tests for frost lifecycle tracking -- OBJECT_LIFECYCLE table operations.

Tests the ChangeTracker lifecycle methods:
  * ensure_lifecycle_table creates the table DDL
  * upsert_lifecycle inserts new objects or updates existing ones
  * retire_object marks objects as RETIRED with a reason
  * get_active_objects returns only ACTIVE FQNs
"""

from unittest.mock import MagicMock, call

import pytest

from frost.tracker import ChangeTracker


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_tracker(**kwargs) -> tuple:
    """Return (tracker, mock_connector)."""
    conn = MagicMock()
    tracker = ChangeTracker(conn, **kwargs)
    return tracker, conn


# ------------------------------------------------------------------
# ensure_lifecycle_table
# ------------------------------------------------------------------

class TestEnsureLifecycleTable:

    def test_creates_table(self):
        tracker, conn = _make_tracker()
        tracker.ensure_lifecycle_table()
        sql = conn.execute.call_args_list[-1][0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "OBJECT_LIFECYCLE" in sql

    def test_table_has_required_columns(self):
        tracker, conn = _make_tracker()
        tracker.ensure_lifecycle_table()
        sql = conn.execute.call_args_list[-1][0][0]
        for col in ("object_fqn", "object_type", "file_path",
                     "first_deployed_at", "last_deployed_at",
                     "retired_at", "retired_reason", "status"):
            assert col in sql

    def test_custom_schema(self):
        tracker, conn = _make_tracker(tracking_schema="CUSTOM")
        tracker.ensure_lifecycle_table()
        sql = conn.execute.call_args_list[-1][0][0]
        assert "CUSTOM.OBJECT_LIFECYCLE" in sql


# ------------------------------------------------------------------
# upsert_lifecycle
# ------------------------------------------------------------------

class TestUpsertLifecycle:

    def test_calls_merge(self):
        tracker, conn = _make_tracker()
        tracker.upsert_lifecycle("PUBLIC.T1", "TABLE", "tables/t1.sql")
        conn.execute_params.assert_called_once()
        sql = conn.execute_params.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "OBJECT_LIFECYCLE" in sql

    def test_merge_params(self):
        tracker, conn = _make_tracker()
        tracker.upsert_lifecycle("PUBLIC.PROC_A", "PROCEDURE", "procs/a.sql")
        params = conn.execute_params.call_args[0][1]
        assert params == ("PUBLIC.PROC_A", "PROCEDURE", "procs/a.sql")

    def test_merge_sets_active(self):
        tracker, conn = _make_tracker()
        tracker.upsert_lifecycle("PUBLIC.V1", "VIEW", "views/v1.sql")
        sql = conn.execute_params.call_args[0][0]
        assert "'ACTIVE'" in sql

    def test_merge_clears_retired_on_reactivation(self):
        """WHEN MATCHED should reset retired_at and retired_reason."""
        tracker, conn = _make_tracker()
        tracker.upsert_lifecycle("PUBLIC.T1", "TABLE", "t1.sql")
        sql = conn.execute_params.call_args[0][0]
        assert "retired_at       = NULL" in sql
        assert "retired_reason   = NULL" in sql


# ------------------------------------------------------------------
# retire_object
# ------------------------------------------------------------------

class TestRetireObject:

    def test_calls_update(self):
        tracker, conn = _make_tracker()
        tracker.retire_object("PUBLIC.OLD_T", reason="DROPPED")
        conn.execute_params.assert_called_once()
        sql = conn.execute_params.call_args[0][0]
        assert "UPDATE" in sql
        assert "RETIRED" in sql

    def test_retire_reason_passed(self):
        tracker, conn = _make_tracker()
        tracker.retire_object("PUBLIC.OLD_T", reason="REMOVED")
        params = conn.execute_params.call_args[0][1]
        assert "REMOVED" in params

    def test_retire_reason_dropped(self):
        tracker, conn = _make_tracker()
        tracker.retire_object("PUBLIC.OLD_T", reason="DROPPED")
        params = conn.execute_params.call_args[0][1]
        assert "DROPPED" in params

    def test_only_updates_active(self):
        """Retire should only affect rows with status = ACTIVE."""
        tracker, conn = _make_tracker()
        tracker.retire_object("PUBLIC.OLD_T")
        sql = conn.execute_params.call_args[0][0]
        assert "status = 'ACTIVE'" in sql

    def test_default_reason_is_removed(self):
        tracker, conn = _make_tracker()
        tracker.retire_object("PUBLIC.OLD_T")
        params = conn.execute_params.call_args[0][1]
        assert params[0] == "REMOVED"


# ------------------------------------------------------------------
# get_active_objects
# ------------------------------------------------------------------

class TestGetActiveObjects:

    def test_returns_fqn_set(self):
        tracker, conn = _make_tracker()
        conn.execute.return_value = [
            ("PUBLIC.T1",),
            ("PUBLIC.V1",),
        ]
        result = tracker.get_active_objects()
        assert result == {"PUBLIC.T1", "PUBLIC.V1"}

    def test_empty_table(self):
        tracker, conn = _make_tracker()
        conn.execute.return_value = []
        result = tracker.get_active_objects()
        assert result == set()

    def test_queries_active_only(self):
        tracker, conn = _make_tracker()
        conn.execute.return_value = []
        tracker.get_active_objects()
        sql = conn.execute.call_args[0][0]
        assert "ACTIVE" in sql
