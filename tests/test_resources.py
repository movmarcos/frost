"""Tests for frost.resources — live Snowflake resource listing."""

from unittest.mock import MagicMock, patch
import pytest

from frost.resources import fetch_resources, RESOURCE_QUERIES


class TestFetchResources:
    """Unit tests for fetch_resources() with mocked connector."""

    def _make_connector(self, show_schemas_result, show_type_results=None):
        """Return a mock SnowflakeConnector.

        show_type_results: dict mapping SQL prefix -> rows, e.g.
          {"SHOW TABLES": [(_, "ORDERS", ..., "SYSADMIN", "my comment")]}
        """
        conn = MagicMock()

        def execute_single_side_effect(sql):
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("SHOW SCHEMAS"):
                return show_schemas_result
            if show_type_results:
                for prefix, rows in show_type_results.items():
                    if sql_upper.startswith(prefix.upper()):
                        return rows
            return []

        conn.execute_single.side_effect = execute_single_side_effect
        return conn

    def test_empty_database(self):
        """No schemas → empty resources, no warnings."""
        conn = self._make_connector(show_schemas_result=[])
        result = fetch_resources(conn, "MY_DB")
        assert result["database"] == "MY_DB"
        assert result["resources"] == []
        assert result["warnings"] == []

    def test_skips_information_schema(self):
        """INFORMATION_SCHEMA must be excluded from queries."""
        conn = self._make_connector(
            show_schemas_result=[
                ("2025-01-01", "PUBLIC", "N", "", "", "", "", "", ""),
                ("2025-01-01", "INFORMATION_SCHEMA", "N", "", "", "", "", "", ""),
            ],
        )
        result = fetch_resources(conn, "MY_DB")
        # Only PUBLIC queried — verify no INFORMATION_SCHEMA call
        for call in conn.execute_single.call_args_list:
            sql = call[0][0]
            assert "INFORMATION_SCHEMA" not in sql or "SHOW SCHEMAS" in sql

    def test_single_schema_with_tables(self):
        """Tables in PUBLIC should appear in resources list."""
        conn = self._make_connector(
            show_schemas_result=[
                ("2025-01-01", "PUBLIC", "N", "", "", "", "", "", ""),
            ],
            show_type_results={
                "SHOW TABLES IN SCHEMA": [
                    ("2025-01-01", "ORDERS", "MY_DB", "PUBLIC", "SYSADMIN",
                     0, 0, 0, "", "", "", "", "", "N", "N", "", "", "order table"),
                ],
            },
        )
        result = fetch_resources(conn, "MY_DB")
        assert len(result["resources"]) == 1
        r = result["resources"][0]
        assert r["schema"] == "PUBLIC"
        assert r["type"] == "TABLE"
        assert r["name"] == "ORDERS"
        assert r["fqn"] == "PUBLIC.ORDERS"
        assert r["owner"] == "SYSADMIN"

    def test_query_failure_becomes_warning(self):
        """If SHOW ALERTS raises, it becomes a warning; other types still work."""
        conn = MagicMock()

        def execute_side_effect(sql):
            sql_upper = sql.strip().upper()
            if sql_upper.startswith("SHOW SCHEMAS"):
                return [("2025-01-01", "PUBLIC", "N", "", "", "", "", "", "")]
            if "ALERT" in sql_upper:
                raise Exception("Insufficient privileges")
            return []

        conn.execute_single.side_effect = execute_side_effect
        result = fetch_resources(conn, "MY_DB")
        assert result["warnings"] != []
        assert any("ALERT" in w for w in result["warnings"])
        # No crash — result is valid
        assert result["database"] == "MY_DB"

    def test_resource_queries_covers_all_types(self):
        """All 15 resource types must be in the query map."""
        expected_types = {
            "TABLE", "VIEW", "PROCEDURE", "FUNCTION", "DYNAMIC TABLE",
            "FILE FORMAT", "STAGE", "TASK", "TAG", "STREAM", "PIPE",
            "MATERIALIZED VIEW", "ALERT", "EVENT TABLE", "SEQUENCE",
        }
        assert set(RESOURCE_QUERIES.keys()) == expected_types

    def test_multiple_schemas(self):
        """Resources from different schemas are all included."""
        conn = self._make_connector(
            show_schemas_result=[
                ("2025-01-01", "PUBLIC", "N", "", "", "", "", "", ""),
                ("2025-01-01", "ANALYTICS", "N", "", "", "", "", "", ""),
            ],
            show_type_results={
                "SHOW TABLES IN SCHEMA MY_DB.PUBLIC": [
                    ("2025-01-01", "ORDERS", "MY_DB", "PUBLIC", "SYSADMIN",
                     0, 0, 0, "", "", "", "", "", "N", "N", "", "", ""),
                ],
                "SHOW TABLES IN SCHEMA MY_DB.ANALYTICS": [
                    ("2025-01-01", "METRICS", "MY_DB", "ANALYTICS", "ANALYST",
                     0, 0, 0, "", "", "", "", "", "N", "N", "", "", ""),
                ],
            },
        )
        result = fetch_resources(conn, "MY_DB")
        schemas = {r["schema"] for r in result["resources"]}
        assert "PUBLIC" in schemas
        assert "ANALYTICS" in schemas


class TestCmdResources:
    """CLI integration test for `frost resources --json`."""

    def test_resources_json_requires_connection(self):
        """Running without valid creds should produce a JSON error."""
        import subprocess, json, sys

        result = subprocess.run(
            [
                sys.executable, "-m", "frost",
                "-c", "nonexistent-config.yml",
                "resources", "--json",
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 1
        # stdout should be parseable JSON with an "error" key
        out = result.stdout.strip()
        # Find the JSON in the output (may have log lines on stderr)
        if out:
            data = json.loads(out)
            assert "error" in data
