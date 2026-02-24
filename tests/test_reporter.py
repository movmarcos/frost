"""Tests for frost.reporter -- violation reporting, deploy error reporting, summaries."""

import os

from frost.reporter import (
    Violation,
    PolicyError,
    DeployError,
    report_violations,
    report_deploy_errors,
    report_deploy_summary,
    report_load_summary,
    _parse_snowflake_error,
)


# ------------------------------------------------------------------
# Violation data model
# ------------------------------------------------------------------

def test_violation_dataclass():
    v = Violation(
        file_path="tables/t1.sql",
        object_type="TABLE",
        fqn="PUBLIC.T1",
        found_form="CREATE OR REPLACE",
        suggested_form="CREATE OR ALTER",
        source_line="CREATE OR REPLACE TABLE PUBLIC.T1 (id INT);",
        line_number=1,
    )
    assert v.fqn == "PUBLIC.T1"
    assert v.found_form == "CREATE OR REPLACE"


def test_policy_error_message():
    v = [Violation("f", "TABLE", "T", "CREATE", "CREATE OR ALTER", "line", 1)]
    err = PolicyError(v)
    assert "1 policy violation" in str(err)


def test_policy_error_plural():
    vs = [
        Violation("f", "TABLE", "T1", "CREATE", "CREATE OR ALTER", "line", 1),
        Violation("f", "TABLE", "T2", "CREATE", "CREATE OR ALTER", "line", 2),
    ]
    err = PolicyError(vs)
    assert "2 policy violations" in str(err)


# ------------------------------------------------------------------
# report_violations
# ------------------------------------------------------------------

def test_report_violations_contains_object(monkeypatch):
    """Report should contain the object FQN and suggested fix."""
    monkeypatch.setenv("NO_COLOR", "1")

    v = Violation(
        file_path="tables/t1.sql",
        object_type="TABLE",
        fqn="PUBLIC.T1",
        found_form="CREATE OR REPLACE",
        suggested_form="CREATE OR ALTER",
        source_line="CREATE OR REPLACE TABLE PUBLIC.T1 (id INT);",
        line_number=1,
    )
    report = report_violations([v])

    assert "PUBLIC.T1" in report
    assert "CREATE OR ALTER" in report
    assert "violation" in report.lower()


def test_report_violations_multiple(monkeypatch):
    """Report with multiple violations should mention count."""
    monkeypatch.setenv("NO_COLOR", "1")

    vs = [
        Violation("f1", "TABLE", "PUBLIC.T1", "CREATE OR REPLACE", "CREATE OR ALTER",
                  "CREATE OR REPLACE TABLE PUBLIC.T1 (id INT);", 1),
        Violation("f2", "VIEW", "PUBLIC.V1", "CREATE OR REPLACE", "CREATE OR ALTER",
                  "CREATE OR REPLACE VIEW PUBLIC.V1 AS SELECT 1;", 1),
    ]
    report = report_violations(vs)
    assert "2 violation" in report.lower()


# ------------------------------------------------------------------
# DeployError data model
# ------------------------------------------------------------------

def test_deploy_error_defaults():
    err = DeployError(
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="SELECT 1",
        error_message="some error",
    )
    assert err.blocked == []
    assert err.ai_suggestion is None
    assert err.error_code is None


# ------------------------------------------------------------------
# report_deploy_errors
# ------------------------------------------------------------------

def test_report_deploy_errors_content(monkeypatch):
    """Deploy error report should contain the object FQN and error."""
    monkeypatch.setenv("NO_COLOR", "1")

    err = DeployError(
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="CREATE TABLE PUBLIC.T1 (id INT);",
        error_message="001003: SQL compilation error",
        error_code="001003",
    )
    report = report_deploy_errors([err])
    assert "PUBLIC.T1" in report
    assert "deployment failure" in report.lower()


def test_report_deploy_errors_with_ai(monkeypatch):
    """AI suggestion should appear in the report."""
    monkeypatch.setenv("NO_COLOR", "1")

    err = DeployError(
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="SELECT 1",
        error_message="error",
        ai_suggestion="Try adding the missing column.",
    )
    report = report_deploy_errors([err])
    assert "Try adding the missing column" in report


def test_report_deploy_errors_with_blocked(monkeypatch):
    """Blocked dependents should appear in the report."""
    monkeypatch.setenv("NO_COLOR", "1")

    err = DeployError(
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="SELECT 1",
        error_message="error",
        blocked=["PUBLIC.V1", "PUBLIC.V2"],
    )
    report = report_deploy_errors([err])
    assert "PUBLIC.V1" in report
    assert "Blocked" in report


# ------------------------------------------------------------------
# _parse_snowflake_error
# ------------------------------------------------------------------

def test_parse_error_with_code():
    code, msg = _parse_snowflake_error("001003 (42000): SQL compilation error:\nInvalid identifier")
    assert code == "001003"
    assert "SQL compilation error" in msg


def test_parse_error_without_code():
    code, msg = _parse_snowflake_error("Something went wrong")
    assert code is None
    assert msg == "Something went wrong"


def test_parse_error_strips_noise():
    raw = "handed_over = True\n001003 (42000): SQL compilation error"
    code, msg = _parse_snowflake_error(raw)
    assert code == "001003"


# ------------------------------------------------------------------
# report_deploy_summary
# ------------------------------------------------------------------

def test_deploy_summary_success(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    report = report_deploy_summary(total=5, deployed=3, skipped=2, failed=0, elapsed=1.5)
    assert "SUCCESS" in report
    assert "5" in report


def test_deploy_summary_failed(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    report = report_deploy_summary(total=5, deployed=2, skipped=1, failed=2, elapsed=3.0)
    assert "FAILED" in report


# ------------------------------------------------------------------
# report_load_summary
# ------------------------------------------------------------------

def test_load_summary_success(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    report = report_load_summary(total=3, loaded=3, failed=0)
    assert "SUCCESS" in report


def test_load_summary_failed(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    report = report_load_summary(total=3, loaded=1, failed=2)
    assert "FAILED" in report
