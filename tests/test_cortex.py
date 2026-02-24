"""Tests for frost.cortex -- Cortex AI suggestion integration."""

from unittest.mock import MagicMock, patch

from frost.cortex import cortex_suggest, enrich_errors_with_cortex
from frost.reporter import DeployError


# ------------------------------------------------------------------
# cortex_suggest
# ------------------------------------------------------------------

def test_cortex_suggest_success(mock_connector):
    """cortex_suggest should return the stripped suggestion text."""
    mock_connector.execute_params.return_value = [("Fix the column name to ID.",)]

    result = cortex_suggest(
        mock_connector,
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="CREATE OR ALTER TABLE PUBLIC.T1 (idx INT);",
        error_message="Invalid identifier 'idx'",
    )

    assert result == "Fix the column name to ID."
    mock_connector.execute_params.assert_called_once()

    # Verify the SQL uses bind variables, not string interpolation
    call_args = mock_connector.execute_params.call_args
    assert "%s" in call_args[0][0]


def test_cortex_suggest_empty_response(mock_connector):
    """cortex_suggest should return None on empty Cortex response."""
    mock_connector.execute_params.return_value = [("",)]

    result = cortex_suggest(
        mock_connector,
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="SELECT 1",
        error_message="error",
    )

    assert result is None


def test_cortex_suggest_exception(mock_connector):
    """cortex_suggest should return None when Cortex raises an exception."""
    mock_connector.execute_params.side_effect = Exception("Cortex not available")

    result = cortex_suggest(
        mock_connector,
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="SELECT 1",
        error_message="error",
    )

    assert result is None


def test_cortex_suggest_no_rows(mock_connector):
    """cortex_suggest should return None when no rows returned."""
    mock_connector.execute_params.return_value = []

    result = cortex_suggest(
        mock_connector,
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql="SELECT 1",
        error_message="error",
    )

    assert result is None


def test_cortex_suggest_trims_sql(mock_connector):
    """SQL longer than 3000 chars should be trimmed in the prompt."""
    mock_connector.execute_params.return_value = [("suggestion",)]
    long_sql = "SELECT " + "x" * 5000

    cortex_suggest(
        mock_connector,
        fqn="PUBLIC.T1",
        object_type="TABLE",
        file_path="t1.sql",
        sql=long_sql,
        error_message="error",
    )

    # Verify the prompt (second bind param) doesn't contain the full 5000 chars
    call_args = mock_connector.execute_params.call_args
    prompt = call_args[0][1][1]  # second param tuple, second element
    assert len(prompt) < len(long_sql)


# ------------------------------------------------------------------
# enrich_errors_with_cortex
# ------------------------------------------------------------------

def test_enrich_attaches_suggestions(mock_connector):
    """enrich_errors_with_cortex should set ai_suggestion on DeployError objects."""
    mock_connector.execute_params.return_value = [("Use CREATE OR ALTER instead.",)]

    errors = [
        DeployError(
            fqn="PUBLIC.T1",
            object_type="TABLE",
            file_path="t1.sql",
            sql="CREATE OR REPLACE TABLE PUBLIC.T1 (id INT);",
            error_message="some error",
        ),
    ]

    count = enrich_errors_with_cortex(mock_connector, errors)
    assert count == 1
    assert errors[0].ai_suggestion == "Use CREATE OR ALTER instead."


def test_enrich_max_suggestions(mock_connector):
    """enrich_errors_with_cortex should only process _MAX_SUGGESTIONS errors."""
    mock_connector.execute_params.return_value = [("fix",)]

    errors = [
        DeployError(fqn=f"PUBLIC.T{i}", object_type="TABLE",
                    file_path=f"t{i}.sql", sql="SELECT 1", error_message="err")
        for i in range(10)
    ]

    count = enrich_errors_with_cortex(mock_connector, errors)

    # Default _MAX_SUGGESTIONS is 3
    assert count == 3
    assert errors[0].ai_suggestion == "fix"
    assert errors[3].ai_suggestion is None  # beyond limit


def test_enrich_partial_failure(mock_connector):
    """If some Cortex calls fail, enriched count reflects that."""
    mock_connector.execute_params.side_effect = [
        [("suggestion 1",)],
        Exception("fail"),
        [("suggestion 3",)],
    ]

    errors = [
        DeployError(fqn=f"PUBLIC.T{i}", object_type="TABLE",
                    file_path=f"t{i}.sql", sql="SELECT 1", error_message="err")
        for i in range(3)
    ]

    count = enrich_errors_with_cortex(mock_connector, errors)
    assert count == 2
    assert errors[0].ai_suggestion == "suggestion 1"
    assert errors[1].ai_suggestion is None
    assert errors[2].ai_suggestion == "suggestion 3"
