"""Tests for frost.parser -- object extraction, violations, and dependencies.

Includes CSV data-driven tests loaded from tests/data/parser_cases.csv
and tests/data/dependency_cases.csv.
"""

import pytest

from frost.parser import SqlParser, ObjectDefinition
from helpers import load_csv


# ------------------------------------------------------------------
# CSV-driven: parser_cases.csv
# ------------------------------------------------------------------

_PARSER_CASES = load_csv("parser_cases.csv")


@pytest.mark.csv
@pytest.mark.parametrize(
    "row",
    _PARSER_CASES,
    ids=[r["description"] for r in _PARSER_CASES],
)
def test_parser_csv_cases(sql_file, row):
    """Each row in parser_cases.csv declares expected type, name, and violation count."""
    parser = SqlParser()
    path = sql_file(row["sql"])
    objs = parser.parse_file(path)

    assert len(objs) >= 1, f"Expected at least 1 object, got {len(objs)}"
    obj = objs[0]

    assert obj.object_type == row["expected_type"]
    assert obj.name == row["expected_name"]
    if row["expected_schema"]:
        assert obj.schema == row["expected_schema"]

    expected_v = int(row["expected_violations"])
    assert len(parser.violations) == expected_v, (
        f"Expected {expected_v} violations, got {len(parser.violations)}: "
        f"{[v.fqn for v in parser.violations]}"
    )


# ------------------------------------------------------------------
# CSV-driven: dependency_cases.csv
# ------------------------------------------------------------------

_DEP_CASES = load_csv("dependency_cases.csv")


@pytest.mark.csv
@pytest.mark.parametrize(
    "row",
    _DEP_CASES,
    ids=[r["description"] for r in _DEP_CASES],
)
def test_dependency_csv_cases(sql_file, row):
    """Each row in dependency_cases.csv declares expected object dependencies."""
    parser = SqlParser()
    row_sql = row["sql"].replace("\\n", "\n")  # CSV stores literal \n
    path = sql_file(row_sql)
    objs = parser.parse_file(path)

    assert len(objs) >= 1
    obj = objs[0]

    expected_deps = set(row["expected_deps"].split("|")) if row["expected_deps"] else set()
    # Filter out self-references (parser does this, but just in case)
    expected_deps.discard(obj.fqn)

    assert obj.dependencies == expected_deps, (
        f"Expected deps {expected_deps}, got {obj.dependencies}"
    )


# ------------------------------------------------------------------
# Unit tests: dollar-quoted block stripping
# ------------------------------------------------------------------

def test_procedure_body_not_scanned(sql_file):
    """CREATE OR REPLACE TABLE inside $$ procedure body must not be detected."""
    sql = """\
CREATE OR ALTER PROCEDURE PUBLIC.SP_LOAD()
RETURNS STRING LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE TABLE TEMP_STAGING (id INT);
    INSERT INTO TEMP_STAGING VALUES (1);
    DROP TABLE TEMP_STAGING;
    RETURN 'done';
END;
$$;
"""
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 1
    assert objs[0].object_type == "PROCEDURE"
    assert objs[0].name == "SP_LOAD"
    assert len(parser.violations) == 0


def test_tagged_dollar_quote_stripped(sql_file):
    """$body$...$body$ tagged blocks should also be stripped."""
    sql = """\
CREATE OR ALTER PROCEDURE PUBLIC.SP_TAGGED()
RETURNS STRING LANGUAGE SQL
AS $body$
BEGIN
    CREATE OR REPLACE TABLE PHANTOM (x INT);
    RETURN 'ok';
END;
$body$;
"""
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 1
    assert objs[0].name == "SP_TAGGED"
    assert len(parser.violations) == 0


# ------------------------------------------------------------------
# Unit tests: variable substitution
# ------------------------------------------------------------------

def test_variable_substitution(sql_file):
    """{{var}} placeholders should be replaced in resolved_sql."""
    sql = "CREATE OR ALTER TABLE {{schema}}.MY_TABLE (id INT);"
    parser = SqlParser(variables={"schema": "STAGING"})
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 1
    assert objs[0].schema == "STAGING"
    assert "{{schema}}" not in objs[0].resolved_sql


# ------------------------------------------------------------------
# Unit tests: USE context
# ------------------------------------------------------------------

def test_use_database_context(sql_file):
    """USE DATABASE sets the context but parser uses 2-part names (no db injection).

    For a 2-part name like PUBLIC.T1 the parser resolves database=None.
    Only 3-part names or unqualified names use the USE context.
    """
    sql = """\
USE DATABASE MY_DB;
CREATE OR ALTER TABLE MY_DB.PUBLIC.T1 (id INT);
"""
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 1
    assert objs[0].database == "MY_DB"


def test_use_schema_context(sql_file):
    """USE SCHEMA should set the default schema for unqualified names."""
    sql = """\
USE SCHEMA ANALYTICS;
CREATE OR ALTER TABLE T1 (id INT);
"""
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 1
    assert objs[0].schema == "ANALYTICS"


# ------------------------------------------------------------------
# Unit tests: multi-object files
# ------------------------------------------------------------------

def test_multi_object_file(sql_file):
    """A file with multiple CREATE statements should produce multiple objects."""
    sql = """\
CREATE OR ALTER TABLE PUBLIC.T1 (id INT);
CREATE OR ALTER VIEW PUBLIC.V1 AS SELECT * FROM PUBLIC.T1;
"""
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 2
    assert objs[0].name == "T1"
    assert objs[1].name == "V1"
    assert "PUBLIC.T1" in objs[1].dependencies


# ------------------------------------------------------------------
# Unit tests: SCRIPT fallback
# ------------------------------------------------------------------

def test_script_fallback(sql_file):
    """A file with no CREATE statement should produce a SCRIPT object."""
    sql = "INSERT INTO PUBLIC.T1 VALUES (1);"
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))

    assert len(objs) == 1
    assert objs[0].object_type == "SCRIPT"


# ------------------------------------------------------------------
# Unit tests: checksum
# ------------------------------------------------------------------

def test_checksum_changes_with_content(sql_file):
    """Different SQL content should produce different checksums."""
    parser = SqlParser()
    o1 = parser.parse_file(sql_file("CREATE OR ALTER TABLE PUBLIC.T1 (id INT);", "a.sql"))

    parser2 = SqlParser()
    o2 = parser2.parse_file(sql_file("CREATE OR ALTER TABLE PUBLIC.T1 (id INT, name VARCHAR);", "b.sql"))

    assert o1[0].checksum != o2[0].checksum


# ------------------------------------------------------------------
# Unit tests: CREATE OR REPLACE enforcement boundary
# ------------------------------------------------------------------

def test_replace_allowed_for_procedure(sql_file):
    """CREATE OR REPLACE PROCEDURE must NOT trigger a violation."""
    parser = SqlParser()
    parser.parse_file(sql_file("CREATE OR REPLACE PROCEDURE PUBLIC.P() RETURNS STRING LANGUAGE SQL AS $$'ok'$$;"))
    assert len(parser.violations) == 0


def test_replace_allowed_for_function(sql_file):
    """CREATE OR REPLACE FUNCTION must NOT trigger a violation."""
    parser = SqlParser()
    parser.parse_file(sql_file("CREATE OR REPLACE FUNCTION PUBLIC.F() RETURNS INT AS $$1$$;"))
    assert len(parser.violations) == 0


def test_replace_triggers_violation_for_table(sql_file):
    """CREATE OR REPLACE TABLE MUST trigger a violation."""
    parser = SqlParser()
    parser.parse_file(sql_file("CREATE OR REPLACE TABLE PUBLIC.T(id INT);"))
    assert len(parser.violations) == 1
    assert parser.violations[0].found_form == "CREATE OR REPLACE"


def test_replace_triggers_violation_for_view(sql_file):
    """CREATE OR REPLACE VIEW MUST trigger a violation."""
    parser = SqlParser()
    parser.parse_file(sql_file("CREATE OR REPLACE VIEW PUBLIC.V AS SELECT 1;"))
    assert len(parser.violations) == 1


def test_plain_create_triggers_violation_for_enforced_types(sql_file):
    """Plain CREATE (without OR ALTER) triggers a violation for enforced types."""
    parser = SqlParser()
    parser.parse_file(sql_file("CREATE TABLE PUBLIC.T(id INT);"))
    assert len(parser.violations) == 1
    assert parser.violations[0].found_form == "CREATE"
