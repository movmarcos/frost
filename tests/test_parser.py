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


# ------------------------------------------------------------------
# DROP statement detection
# ------------------------------------------------------------------

def test_drop_table_detected(sql_file):
    """DROP TABLE should produce an ObjectDefinition with is_drop=True."""
    parser = SqlParser()
    objs = parser.parse_file(sql_file("DROP TABLE PUBLIC.OLD_TABLE;"))
    drops = [o for o in objs if o.is_drop]
    assert len(drops) == 1
    assert drops[0].name == "OLD_TABLE"
    assert drops[0].object_type == "TABLE"
    assert drops[0].is_drop is True


def test_drop_if_exists_detected(sql_file):
    """DROP ... IF EXISTS should also be detected."""
    parser = SqlParser()
    objs = parser.parse_file(sql_file("DROP VIEW IF EXISTS PUBLIC.LEGACY_VIEW;"))
    drops = [o for o in objs if o.is_drop]
    assert len(drops) == 1
    assert drops[0].name == "LEGACY_VIEW"
    assert drops[0].object_type == "VIEW"


def test_drop_with_three_part_name(sql_file):
    """DROP with fully-qualified 3-part name."""
    parser = SqlParser()
    objs = parser.parse_file(sql_file("DROP PROCEDURE IF EXISTS MYDB.PUBLIC.OLD_PROC;"))
    drops = [o for o in objs if o.is_drop]
    assert len(drops) == 1
    assert drops[0].fqn == "MYDB.PUBLIC.OLD_PROC"


def test_create_and_drop_in_same_file(sql_file):
    """A file with both CREATE and DROP should produce both object types."""
    sql = """\
        CREATE OR ALTER TABLE PUBLIC.NEW_T (ID INT);
        DROP TABLE IF EXISTS PUBLIC.OLD_T;
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    creates = [o for o in objs if not o.is_drop]
    drops = [o for o in objs if o.is_drop]
    assert len(creates) == 1
    assert len(drops) == 1
    assert creates[0].name == "NEW_T"
    assert drops[0].name == "OLD_T"


def test_drop_only_file_not_treated_as_script(sql_file):
    """A file containing only a DROP should not produce a SCRIPT object."""
    parser = SqlParser()
    objs = parser.parse_file(sql_file("DROP TABLE IF EXISTS PUBLIC.GONE;"))
    # Should have the drop object but NOT a SCRIPT fallback
    assert all(o.object_type != "SCRIPT" for o in objs)
    assert any(o.is_drop for o in objs)


def test_default_is_drop_false(sql_file):
    """Normal CREATE objects have is_drop=False."""
    parser = SqlParser()
    objs = parser.parse_file(sql_file("CREATE OR ALTER TABLE PUBLIC.T(id INT);"))
    for o in objs:
        assert o.is_drop is False


# ------------------------------------------------------------------
# Column extraction
# ------------------------------------------------------------------


def test_columns_from_create_table(sql_file):
    """Column names and types are extracted from a CREATE TABLE definition."""
    sql = """
    CREATE OR ALTER TABLE PUBLIC.USERS (
        USER_ID   NUMBER(10,0)  NOT NULL,
        EMAIL     VARCHAR(255),
        STATUS    VARCHAR(20) DEFAULT 'ACTIVE',
        CREATED   TIMESTAMP_NTZ
    );
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    tbl = [o for o in objs if o.object_type == "TABLE"][0]
    assert len(tbl.columns) == 4
    assert tbl.columns[0] == {"name": "USER_ID", "type": "NUMBER(10,0)"}
    assert tbl.columns[1] == {"name": "EMAIL", "type": "VARCHAR(255)"}
    assert tbl.columns[2] == {"name": "STATUS", "type": "VARCHAR(20)"}
    assert tbl.columns[3] == {"name": "CREATED", "type": "TIMESTAMP_NTZ"}


def test_columns_skip_constraints(sql_file):
    """Constraint keywords (PRIMARY KEY, UNIQUE, etc.) are not treated as columns."""
    sql = """
    CREATE OR ALTER TABLE PUBLIC.ORDERS (
        ORDER_ID NUMBER NOT NULL,
        AMOUNT   FLOAT,
        PRIMARY KEY (ORDER_ID),
        UNIQUE (AMOUNT),
        CONSTRAINT chk_amt CHECK (AMOUNT > 0)
    );
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    tbl = [o for o in objs if o.object_type == "TABLE"][0]
    names = [c["name"] for c in tbl.columns]
    assert names == ["ORDER_ID", "AMOUNT"]
    assert tbl.columns[0]["type"] == "NUMBER"
    assert tbl.columns[1]["type"] == "FLOAT"


def test_columns_from_view_select(sql_file):
    """Views without an explicit header column list return no columns.

    Column extraction from SELECT is unreliable (CTEs, sub-queries, SELECT *).
    Only explicit header columns are extracted.
    """
    sql = "CREATE OR ALTER VIEW PUBLIC.V AS SELECT 1 AS ID;"
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    assert view.columns == []


def test_columns_from_view_header(sql_file):
    """Views with explicit header column list extract columns correctly."""
    sql = "CREATE OR ALTER VIEW PUBLIC.V(ID) AS SELECT 1;"
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    assert len(view.columns) == 1
    assert view.columns[0] == {"name": "ID", "type": ""}


def test_columns_view_with_aliases(sql_file):
    """View without header returns no columns even with aliases."""
    sql = """
    CREATE OR ALTER VIEW PUBLIC.VW_SUMMARY AS
    SELECT
        s.id            AS sample_id,
        s.name          AS sample_name,
        COUNT(o.order_id) AS order_count,
        SUM(o.total_amount) AS total_amount
    FROM PUBLIC.SAMPLES s
    LEFT JOIN PUBLIC.ORDERS o ON o.sample_id = s.id
    GROUP BY s.id, s.name;
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    assert view.columns == []


def test_columns_view_header_with_aliases(sql_file):
    """View with explicit header columns extracts from the header."""
    sql = """
    CREATE OR ALTER VIEW PUBLIC.VW_SUMMARY(SAMPLE_ID, SAMPLE_NAME, ORDER_COUNT, TOTAL_AMOUNT) AS
    SELECT
        s.id, s.name, COUNT(o.order_id), SUM(o.total_amount)
    FROM PUBLIC.SAMPLES s
    LEFT JOIN PUBLIC.ORDERS o ON o.sample_id = s.id
    GROUP BY s.id, s.name;
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    names = [c["name"] for c in view.columns]
    assert names == ["SAMPLE_ID", "SAMPLE_NAME", "ORDER_COUNT", "TOTAL_AMOUNT"]
    assert all(c["type"] == "" for c in view.columns)


def test_columns_view_without_aliases(sql_file):
    """View without header and without aliases returns empty."""
    sql = """
    CREATE OR ALTER VIEW PUBLIC.VW_PLAIN AS
    SELECT id, name, status
    FROM PUBLIC.MY_TABLE;
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    assert view.columns == []


def test_columns_view_cte_with_header(sql_file):
    """View with CTE + header extracts from the header, not CTE body."""
    sql = """
    CREATE OR ALTER VIEW PUBLIC.VW_CTE(ID, NAME) AS
    WITH base AS (SELECT id, name, status FROM PUBLIC.SRC)
    SELECT id, name FROM base;
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    names = [c["name"] for c in view.columns]
    assert names == ["ID", "NAME"]


def test_columns_view_cte_without_header(sql_file):
    """View with CTE but no header returns empty columns."""
    sql = """
    CREATE OR ALTER VIEW PUBLIC.VW_CTE AS
    WITH base AS (SELECT id, name, status FROM PUBLIC.SRC)
    SELECT id, name FROM base;
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    assert view.columns == []


def test_columns_view_star_returns_empty(sql_file):
    """SELECT * views return no columns (we can't resolve them statically)."""
    sql = "CREATE OR ALTER VIEW PUBLIC.V AS SELECT * FROM PUBLIC.TBL;"
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    view = [o for o in objs if o.object_type == "VIEW"][0]
    assert view.columns == []


def test_columns_empty_for_procedure(sql_file):
    """Procedures have no columns."""
    sql = "CREATE OR REPLACE PROCEDURE PUBLIC.P() RETURNS VARCHAR LANGUAGE SQL AS 'SELECT 1';"
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    proc = [o for o in objs if o.object_type == "PROCEDURE"][0]
    assert proc.columns == []


def test_columns_nested_parens(sql_file):
    """Columns with nested parens (e.g. NUMBER(10,2)) are handled correctly."""
    sql = """
    CREATE OR ALTER TABLE PUBLIC.PRICES (
        PRICE_ID  NUMBER(10,0),
        VALUE     NUMBER(18,4),
        CURRENCY  VARCHAR(3)
    );
    """
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    tbl = [o for o in objs if o.object_type == "TABLE"][0]
    names = [c["name"] for c in tbl.columns]
    assert names == ["PRICE_ID", "VALUE", "CURRENCY"]
    assert tbl.columns[0]["type"] == "NUMBER(10,0)"
    assert tbl.columns[1]["type"] == "NUMBER(18,4)"
    assert tbl.columns[2]["type"] == "VARCHAR(3)"


def test_columns_quoted_identifiers(sql_file):
    """Double-quoted column names are extracted without quotes."""
    sql = '''
    CREATE OR ALTER TABLE PUBLIC.MIXED (
        "user id"  NUMBER,
        STATUS     VARCHAR
    );
    '''
    parser = SqlParser()
    objs = parser.parse_file(sql_file(sql))
    tbl = [o for o in objs if o.object_type == "TABLE"][0]
    names = [c["name"] for c in tbl.columns]
    assert "STATUS" in names
    assert len(tbl.columns) == 2
    # Types should be captured
    types = {c["name"]: c["type"] for c in tbl.columns}
    assert types["STATUS"] == "VARCHAR"
