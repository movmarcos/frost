"""Scaffold a new frost project with config, example SQL, and helper scripts."""

import textwrap
from pathlib import Path


_FROST_CONFIG = textwrap.dedent("""\
    # -----------------------------------------------------------
    #  frost configuration
    # -----------------------------------------------------------

    # Folder containing SQL object definitions (scanned recursively)
    objects-folder: objects

    # Folder containing CSV data files for 'frost load'
    data-folder: data

    # Target schema for CSV data tables (override with FROST_DATA_SCHEMA env var)
    data-schema: PUBLIC

    # -- Snowflake connection (override with env vars) ----------
    # Environment variables take precedence:
    #   SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE,
    #   SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    #   SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    account: null
    user: null
    role: SYSADMIN
    warehouse: COMPUTE_WH
    database: null
    private-key-path: null
    private-key-passphrase: null

    # -- Change tracking ----------------------------------------
    # frost stores deployment history as a schema in the target database
    # e.g. MY_DATABASE.FROST.DEPLOY_HISTORY
    tracking-schema: FROST
    tracking-table: DEPLOY_HISTORY

    # -- Variables ----------------------------------------------
    # Substituted in SQL files as {{variable_name}}
    # Override with FROST_VARS env var (JSON) or --vars CLI flag
    variables: {}
""")

_ENV_EXAMPLE = textwrap.dedent("""\
    # -----------------------------------------------------------
    # Environment variables for frost (Snowflake DDL manager)
    # -----------------------------------------------------------
    # Copy this file to .env and fill in the values.
    # NEVER commit .env to version control.
    # -----------------------------------------------------------

    # Snowflake account identifier (e.g., xy12345.us-east-1)
    SNOWFLAKE_ACCOUNT=

    # Snowflake username
    SNOWFLAKE_USER=

    # Snowflake role
    SNOWFLAKE_ROLE=SYSADMIN

    # Snowflake warehouse
    SNOWFLAKE_WAREHOUSE=COMPUTE_WH

    # Snowflake target database
    SNOWFLAKE_DATABASE=

    # Path to the RSA private key file (PEM format)
    SNOWFLAKE_PRIVATE_KEY_PATH=./keys/rsa_key.p8

    # Private key passphrase (leave empty if key is not encrypted)
    SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=

    # -----------------------------------------------------------
    # frost variables (JSON format)
    # -----------------------------------------------------------
    # FROST_VARS='{"admin_role": "SYSADMIN", "read_role": "READ_ONLY_ROLE"}'
""")

_GITIGNORE = textwrap.dedent("""\
    # Secrets and credentials
    .env
    keys/
    *.p8
    *.pem

    # Python
    __pycache__/
    *.py[cod]
    *.egg-info/
    dist/
    build/
    .venv/
    venv/

    # OS files
    .DS_Store
    Thumbs.db

    # IDE
    .idea/
    .vscode/
    *.swp
    *.swo
""")

_SAMPLE_TABLE = textwrap.dedent("""\
    CREATE OR ALTER TABLE PUBLIC.SAMPLE_TABLE (
        ID              NUMBER          NOT NULL,
        NAME            VARCHAR(255)    NOT NULL,
        STATUS          VARCHAR(50)     DEFAULT 'ACTIVE',
        CREATED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT PK_SAMPLE_TABLE PRIMARY KEY (ID)
    );
""")

_SAMPLE_VIEW = textwrap.dedent("""\
    CREATE OR ALTER VIEW PUBLIC.VW_ACTIVE_SAMPLES
    AS
    SELECT
        ID,
        NAME,
        CREATED_AT
    FROM
        PUBLIC.SAMPLE_TABLE
    WHERE
        STATUS = 'ACTIVE';
""")

_SAMPLE_CSV = textwrap.dedent("""\
id,name,status
1,Alice,ACTIVE
2,Bob,ACTIVE
3,Charlie,INACTIVE
""")

_SAMPLE_CSV_YML = textwrap.dedent("""\
# Column type overrides for sample_users.csv
# Columns not listed here default to VARCHAR.
columns:
  id: NUMBER

# -----------------------------------------------------------
#  Data-quality tests for sample_users.csv
# -----------------------------------------------------------
# Run with:
#   frost test                           (all files in data/)
#   frost test sample_users              (this file only)

tests:
  - name: users_id_unique
    column: id
    test: unique

  - name: users_id_not_null
    column: id
    test: not_null

  - name: users_status_values
    column: status
    test: accepted_values
    values: [ACTIVE, INACTIVE, PENDING]

  - name: users_has_rows
    test: row_count
    min: 1
""")

_SAMPLE_PROCEDURE = textwrap.dedent("""\
    CREATE OR ALTER PROCEDURE PUBLIC.REFRESH_ACTIVE_SAMPLES()
    RETURNS VARCHAR
    LANGUAGE SQL
    AS
    $$
    BEGIN
        TRUNCATE TABLE IF EXISTS PUBLIC.ACTIVE_SUMMARY;
        INSERT INTO PUBLIC.ACTIVE_SUMMARY
        SELECT ID, NAME, CREATED_AT
        FROM PUBLIC.SAMPLE_TABLE
        WHERE STATUS = 'ACTIVE';
        RETURN 'OK';
    END;
    $$;
""")

_SAMPLE_PROCEDURE_YML = textwrap.dedent("""\
    # -----------------------------------------------------------
    #  Lineage declaration for refresh_active_samples procedure
    # -----------------------------------------------------------
    # frost uses this to document data flow (not enforced at deploy).
    # Shown by 'frost graph' and stored in FROST.OBJECT_LINEAGE.

    sources:
      - PUBLIC.SAMPLE_TABLE

    targets:
      - PUBLIC.ACTIVE_SUMMARY

    description: >
      Refreshes the active-samples summary table from the main table.
""")


def scaffold(target_dir: str) -> list[str]:
    """Create a frost project scaffold in *target_dir*.

    Returns a list of relative paths that were created.
    """
    root = Path(target_dir)
    created: list[str] = []

    files: dict[str, str] = {
        "frost-config.yml":               _FROST_CONFIG,
        ".env.example":                    _ENV_EXAMPLE,
        ".gitignore":                      _GITIGNORE,
        "objects/tables/sample_table.sql": _SAMPLE_TABLE,
        "objects/views/vw_active_samples.sql": _SAMPLE_VIEW,
        "objects/procedures/refresh_active_samples.sql": _SAMPLE_PROCEDURE,
        "objects/procedures/refresh_active_samples.yml": _SAMPLE_PROCEDURE_YML,
        "data/sample_users.csv":                _SAMPLE_CSV,
        "data/sample_users.yml":                _SAMPLE_CSV_YML,
    }

    for rel_path, content in files.items():
        dest = root / rel_path
        if dest.exists():
            continue  # never overwrite
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
        created.append(rel_path)

    # empty dirs
    for d in ("keys", "objects/schemas", "objects/procedures", "objects/grants", "data"):
        (root / d).mkdir(parents=True, exist_ok=True)

    return created
