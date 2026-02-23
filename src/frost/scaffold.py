"""Scaffold a new frost project with config, example SQL, and helper scripts."""

import os
import textwrap
from pathlib import Path


_FROST_CONFIG = textwrap.dedent("""\
    # ──────────────────────────────────────────────────────────────
    #  frost configuration
    # ──────────────────────────────────────────────────────────────

    # Folder containing SQL object definitions (scanned recursively)
    objects-folder: objects

    # ── Snowflake connection (override with env vars) ─────────────
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

    # ── Change tracking ───────────────────────────────────────────
    # frost stores deployment history in this Snowflake table
    tracking-database: FROST
    tracking-schema: METADATA
    tracking-table: DEPLOY_HISTORY

    # ── Variables ─────────────────────────────────────────────────
    # Substituted in SQL files as {{variable_name}}
    # Override with FROST_VARS env var (JSON) or --vars CLI flag
    variables:
      database_name: MY_DATABASE
      schema_name: PUBLIC
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
    # FROST_VARS='{"database_name": "MY_DATABASE", "schema_name": "PUBLIC"}'
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
    CREATE OR ALTER TABLE {{database_name}}.{{schema_name}}.SAMPLE_TABLE (
        ID              NUMBER          NOT NULL,
        NAME            VARCHAR(255)    NOT NULL,
        STATUS          VARCHAR(50)     DEFAULT 'ACTIVE',
        CREATED_AT      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
        CONSTRAINT PK_SAMPLE_TABLE PRIMARY KEY (ID)
    );
""")

_SAMPLE_VIEW = textwrap.dedent("""\
    CREATE OR ALTER VIEW {{database_name}}.{{schema_name}}.VW_ACTIVE_SAMPLES
    AS
    SELECT
        ID,
        NAME,
        CREATED_AT
    FROM
        {{database_name}}.{{schema_name}}.SAMPLE_TABLE
    WHERE
        STATUS = 'ACTIVE';
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
    }

    for rel_path, content in files.items():
        dest = root / rel_path
        if dest.exists():
            continue  # never overwrite
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content)
        created.append(rel_path)

    # empty dirs
    for d in ("keys", "objects/schemas", "objects/procedures", "objects/grants"):
        (root / d).mkdir(parents=True, exist_ok=True)

    return created
