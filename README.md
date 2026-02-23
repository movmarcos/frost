# frost

Declarative Snowflake DDL manager with automatic dependency resolution.

Write one SQL file per object, and frost figures out the correct deployment order — no numbered filenames, no manual ordering, no state files.

## How It Works

1. **You write SQL files** — one per object (`CREATE OR ALTER TABLE ...`, `CREATE OR REPLACE VIEW ...`)
2. **frost parses each file** — extracts what it creates and what it references (`FROM`, `JOIN`, `REFERENCES`, etc.)
3. **Builds a dependency graph** (DAG) — determines the safe execution order via topological sort
4. **Compares checksums** — only deploys changed objects + their dependents (cascade)
5. **Executes in topological order** — dependencies always run first

## Features

| Feature | Description |
|---------|-------------|
| **Auto dependency resolution** | Parses `FROM`, `JOIN`, `REFERENCES`, `ON TABLE`, `GRANT ON` to build the DAG |
| **No manual ordering** | No numbered prefixes, no `V`/`R` conventions |
| **Cascade re-deploy** | If table A changes, all views depending on A are also re-deployed |
| **Checksum tracking** | Only changed files are executed (tracked in Snowflake, not on disk) |
| **Cycle detection** | Reports circular dependencies before deploying |
| **Explicit overrides** | `-- @depends_on: SCHEMA.OBJECT` for edge cases the parser can’t detect |
| **Dry run** | Preview the execution plan without touching Snowflake |
| **Key pair auth** | RSA key pair authentication (no passwords) |
| **Variable substitution** | `{{variable_name}}` in SQL files |
| **CSV data loading** | Load static data from CSV files into Snowflake tables (`frost load`) |

## Installation

```bash
pip install frost-ddl
```

Or install from source:

```bash
pip install git+https://github.com/movmarcos/frost.git
```

## Quick Start

### 1. Initialize a new project

```bash
frost init my-snowflake-project
cd my-snowflake-project
```

This creates:

```
my-snowflake-project/
├── frost-config.yml               # Configuration file
├── .env.example                   # Environment variable template
├── .gitignore
├── objects/                       # Your SQL object definitions
│   ├── tables/
│   │   └── sample_table.sql       # Example table
│   ├── views/
│   │   └── vw_active_samples.sql  # Example view (depends on sample_table)
│   ├── schemas/
│   ├── procedures/
│   └── grants/
└── data/                          # CSV data files for 'frost load'
    ├── sample_users.csv           # Example CSV
    └── sample_users.yml           # Column type overrides
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake account, user, database, and key path
```

### 3. Preview the execution plan

```bash
frost plan
```

```
Execution order:
  1. [TABLE] SAMPLE_TABLE
  2. [VIEW]  VW_ACTIVE_SAMPLES  ← depends on: SAMPLE_TABLE
```

### 4. Deploy

```bash
frost deploy --dry-run   # preview first
frost deploy             # apply changes
```

## Writing SQL Objects

### One file per object

Each `.sql` file under your objects folder should define **one** Snowflake object. frost scans the folder recursively — organize however you like:

```
objects/
├── tables/
│   ├── users.sql
│   └── orders.sql
├── views/
│   └── vw_order_summary.sql
├── procedures/
│   └── sp_refresh_cache.sql
└── grants/
    └── read_only_grants.sql
```

### Supported DDL patterns

Use `CREATE OR ALTER` for tables and views, `CREATE OR REPLACE` for procedures:

```sql
CREATE OR ALTER TABLE MY_SCHEMA.ORDERS (
    ID          NUMBER NOT NULL,
    USER_ID     NUMBER NOT NULL,
    AMOUNT      DECIMAL(12,2),
    CREATED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT FK_USER REFERENCES MY_SCHEMA.USERS(ID)
);
```

### Automatic dependency detection

frost automatically detects these patterns:

| Pattern | Example |
|---------|---------|
| `FROM` | `FROM schema.table_name` |
| `JOIN` | `LEFT JOIN schema.other_table` |
| `REFERENCES` | `REFERENCES parent_table(id)` |
| `ON TABLE` | `CREATE STREAM ... ON TABLE ...` |
| `ON VIEW` | `CREATE STREAM ... ON VIEW ...` |
| `GRANT ON` | `GRANT SELECT ON TABLE ...` |

### Explicit dependencies

When the parser can't detect a dependency (e.g., dynamic SQL), use a comment annotation:

```sql
-- @depends_on: MY_SCHEMA.TABLE_A, MY_SCHEMA.VIEW_B

CREATE OR REPLACE PROCEDURE my_proc()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
  EXECUTE IMMEDIATE 'SELECT * FROM TABLE_A';
$$;
```

### Variables

Use `{{variable_name}}` in SQL files. Variables are loaded from (highest priority first):

1. `--vars` CLI flag (JSON)
2. `FROST_VARS` environment variable (JSON)
3. `frost-config.yml` → `variables:` section

## Data Loading

frost can load static reference data from CSV files into Snowflake tables — no manual DDL needed.

### Setup

Place `.csv` files in the `data/` folder (configurable via `data-folder` in config):

```
data/
├── countries.csv
├── countries.yml      # optional: column type overrides
├── currencies.csv
└── status_codes.csv
```

### CSV format

The first row must be headers. Values are inserted as-is; empty strings and `NULL` become SQL `NULL`:

```csv
code,name,region
US,United States,North America
CA,Canada,North America
GB,United Kingdom,Europe
```

### Column type overrides

By default, all columns are `VARCHAR`. To override, create a YAML sidecar file with the same name:

```yaml
# countries.yml
columns:
  code: VARCHAR(10)
  name: VARCHAR(100)
  region: VARCHAR(50)
```

### Loading data

```bash
frost load --dry-run   # preview what would be loaded
frost load             # create tables and insert data
```

frost will:
1. `CREATE OR REPLACE TABLE` with the columns from the CSV header
2. `INSERT INTO … VALUES` in batches of 1,000 rows
3. Track checksums — unchanged CSVs are skipped on subsequent runs

### Table naming

The table name is derived from the CSV filename:
- `countries.csv` → `PUBLIC.COUNTRIES`
- `status_codes.csv` → `PUBLIC.STATUS_CODES`

The database is set at connection level; the schema defaults to `PUBLIC`. Override it with `data-schema` in config or `--data-schema` on the CLI:

```bash
frost load --data-schema STAGING
```

## CLI Reference

```
frost init [directory]             Scaffold a new frost project
frost plan                         Show execution order (no Snowflake connection)
frost deploy                       Deploy changes to Snowflake
frost deploy --dry-run             Preview deployment without executing
frost load                         Load CSV data files into Snowflake
frost load --dry-run               Preview data loading without executing
frost load --data-folder DIR       Override data folder path
frost load --data-schema SCHEMA    Override target schema (default: PUBLIC)
frost graph                        Show the dependency graph
```

**Global flags:**

```
--config, -c FILE                  Config file (default: frost-config.yml)
--objects-folder, -f DIR           Override objects folder
--vars JSON                        Variable overrides as JSON string
--verbose, -v                      Enable debug logging
--version                          Show version
```

## Configuration

frost uses `frost-config.yml` at the project root. All values can be overridden with environment variables:

```yaml
objects-folder: objects
data-folder: data       # folder for CSV data files
data-schema: PUBLIC     # target schema for CSV tables (or FROST_DATA_SCHEMA env var)

# Snowflake connection
account: null          # or SNOWFLAKE_ACCOUNT env var
user: null             # or SNOWFLAKE_USER
role: SYSADMIN         # or SNOWFLAKE_ROLE
warehouse: COMPUTE_WH  # or SNOWFLAKE_WAREHOUSE
database: null         # or SNOWFLAKE_DATABASE
private-key-path: null # or SNOWFLAKE_PRIVATE_KEY_PATH
private-key-passphrase: null  # or SNOWFLAKE_PRIVATE_KEY_PASSPHRASE

# Change tracking (schema inside the target database)
tracking-schema: FROST
tracking-table: DEPLOY_HISTORY

# SQL variables (substituted as {{key}} in SQL files)
variables: {}
```

**Priority order:** CLI flags > environment variables > YAML config > defaults.

## Change Tracking

frost stores deployment history as a schema inside the target database (default: `FROST.DEPLOY_HISTORY`). The database is set at connection level via `USE DATABASE`. On each run:

1. Compares file checksums against the last successful deployment
2. Changed files are marked for deployment
3. **Cascade**: all objects that depend on a changed object are also re-deployed
4. Unchanged objects are skipped

No state file to manage — query history directly:

```sql
SELECT * FROM FROST.DEPLOY_HISTORY ORDER BY DEPLOYED_AT DESC;
```

## Multi-Environment

Use different environment variables or `.env` files per environment:

```bash
# Development
SNOWFLAKE_ACCOUNT=dev-acct SNOWFLAKE_DATABASE=DEV_DB frost deploy

# Production
SNOWFLAKE_ACCOUNT=prod-acct SNOWFLAKE_DATABASE=PROD_DB frost deploy
```

## Authentication

frost uses RSA key pair authentication. Generate a key pair:

```bash
# Generate unencrypted key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Register public key in Snowflake
# ALTER USER my_user SET RSA_PUBLIC_KEY='<contents of rsa_key.pub without header/footer>';
```

Set `SNOWFLAKE_PRIVATE_KEY_PATH` to point to your `.p8` file.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Circular dependency detected` | Check the cycle path in the error; restructure objects or use `@depends_on` |
| Object not detected | Ensure SQL uses `CREATE [OR ALTER/REPLACE] <TYPE> <name>` syntax |
| False dependency | The parser matched a keyword as an object name; add it to the exclusion list in `parser.py` |
| Missing dependency | Add `-- @depends_on: SCHEMA.OBJECT` comment to the file |
| `Private key not found` | Check `SNOWFLAKE_PRIVATE_KEY_PATH` in your `.env` or config |

## License

[MIT](LICENSE)
