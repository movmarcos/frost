# frost — Declarative Snowflake DDL Manager

A custom tool that **automatically resolves dependencies** between Snowflake objects and deploys them in the correct order. No numbered filenames, no manual ordering.

## How It Works

1. **You write SQL files** — one per object, organized however you like
2. **frost parses each file** — extracts what it creates and what it references (`FROM`, `JOIN`, `REFERENCES`, etc.)
3. **Builds a dependency graph** (DAG) — determines the safe execution order
4. **Compares checksums** — only deploys changed objects + their dependents (cascade)
5. **Executes in topological order** — dependencies always run first

```
objects/
├── tables/
│   ├── sample_table.sql          ← defines sample_table
│   └── orders.sql                ← REFERENCES sample_table → frost knows it runs after
├── views/
│   ├── vw_active_samples.sql     ← FROM sample_table → runs after sample_table
│   └── vw_order_summary.sql      ← FROM vw_active_samples JOIN orders → runs after both
├── procedures/
│   └── sp_get_sample_count.sql   ← FROM sample_table → runs after sample_table
└── grants/
    └── read_only_grants.sql      ← @depends_on annotation → runs last
```

frost figures out the order automatically:

```
  1. [TABLE] SAMPLE_TABLE
  2. [TABLE] ORDERS                  ← depends on: SAMPLE_TABLE
  3. [VIEW]  VW_ACTIVE_SAMPLES       ← depends on: SAMPLE_TABLE
  4. [PROCEDURE] SP_GET_SAMPLE_COUNT  ← depends on: SAMPLE_TABLE
  5. [VIEW]  VW_ORDER_SUMMARY        ← depends on: VW_ACTIVE_SAMPLES, ORDERS
  6. [SCRIPT] READ_ONLY_GRANTS       ← depends on: all of the above
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Auto dependency resolution** | Parses `FROM`, `JOIN`, `REFERENCES`, `ON TABLE`, `GRANT ON` to build the DAG |
| **No manual ordering** | No numbered prefixes, no `V`/`R` conventions |
| **Cascade re-deploy** | If table A changes, all views depending on A are also re-deployed |
| **Checksum tracking** | Only changed files are executed (tracked in Snowflake) |
| **No state file** | Deployment history lives in Snowflake, not on disk |
| **Cycle detection** | Reports circular dependencies before deploying |
| **Explicit overrides** | `-- @depends_on: SCHEMA.OBJECT` for edge cases the parser can't detect |
| **Dry run** | Preview the execution plan without touching Snowflake |
| **Key pair auth** | RSA key pair authentication (no passwords) |
| **Variable substitution** | `{{variable_name}}` in SQL files |

---

## Project Structure

```
├── frost/                         # The tool (Python package)
│   ├── __init__.py
│   ├── __main__.py                # python -m frost
│   ├── cli.py                     # CLI: plan / deploy / graph
│   ├── parser.py                  # SQL parser (object + dependency extraction)
│   ├── graph.py                   # DAG + topological sort
│   ├── connector.py               # Snowflake connection (key pair auth)
│   ├── deployer.py                # Orchestration engine
│   ├── tracker.py                 # Checksum tracking in Snowflake
│   └── config.py                  # YAML + env var config loader
├── objects/                       # SQL object definitions (your code)
│   ├── databases/
│   ├── schemas/
│   ├── tables/
│   ├── views/
│   ├── procedures/
│   └── grants/
├── keys/                          # RSA keys (git-ignored)
├── frost-config.yml               # Configuration
├── deploy.sh                      # Deployment wrapper
├── generate_key_pair.sh           # RSA key generator
├── setup.py                       # Package setup
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Setup

### 1. Install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .    # Install frost as a local package
```

### 2. Generate RSA key pair

```bash
chmod +x generate_key_pair.sh
./generate_key_pair.sh
```

### 3. Register the public key in Snowflake

```sql
ALTER USER my_service_user SET RSA_PUBLIC_KEY='MIIBIjANBgk...';
```

### 4. Configure environment

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

---

## Usage

### View the execution plan (no Snowflake connection needed)

```bash
python -m frost plan
```

### Deploy (with dry run)

```bash
python -m frost deploy --dry-run
```

### Deploy

```bash
python -m frost deploy
```

### View dependency graph

```bash
python -m frost graph
```

### Using the wrapper script

```bash
chmod +x deploy.sh

./deploy.sh plan                    # Show execution plan
./deploy.sh deploy --dry-run        # Preview deployment
./deploy.sh deploy                  # Deploy changes
./deploy.sh graph                   # Show dependency graph
```

### Verbose output

```bash
python -m frost -v deploy
```

---

## Writing SQL Objects

### One file per object

Each `.sql` file under `objects/` should define one object. frost scans the folder recursively — organize however you like.

### Supported patterns

frost automatically detects these patterns for dependency resolution:

| Pattern | Example |
|---------|---------|
| `FROM` | `FROM schema.table_name` |
| `JOIN` | `LEFT JOIN schema.other_table` |
| `REFERENCES` | `REFERENCES parent_table(id)` |
| `ON TABLE` | `CREATE STREAM ... ON TABLE ...` |
| `ON VIEW` | `CREATE STREAM ... ON VIEW ...` |
| `GRANT ON` | `GRANT SELECT ON TABLE ...` |

### Explicit dependencies (`@depends_on`)

When the parser can't detect a dependency (e.g., dynamic SQL, grants on multiple objects), use a comment annotation:

```sql
-- @depends_on: MY_DB.MY_SCHEMA.TABLE_A, MY_DB.MY_SCHEMA.VIEW_B

CREATE OR REPLACE PROCEDURE my_proc()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
  -- dynamic SQL that references TABLE_A
  EXECUTE IMMEDIATE 'SELECT * FROM TABLE_A';
$$;
```

### Variables

Use `{{variable_name}}` in SQL files. Variables are loaded from:
1. `frost-config.yml` → `variables:` section
2. `FROST_VARS` environment variable (JSON)
3. `--vars` CLI flag (JSON)

---

## Change Tracking

frost stores deployment history in Snowflake (default: `FROST.METADATA.DEPLOY_HISTORY`). On each run:

1. Compares file checksums against the last successful deployment
2. Changed files are marked for deployment
3. **Cascade**: all objects that depend on a changed object are also re-deployed
4. Unchanged objects are skipped

Query the history:

```sql
SELECT * FROM FROST.METADATA.DEPLOY_HISTORY ORDER BY DEPLOYED_AT DESC;
```

---

## Multi-Environment

No state file to manage. For different environments, just use different `.env` files or environment variables:

```bash
# Development
SNOWFLAKE_ACCOUNT=dev-account SNOWFLAKE_DATABASE=DEV_DB python -m frost deploy

# Production
SNOWFLAKE_ACCOUNT=prod-account SNOWFLAKE_DATABASE=PROD_DB python -m frost deploy
```

Or use separate `.env` files:

```bash
source .env.dev && ./deploy.sh deploy
source .env.prod && ./deploy.sh deploy
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Circular dependency detected` | Check the cycle path in the error; break it with object restructuring or `@depends_on` |
| Object not detected | Ensure the SQL uses `CREATE [OR ALTER/REPLACE] <TYPE> <name>` syntax |
| False dependency | The parser matched a keyword as an object name; add it to the keywords list in `parser.py` |
| Missing dependency | Add `-- @depends_on: DB.SCHEMA.OBJECT` to the file |
| `Private key not found` | Check `SNOWFLAKE_PRIVATE_KEY_PATH` in `.env` |
