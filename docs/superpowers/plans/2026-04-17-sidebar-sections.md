# Sidebar Sections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Configuration, Resource Explorer, and Deploy-target sidebar panels to the Frost VSCode extension, with per-resource lineage via Phase 1 subgraph.

**Architecture:** Shared `ConfigReader` parses `frost-config.yml` once and feeds both the Configuration tree and the Deploy header. A new `frost resources --json` CLI command queries live Snowflake via the existing `SnowflakeConnector` and returns all schema-scoped objects. The Resource Explorer tree renders that JSON, and its lineage action routes through the existing Phase 1 `LineagePanel`.

**Tech Stack:** Python 3.10+ (argparse, snowflake-connector-python), TypeScript (VSCode Extension API, TreeDataProvider), pytest.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/frost/resources.py` | Create | `fetch_resources(connector, database)` — query fanout |
| `src/frost/cli.py` | Modify | `_cmd_resources` handler + argparse `resources` subcommand |
| `tests/test_resources.py` | Create | Unit + CLI integration tests for resources |
| `vscode-frost/src/configReader.ts` | Create | Shared YAML config reader + cache |
| `vscode-frost/src/configTree.ts` | Create | Configuration panel tree provider |
| `vscode-frost/src/resourcesTree.ts` | Create | Resource Explorer tree provider |
| `vscode-frost/src/frostRunner.ts` | Modify | `ResourcesPayload` type + `resourcesJson()` method |
| `vscode-frost/src/deployTree.ts` | Modify | Add target header, accept `ConfigReader` |
| `vscode-frost/src/lineagePanel.ts` | Modify | Accept `pickObject` host-to-webview message |
| `vscode-frost/media/lineage/lineage.js` | Modify | Handle `pickObject` inbound message |
| `vscode-frost/src/extension.ts` | Modify | Register new providers, commands, watchers |
| `vscode-frost/package.json` | Modify | Declare new views, commands, menus |
| `vscode-frost/README.md` | Modify | Update features section with new panels |

---

## Task 1: Python `fetch_resources` with TDD

**Files:**
- Create: `src/frost/resources.py`
- Create: `tests/test_resources.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_resources.py`:

```python
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
                "SHOW TABLES IN SCHEMA PUBLIC": [
                    ("2025-01-01", "ORDERS", "MY_DB", "PUBLIC", "SYSADMIN",
                     0, 0, 0, "", "", "", "", "", "N", "N", "", "", ""),
                ],
                "SHOW TABLES IN SCHEMA ANALYTICS": [
                    ("2025-01-01", "METRICS", "MY_DB", "ANALYTICS", "ANALYST",
                     0, 0, 0, "", "", "", "", "", "N", "N", "", "", ""),
                ],
            },
        )
        result = fetch_resources(conn, "MY_DB")
        schemas = {r["schema"] for r in result["resources"]}
        assert "PUBLIC" in schemas
        assert "ANALYTICS" in schemas
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_resources.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'frost.resources'`

- [ ] **Step 3: Implement `fetch_resources`**

Create `src/frost/resources.py`:

```python
"""List live Snowflake resources for the VS Code Resource Explorer."""

import logging
from typing import Any, Dict, List

log = logging.getLogger("frost")

# Map frost-relevant resource type → SHOW command template.
# {schema} is replaced with the fully-qualified schema name.
RESOURCE_QUERIES: Dict[str, str] = {
    "TABLE":             "SHOW TABLES IN SCHEMA {schema}",
    "VIEW":              "SHOW VIEWS IN SCHEMA {schema}",
    "PROCEDURE":         "SHOW PROCEDURES IN SCHEMA {schema}",
    "FUNCTION":          "SHOW USER FUNCTIONS IN SCHEMA {schema}",
    "DYNAMIC TABLE":     "SHOW DYNAMIC TABLES IN SCHEMA {schema}",
    "FILE FORMAT":       "SHOW FILE FORMATS IN SCHEMA {schema}",
    "STAGE":             "SHOW STAGES IN SCHEMA {schema}",
    "TASK":              "SHOW TASKS IN SCHEMA {schema}",
    "TAG":               "SHOW TAGS IN SCHEMA {schema}",
    "STREAM":            "SHOW STREAMS IN SCHEMA {schema}",
    "PIPE":              "SHOW PIPES IN SCHEMA {schema}",
    "MATERIALIZED VIEW": "SHOW MATERIALIZED VIEWS IN SCHEMA {schema}",
    "ALERT":             "SHOW ALERTS IN SCHEMA {schema}",
    "EVENT TABLE":       "SHOW EVENT TABLES IN SCHEMA {schema}",
    "SEQUENCE":          "SHOW SEQUENCES IN SCHEMA {schema}",
}

_SKIP_SCHEMAS = {"INFORMATION_SCHEMA"}


def fetch_resources(connector: Any, database: str) -> Dict[str, Any]:
    """Query Snowflake for all schema-scoped resources in *database*.

    Returns a dict matching the ``frost resources --json`` output schema:
    ``{"database": ..., "resources": [...], "warnings": [...]}``.

    Individual SHOW queries that fail (e.g. insufficient privileges) are
    logged and added to ``warnings`` — they do not abort the operation.
    """
    resources: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # Discover schemas
    try:
        schema_rows = connector.execute_single(
            f"SHOW SCHEMAS IN DATABASE {database}"
        )
    except Exception as exc:
        log.error("Could not list schemas in %s: %s", database, exc)
        return {"database": database, "resources": [], "warnings": [str(exc)]}

    schemas = [
        row[1] for row in schema_rows
        if isinstance(row[1], str) and row[1].upper() not in _SKIP_SCHEMAS
    ]

    for schema in schemas:
        fq_schema = f"{database}.{schema}"
        for rtype, query_tpl in RESOURCE_QUERIES.items():
            try:
                rows = connector.execute_single(query_tpl.format(schema=fq_schema))
            except Exception as exc:
                msg = f"Could not list {rtype} in schema {schema}: {exc}"
                log.debug(msg)
                warnings.append(msg)
                continue

            for row in rows:
                name = row[1] if len(row) > 1 else ""
                if isinstance(name, str):
                    name = name.split("(")[0].strip().upper()
                created_on = str(row[0]) if row else ""
                # Owner is typically at index 4 for most SHOW commands
                owner = str(row[4]) if len(row) > 4 else ""
                # Comment position varies; last non-empty field is a guess —
                # prefer explicit index 17 (tables) or fall back to empty.
                comment = ""
                if len(row) > 17 and isinstance(row[17], str):
                    comment = row[17]
                elif len(row) > 8 and isinstance(row[8], str):
                    comment = row[8]

                resources.append({
                    "schema": schema,
                    "type": rtype,
                    "name": name,
                    "fqn": f"{schema}.{name}",
                    "created_on": created_on,
                    "owner": owner,
                    "comment": comment,
                })

    return {"database": database, "resources": resources, "warnings": warnings}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_resources.py -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/frost/resources.py tests/test_resources.py
git commit -m "feat: add fetch_resources for live Snowflake resource listing"
```

---

## Task 2: Wire `frost resources --json` CLI subcommand

**Files:**
- Modify: `src/frost/cli.py`
- Add to: `tests/test_resources.py`

- [ ] **Step 1: Write the failing CLI integration test**

Append to `tests/test_resources.py`:

```python
class TestCmdResources:
    """CLI integration test for `frost resources --json`."""

    def test_resources_json_requires_connection(self):
        """Running without valid creds should produce a JSON error."""
        import subprocess, json

        result = subprocess.run(
            [
                "python", "-m", "frost",
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_resources.py::TestCmdResources -v`
Expected: FAIL — `frost resources` is not a recognised subcommand yet.

- [ ] **Step 3: Add the `resources` subcommand to the parser**

In `src/frost/cli.py`, find the `streamlit` parser block (around line 888–925). **After** the entire streamlit block (after line 925), add:

```python
    # resources
    resources_parser = sub.add_parser(
        "resources",
        help="List live Snowflake resources in the configured database",
    )
    resources_parser.add_argument(
        "--json",
        action="store_true",
        help="Output resources as JSON (required)",
    )
```

- [ ] **Step 4: Add `_cmd_resources` handler**

In `src/frost/cli.py`, add the handler. Find the `_cmd_streamlit` function. **After** the entire `_cmd_streamlit` function (typically the last command handler before `_build_parser`), add:

```python
def _cmd_resources(config, args):
    """List all Snowflake resources in the configured database as JSON."""
    from frost.connector import SnowflakeConnector, ConnectionConfig
    from frost.resources import fetch_resources

    if not getattr(args, "json", False):
        print("Error: frost resources requires --json", file=sys.stderr)
        sys.exit(1)

    conn_config = ConnectionConfig(
        account=config.account,
        user=config.user,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        private_key_path=config.private_key_path,
        private_key_passphrase=config.private_key_passphrase,
    )

    try:
        connector = SnowflakeConnector(conn_config)
        connector.connect()
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)

    try:
        result = fetch_resources(connector, config.database or "")
        print(json.dumps(result, indent=2, default=str))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)
    finally:
        connector.close()
```

- [ ] **Step 5: Wire the dispatch in `main()`**

In `src/frost/cli.py`, find the dispatch block (around line 68–84). After the `elif args.command == "streamlit":` line and its body, add:

```python
    elif args.command == "resources":
        _cmd_resources(config, args)
```

- [ ] **Step 6: Run the integration test**

Run: `pytest tests/test_resources.py -v`
Expected: All 7 tests PASS. The CLI test should now fail with exit 1 and produce `{"error": "..."}` (connection failure because `nonexistent-config.yml` has no valid creds).

- [ ] **Step 7: Run full test suite for regression**

Run: `pytest`
Expected: All tests pass (468+ from Phase 1 + 7 new).

- [ ] **Step 8: Commit**

```bash
git add src/frost/cli.py tests/test_resources.py
git commit -m "feat(cli): add frost resources --json subcommand"
```

---

## Task 3: Shared `ConfigReader` for the extension

**Files:**
- Create: `vscode-frost/src/configReader.ts`

- [ ] **Step 1: Create `configReader.ts`**

Create `vscode-frost/src/configReader.ts`:

```typescript
/**
 * ConfigReader — parses frost-config.yml and caches the result.
 *
 * Consumed by ConfigTree (full display), DeployTree (target header),
 * and ResourcesTree (root label). Uses a lightweight regex-based parser
 * to avoid adding a js-yaml dependency.
 */

import * as vscode from "vscode";
import * as fs from "fs";
import * as path from "path";

export interface FrostConfigData {
  // Project
  objectsFolder: string;
  dataFolder: string;
  dataSchema: string;
  // Connection
  account: string;
  user: string;
  role: string;
  warehouse: string;
  database: string;
  // Authentication
  privateKeyPath: string;
  hasPassphrase: boolean;
  // Deploy tracking
  trackingSchema: string;
  trackingTable: string;
}

const DEFAULTS: FrostConfigData = {
  objectsFolder: "objects",
  dataFolder: "data",
  dataSchema: "PUBLIC",
  account: "",
  user: "",
  role: "SYSADMIN",
  warehouse: "COMPUTE_WH",
  database: "",
  privateKeyPath: "",
  hasPassphrase: false,
  trackingSchema: "FROST",
  trackingTable: "DEPLOY_HISTORY",
};

/** Map from YAML key (kebab-case) → FrostConfigData field name. */
const KEY_MAP: Record<string, keyof FrostConfigData> = {
  "objects-folder": "objectsFolder",
  "objects_folder": "objectsFolder",
  "data-folder": "dataFolder",
  "data_folder": "dataFolder",
  "data-schema": "dataSchema",
  "data_schema": "dataSchema",
  "account": "account",
  "user": "user",
  "role": "role",
  "warehouse": "warehouse",
  "database": "database",
  "private-key-path": "privateKeyPath",
  "private_key_path": "privateKeyPath",
  "private-key-passphrase": "hasPassphrase",
  "private_key_passphrase": "hasPassphrase",
  "tracking-schema": "trackingSchema",
  "tracking_schema": "trackingSchema",
  "tracking-table": "trackingTable",
  "tracking_table": "trackingTable",
};

/** Env-var overrides for each config field (shown as hints for null values). */
const ENV_HINTS: Partial<Record<keyof FrostConfigData, string>> = {
  account: "SNOWFLAKE_ACCOUNT",
  user: "SNOWFLAKE_USER",
  role: "SNOWFLAKE_ROLE",
  warehouse: "SNOWFLAKE_WAREHOUSE",
  database: "SNOWFLAKE_DATABASE",
  privateKeyPath: "SNOWFLAKE_PRIVATE_KEY_PATH",
};

export class ConfigReader {
  private _data: FrostConfigData | undefined;
  private _error: string | undefined;

  constructor(private readonly cwd: string) {}

  /** Resolve the config file path using the frost.configPath setting. */
  get configPath(): string {
    const name = vscode.workspace
      .getConfiguration("frost")
      .get<string>("configPath", "frost-config.yml");
    return path.resolve(this.cwd, name);
  }

  /** Returns cached config or re-reads from disk. */
  get data(): FrostConfigData | undefined {
    if (!this._data) {
      this.reload();
    }
    return this._data;
  }

  /** Returns the parse error, if any. */
  get error(): string | undefined {
    if (!this._data && !this._error) {
      this.reload();
    }
    return this._error;
  }

  /** Force re-read from disk. */
  reload(): void {
    this._data = undefined;
    this._error = undefined;

    const cfgPath = this.configPath;
    if (!fs.existsSync(cfgPath)) {
      this._error = "frost-config.yml not found";
      return;
    }

    try {
      const content = fs.readFileSync(cfgPath, "utf-8");
      this._data = this.parse(content);
    } catch (err: any) {
      this._error = `Could not parse config: ${err.message}`;
    }
  }

  /** Invalidate the cache so the next read re-parses. */
  invalidate(): void {
    this._data = undefined;
    this._error = undefined;
  }

  /** Get the env-var hint for a config field, if any. */
  envHint(field: keyof FrostConfigData): string | undefined {
    return ENV_HINTS[field];
  }

  // ── parsing ──────────────────────────────────────────

  private parse(content: string): FrostConfigData {
    const data: FrostConfigData = { ...DEFAULTS };
    const lines = content.split("\n");

    for (const line of lines) {
      // Skip comments and blank lines
      if (/^\s*#/.test(line) || /^\s*$/.test(line)) {
        continue;
      }
      // Only top-level keys (no leading whitespace)
      const match = line.match(/^([a-z][a-z0-9_-]*)\s*:\s*(.*)$/i);
      if (!match) {
        continue;
      }
      const yamlKey = match[1].toLowerCase();
      const rawValue = match[2].trim();
      const field = KEY_MAP[yamlKey];
      if (!field) {
        continue;
      }

      // Strip quotes
      let value = rawValue;
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }

      // Handle null
      if (value === "null" || value === "~" || value === "") {
        continue; // keep default
      }

      // Special case: passphrase is a boolean flag
      if (field === "hasPassphrase") {
        data.hasPassphrase = true;
        continue;
      }

      (data as any)[field] = value;
    }

    return data;
  }
}
```

- [ ] **Step 2: Compile**

Run: `cd vscode-frost && npm run compile`
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add vscode-frost/src/configReader.ts
git commit -m "feat(vscode): add shared ConfigReader for frost-config.yml"
```

---

## Task 4: Configuration panel tree provider

**Files:**
- Create: `vscode-frost/src/configTree.ts`

- [ ] **Step 1: Create `configTree.ts`**

Create `vscode-frost/src/configTree.ts`:

```typescript
/**
 * FrostConfigProvider — read-only sidebar tree showing frost-config.yml
 * at a glance: project folders, connection details, auth, tracking.
 */

import * as vscode from "vscode";
import { ConfigReader, FrostConfigData } from "./configReader";

type TreeItem = ConfigGroupItem | ConfigValueItem | ConfigErrorItem;

class ConfigGroupItem extends vscode.TreeItem {
  constructor(
    public readonly groupLabel: string,
    public readonly children: ConfigValueItem[],
    icon: string,
  ) {
    super(groupLabel, vscode.TreeItemCollapsibleState.Expanded);
    this.iconPath = new vscode.ThemeIcon(icon);
    this.contextValue = "frostConfigGroup";
  }
}

class ConfigValueItem extends vscode.TreeItem {
  constructor(label: string, value: string, envHint?: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
    if (value) {
      this.description = value;
    } else if (envHint) {
      this.description = `(env: ${envHint})`;
    } else {
      this.description = "(not set)";
    }
    this.iconPath = new vscode.ThemeIcon("symbol-property");
    this.contextValue = "frostConfigValue";
  }
}

class ConfigErrorItem extends vscode.TreeItem {
  constructor(message: string, configPath: string) {
    super(message, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon("warning");
    this.command = {
      command: "frost.openConfig",
      title: "Open frost-config.yml",
      arguments: [configPath],
    };
    this.contextValue = "frostConfigError";
  }
}

export class FrostConfigProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private tree: TreeItem[] = [];

  constructor(private readonly configReader: ConfigReader) {
    this.buildTree();
  }

  refresh(): void {
    this.configReader.reload();
    this.buildTree();
  }

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(el?: TreeItem): TreeItem[] {
    if (!el) {
      return this.tree;
    }
    if (el instanceof ConfigGroupItem) {
      return el.children;
    }
    return [];
  }

  private buildTree(): void {
    const data = this.configReader.data;

    if (!data) {
      const errMsg = this.configReader.error ?? "frost-config.yml not found";
      this.tree = [new ConfigErrorItem(errMsg, this.configReader.configPath)];
      this._onDidChangeTreeData.fire();
      return;
    }

    const hint = (f: keyof FrostConfigData) => this.configReader.envHint(f);

    this.tree = [
      new ConfigGroupItem("Project", [
        new ConfigValueItem("Objects folder", data.objectsFolder),
        new ConfigValueItem("Data folder", data.dataFolder),
        new ConfigValueItem("Data schema", data.dataSchema),
      ], "folder"),

      new ConfigGroupItem("Connection", [
        new ConfigValueItem("Account", data.account, hint("account")),
        new ConfigValueItem("User", data.user, hint("user")),
        new ConfigValueItem("Role", data.role, hint("role")),
        new ConfigValueItem("Warehouse", data.warehouse, hint("warehouse")),
        new ConfigValueItem("Database", data.database, hint("database")),
      ], "plug"),

      new ConfigGroupItem("Authentication", [
        new ConfigValueItem("Type", "RSA key-pair"),
        new ConfigValueItem("Key path", data.privateKeyPath, hint("privateKeyPath")),
        new ConfigValueItem("Passphrase", data.hasPassphrase ? "set" : "not set"),
      ], "key"),

      new ConfigGroupItem("Deploy tracking", [
        new ConfigValueItem(
          "History table",
          `${data.trackingSchema}.${data.trackingTable}`,
        ),
      ], "history"),
    ];

    this._onDidChangeTreeData.fire();
  }
}
```

- [ ] **Step 2: Compile**

Run: `cd vscode-frost && npm run compile`
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add vscode-frost/src/configTree.ts
git commit -m "feat(vscode): add Configuration panel tree provider"
```

---

## Task 5: Add `ResourcesPayload` and `resourcesJson()` to `frostRunner.ts`

**Files:**
- Modify: `vscode-frost/src/frostRunner.ts`

- [ ] **Step 1: Add the types**

In `vscode-frost/src/frostRunner.ts`, find the `SubgraphPayload` interface (around line 90–97). **After** the closing brace of `SubgraphPayload`, add:

```typescript

/** A single Snowflake resource from `frost resources --json`. */
export interface ResourceItem {
  schema: string;
  type: string;
  name: string;
  fqn: string;
  created_on: string;
  owner: string;
  comment: string;
}

/** Response from `frost resources --json`. */
export interface ResourcesPayload {
  database: string;
  resources: ResourceItem[];
  warnings?: string[];
}
```

- [ ] **Step 2: Add the `resourcesJson()` method**

In `vscode-frost/src/frostRunner.ts`, find the `async lineageFullJson()` method and the `execLineageJson` private helper that follows it. **After** the closing brace of `execLineageJson` (the last method before `async loadJson()`), add:

```typescript

  /** List live Snowflake resources as JSON. */
  async resourcesJson(): Promise<ResourcesPayload> {
    const raw = await this.exec("resources --json");
    return JSON.parse(this.extractJson(raw)) as ResourcesPayload;
  }
```

- [ ] **Step 3: Compile**

Run: `cd vscode-frost && npm run compile`
Expected: No errors.

- [ ] **Step 4: Commit**

```bash
git add vscode-frost/src/frostRunner.ts
git commit -m "feat(vscode): add ResourcesPayload type and resourcesJson() helper"
```

---

## Task 6: Resource Explorer tree provider

**Files:**
- Create: `vscode-frost/src/resourcesTree.ts`

- [ ] **Step 1: Create `resourcesTree.ts`**

Create `vscode-frost/src/resourcesTree.ts`:

```typescript
/**
 * FrostResourcesProvider — sidebar tree listing live Snowflake resources
 * in the configured database, grouped by Schema → Type → Resource.
 *
 * Loads on-demand (first panel expand), not on activation.
 */

import * as vscode from "vscode";
import { FrostRunner, ResourceItem, ResourcesPayload } from "./frostRunner";
import { ConfigReader } from "./configReader";
import { FrostObjectsProvider } from "./objectsTree";

// ── Tree items ────────────────────────────────────────────

type TreeItem = SchemaItem | TypeGroupItem | ResourceTreeItem | PlaceholderItem | WarningGroupItem | WarningItem;

class SchemaItem extends vscode.TreeItem {
  constructor(
    public readonly schemaName: string,
    public readonly children: TypeGroupItem[],
  ) {
    super(schemaName, vscode.TreeItemCollapsibleState.Collapsed);
    this.iconPath = new vscode.ThemeIcon("database");
    this.contextValue = "frostResourceSchema";
  }
}

class TypeGroupItem extends vscode.TreeItem {
  constructor(
    public readonly typeName: string,
    public readonly children: ResourceTreeItem[],
  ) {
    super(typeName, vscode.TreeItemCollapsibleState.Collapsed);
    this.description = `${children.length}`;
    this.iconPath = new vscode.ThemeIcon("symbol-class");
    this.contextValue = "frostResourceTypeGroup";
  }
}

class ResourceTreeItem extends vscode.TreeItem {
  public readonly fqn: string;
  public readonly resourceType: string;

  constructor(resource: ResourceItem, private readonly isLocal: boolean) {
    super(resource.name, vscode.TreeItemCollapsibleState.None);
    this.fqn = resource.fqn;
    this.resourceType = resource.type;
    this.description = resource.owner;
    this.tooltip = new vscode.MarkdownString(
      `**${resource.fqn}**  \nType: ${resource.type}  \nOwner: ${resource.owner}` +
      (resource.comment ? `  \nComment: ${resource.comment}` : "") +
      (resource.created_on ? `  \nCreated: ${resource.created_on}` : ""),
    );
    this.iconPath = new vscode.ThemeIcon(
      isLocal ? "symbol-method" : "symbol-reference",
    );
    this.contextValue = isLocal ? "frostResourceLocal" : "frostResourceRemote";
  }
}

class PlaceholderItem extends vscode.TreeItem {
  constructor(label: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
  }
}

class WarningGroupItem extends vscode.TreeItem {
  constructor(public readonly children: WarningItem[]) {
    super("Warnings", vscode.TreeItemCollapsibleState.Collapsed);
    this.description = `${children.length}`;
    this.iconPath = new vscode.ThemeIcon("warning");
  }
}

class WarningItem extends vscode.TreeItem {
  constructor(message: string) {
    super(message, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon("info");
  }
}

// ── Provider ──────────────────────────────────────────────

export class FrostResourcesProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private tree: TreeItem[] = [];
  private _loaded = false;
  private _loading = false;

  constructor(
    private readonly runner: FrostRunner,
    private readonly configReader: ConfigReader,
    private readonly objectsProvider: FrostObjectsProvider,
  ) {}

  refresh(): void {
    this._loaded = false;
    this.loadResources();
  }

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(el?: TreeItem): TreeItem[] {
    if (!el) {
      // Lazy-init: trigger load on first expand
      if (!this._loaded && !this._loading) {
        this.loadResources();
      }
      return this.tree;
    }
    if (el instanceof SchemaItem) {
      return el.children;
    }
    if (el instanceof TypeGroupItem) {
      return el.children;
    }
    if (el instanceof WarningGroupItem) {
      return el.children;
    }
    return [];
  }

  /** Look up whether a resource FQN exists in the local Objects tree. */
  isLocalFqn(fqn: string): boolean {
    const localFqns = this.objectsProvider.getAllFqns();
    const upper = fqn.toUpperCase();
    return localFqns.some((f) => f.toUpperCase() === upper);
  }

  // ── internal ────────────────────────────────────────────

  private async loadResources(): Promise<void> {
    if (this._loading) {
      return;
    }
    this._loading = true;
    this._loaded = true;

    this.tree = [new PlaceholderItem("Loading resources...")];
    this._onDidChangeTreeData.fire();

    try {
      const payload = await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Window,
          title: "Frost: loading resources...",
        },
        () => this.runner.resourcesJson(),
      );

      this.tree = this.buildTree(payload);
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Frost: could not load resources – ${err.message}`,
      );
      this.tree = [new PlaceholderItem("Could not connect to Snowflake")];
    } finally {
      this._loading = false;
    }
    this._onDidChangeTreeData.fire();
  }

  private buildTree(payload: ResourcesPayload): TreeItem[] {
    const localFqns = new Set(
      this.objectsProvider.getAllFqns().map((f) => f.toUpperCase()),
    );

    // Group: schema → type → resources
    const bySchema = new Map<string, Map<string, ResourceTreeItem[]>>();

    for (const r of payload.resources) {
      if (!bySchema.has(r.schema)) {
        bySchema.set(r.schema, new Map());
      }
      const byType = bySchema.get(r.schema)!;
      // Pluralise the type for display
      const displayType = pluralise(r.type);
      if (!byType.has(displayType)) {
        byType.set(displayType, []);
      }
      const isLocal = localFqns.has(r.fqn.toUpperCase());
      byType.get(displayType)!.push(new ResourceTreeItem(r, isLocal));
    }

    const items: TreeItem[] = [];

    // Sort schemas alphabetically
    const sortedSchemas = Array.from(bySchema.keys()).sort();
    for (const schema of sortedSchemas) {
      const byType = bySchema.get(schema)!;
      const typeGroups: TypeGroupItem[] = [];
      const sortedTypes = Array.from(byType.keys()).sort();
      for (const typeName of sortedTypes) {
        const resources = byType.get(typeName)!;
        resources.sort((a, b) => a.label!.toString().localeCompare(b.label!.toString()));
        typeGroups.push(new TypeGroupItem(typeName, resources));
      }
      items.push(new SchemaItem(schema, typeGroups));
    }

    // Append warnings group if any
    if (payload.warnings && payload.warnings.length > 0) {
      const warningItems = payload.warnings.map((w) => new WarningItem(w));
      items.push(new WarningGroupItem(warningItems));
    }

    if (items.length === 0) {
      return [new PlaceholderItem("No resources found")];
    }

    return items;
  }
}

/** Simple pluraliser for resource type labels. */
function pluralise(type: string): string {
  const lower = type.toLowerCase();
  if (lower.endsWith("s")) {
    return type + "es";
  }
  if (lower.endsWith("y")) {
    return type.slice(0, -1) + "ies";
  }
  return type + "s";
}
```

- [ ] **Step 2: Compile**

Run: `cd vscode-frost && npm run compile`
Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add vscode-frost/src/resourcesTree.ts
git commit -m "feat(vscode): add Resource Explorer tree provider"
```

---

## Task 7: Update `deployTree.ts` — add target header

**Files:**
- Modify: `vscode-frost/src/deployTree.ts`

- [ ] **Step 1: Rewrite `deployTree.ts`**

Replace the entire contents of `vscode-frost/src/deployTree.ts` with:

```typescript
/**
 * FrostDeployProvider – shows deploy target + quick-access action buttons.
 */

import * as vscode from "vscode";
import { ConfigReader } from "./configReader";

type TreeItem = TargetItem | ActionItem;

class TargetItem extends vscode.TreeItem {
  constructor(database: string, role: string) {
    const label = database
      ? `Target: ${database} (${role})`
      : "Target: not configured";
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon("database");
    this.description = "";
    this.contextValue = "frostDeployTarget";
  }
}

class ActionItem extends vscode.TreeItem {
  constructor(
    label: string,
    icon: string,
    private readonly cmdId: string,
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
    this.command = { command: cmdId, title: label };
  }
}

export class FrostDeployProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(private readonly configReader: ConfigReader) {}

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(): TreeItem[] {
    const data = this.configReader.data;
    const database = data?.database ?? "";
    const role = data?.role ?? "SYSADMIN";

    return [
      new TargetItem(database, role),
      new ActionItem("Plan", "list-ordered", "frost.plan"),
      new ActionItem("Deploy", "cloud-upload", "frost.deploy"),
      new ActionItem("Deploy (Force)", "sync", "frost.deployForce"),
      new ActionItem("Lineage (remote)", "type-hierarchy", "frost.lineage"),
      new ActionItem("Lineage (local)", "type-hierarchy-sub", "frost.lineageLocal"),
    ];
  }
}
```

- [ ] **Step 2: Compile**

Run: `cd vscode-frost && npm run compile`
Expected: Compile error — `FrostDeployProvider` constructor now requires a `ConfigReader`, but `extension.ts` still calls `new FrostDeployProvider()` with no args. This is expected; Task 10 wires it up.

Note the error and proceed; it will be resolved in Task 10.

- [ ] **Step 3: Commit**

```bash
git add vscode-frost/src/deployTree.ts
git commit -m "feat(vscode): add deploy target header from config reader"
```

---

## Task 8: Add `pickObject` message to LineagePanel + webview

**Files:**
- Modify: `vscode-frost/src/lineagePanel.ts`
- Modify: `vscode-frost/media/lineage/lineage.js`

- [ ] **Step 1: Add `pickObject` case to `LineagePanel.onMessage` and a static `showWithFqn` method**

In `vscode-frost/src/lineagePanel.ts`, find the `static show(` method. **After** it (after the closing brace of `show`, around line 59), add:

```typescript

  /**
   * Open the lineage panel and pre-select a specific object.
   * Used by the Resource Explorer's "Show Lineage" action.
   */
  static showWithFqn(
    extensionUri: vscode.Uri,
    runner: FrostRunner,
    objectsProvider: FrostObjectsProvider,
    fqn: string,
  ): void {
    LineagePanel.show(extensionUri, runner, objectsProvider);
    // The panel may not have received "ready" yet; queue the pick.
    if (LineagePanel.currentPanel) {
      LineagePanel.currentPanel.panel.webview.postMessage({
        type: "pickObject",
        fqn,
      });
    }
  }
```

- [ ] **Step 2: Handle `pickObject` in `lineage.js`**

In `vscode-frost/media/lineage/lineage.js`, find the `window.addEventListener("message"` handler. Inside the event handler function body, find the switch/if-else block that handles `"objectList"`, `"subgraph"`, and `"error"` message types. Add a new case for `"pickObject"`.

Find this section (around the end of the file):

```javascript
  window.addEventListener("message", (event) => {
    const msg = event.data;
```

Inside the handler, after the existing cases (after the `else if (msg.type === "error")` block), add:

```javascript
    else if (msg.type === "pickObject") {
      searchInput.value = msg.fqn;
      pickObject(msg.fqn);
    }
```

- [ ] **Step 3: Compile and verify**

Run: `cd vscode-frost && npm run compile`
Expected: No errors (TypeScript compilation does not cover `.js` files).

- [ ] **Step 4: Commit**

```bash
git add vscode-frost/src/lineagePanel.ts vscode-frost/media/lineage/lineage.js
git commit -m "feat(vscode): add pickObject message for pre-selecting lineage target"
```

---

## Task 9: Update `package.json` — new views, commands, menus

**Files:**
- Modify: `vscode-frost/package.json`

- [ ] **Step 1: Add the two new views to the sidebar**

In `vscode-frost/package.json`, find the `"views"` → `"frost-explorer"` array (lines 46–67). Replace the entire array with the new order:

```json
      "frost-explorer": [
        {
          "id": "frostConfig",
          "name": "Configuration"
        },
        {
          "id": "frostObjects",
          "name": "Objects"
        },
        {
          "id": "frostResources",
          "name": "Resources"
        },
        {
          "id": "frostData",
          "name": "Data"
        },
        {
          "id": "frostVariables",
          "name": "Variables"
        },
        {
          "id": "frostStreamlit",
          "name": "Streamlit"
        },
        {
          "id": "frostDeployHistory",
          "name": "Deploy"
        }
      ]
```

- [ ] **Step 2: Add new commands**

In the `"commands"` array, after the last existing command entry (the `frost.openStreamlitUrl` command, around line 173), add:

```json
      ,{
        "command": "frost.openConfig",
        "title": "Open frost-config.yml",
        "icon": "$(gear)"
      },
      {
        "command": "frost.refreshConfig",
        "title": "Frost: Refresh Configuration",
        "icon": "$(refresh)"
      },
      {
        "command": "frost.refreshResources",
        "title": "Frost: Refresh Resources",
        "icon": "$(refresh)"
      },
      {
        "command": "frost.showResourceLineage",
        "title": "Show Lineage",
        "icon": "$(type-hierarchy-sub)"
      },
      {
        "command": "frost.openResourceFile",
        "title": "Open SQL File",
        "icon": "$(go-to-file)"
      }
```

- [ ] **Step 3: Add view/title menus for the new panels**

In the `"menus"` → `"view/title"` array, after the last existing entry (the `frost.deployStreamlit` entry for `frostStreamlit`), add:

```json
        ,{
          "command": "frost.openConfig",
          "when": "view == frostConfig",
          "group": "navigation@1"
        },
        {
          "command": "frost.refreshConfig",
          "when": "view == frostConfig",
          "group": "navigation@2"
        },
        {
          "command": "frost.refreshResources",
          "when": "view == frostResources",
          "group": "navigation"
        }
```

- [ ] **Step 4: Add context menus for Resource items**

In the `"menus"` → `"view/item/context"` array, after the last existing entry (the `frost.teardownStreamlitApp` entry), add:

```json
        ,{
          "command": "frost.showResourceLineage",
          "when": "view == frostResources && viewItem =~ /^frostResource(Local|Remote)$/",
          "group": "frost@1"
        },
        {
          "command": "frost.openResourceFile",
          "when": "view == frostResources && viewItem == frostResourceLocal",
          "group": "frost@2"
        }
```

- [ ] **Step 5: Commit**

```bash
git add vscode-frost/package.json
git commit -m "feat(vscode): declare Configuration and Resources views in package.json"
```

---

## Task 10: Wire everything in `extension.ts`

**Files:**
- Modify: `vscode-frost/src/extension.ts`

- [ ] **Step 1: Add imports**

In `vscode-frost/src/extension.ts`, find the import block (lines 12–21). Add the new imports after the existing ones:

```typescript
import { ConfigReader } from "./configReader";
import { FrostConfigProvider } from "./configTree";
import { FrostResourcesProvider } from "./resourcesTree";
```

- [ ] **Step 2: Create new providers in `activate()`**

In the `activate()` function, find the line that creates the deploy provider (line 28):

```typescript
  const deployProvider = new FrostDeployProvider();
```

Replace it with:

```typescript
  const configReader = new ConfigReader(runner.cwd);
  const configProvider = new FrostConfigProvider(configReader);
  const deployProvider = new FrostDeployProvider(configReader);
```

Then, find the line that creates `streamlitProvider` (line 31):

```typescript
  const streamlitProvider = new FrostStreamlitProvider(runner);
```

**After** that line, add:

```typescript
  const resourcesProvider = new FrostResourcesProvider(runner, configReader, objectsProvider);
```

- [ ] **Step 3: Register the new tree views**

Find the tree view registration block. After the existing `objectsTree` registration (around line 35–39), add:

```typescript
  const configTree = vscode.window.createTreeView("frostConfig", {
    treeDataProvider: configProvider,
  });
  context.subscriptions.push(configTree);
```

Find the existing `deployTree` registration (around line 41–44). After it, add:

```typescript
  const resourcesTree = vscode.window.createTreeView("frostResources", {
    treeDataProvider: resourcesProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(resourcesTree);
```

- [ ] **Step 4: Register new commands**

Inside the `context.subscriptions.push(` block that registers all commands, after the last existing command registration (around line 295, before the closing `);`), add:

```typescript
    vscode.commands.registerCommand("frost.openConfig", () => {
      const configPath = configReader.configPath;
      const uri = vscode.Uri.file(configPath);
      vscode.window.showTextDocument(uri).then(undefined, () => {
        vscode.window.showWarningMessage(
          `Frost: could not open ${configPath}`,
        );
      });
    }),
    vscode.commands.registerCommand("frost.refreshConfig", () => {
      configProvider.refresh();
      deployProvider.refresh();
    }),
    vscode.commands.registerCommand("frost.refreshResources", () => {
      resourcesProvider.refresh();
    }),
    vscode.commands.registerCommand("frost.showResourceLineage", (item) => {
      const fqn = item?.fqn;
      if (!fqn) { return; }
      if (resourcesProvider.isLocalFqn(fqn)) {
        LineagePanel.showWithFqn(context.extensionUri, runner, objectsProvider, fqn);
      } else {
        // Fall back to remote lineage in browser (same as frost.lineage command)
        vscode.commands.executeCommand("frost.lineage");
      }
    }),
    vscode.commands.registerCommand("frost.openResourceFile", (item) => {
      const fqn = item?.fqn;
      if (fqn) {
        vscode.commands.executeCommand("frost.openFile", { fqn, filePath: undefined });
      }
    }),
```

- [ ] **Step 5: Wire config file watcher to new providers**

Find the existing config file watcher block (around line 344–348):

```typescript
    const configWatcher = vscode.workspace.createFileSystemWatcher("**/frost-config*.yml");
    configWatcher.onDidChange(() => variablesProvider.refresh());
    configWatcher.onDidCreate(() => variablesProvider.refresh());
    configWatcher.onDidDelete(() => variablesProvider.refresh());
    context.subscriptions.push(configWatcher);
```

Replace those three `.onDid*` lines with:

```typescript
    configWatcher.onDidChange(() => {
      configProvider.refresh();
      deployProvider.refresh();
      variablesProvider.refresh();
    });
    configWatcher.onDidCreate(() => {
      configProvider.refresh();
      deployProvider.refresh();
      variablesProvider.refresh();
    });
    configWatcher.onDidDelete(() => {
      configProvider.refresh();
      deployProvider.refresh();
      variablesProvider.refresh();
    });
```

- [ ] **Step 6: Compile**

Run: `cd vscode-frost && npm run compile`
Expected: No errors. All providers wired, all commands registered.

- [ ] **Step 7: Commit**

```bash
git add vscode-frost/src/extension.ts
git commit -m "feat(vscode): wire Configuration, Resources, and Deploy providers"
```

---

## Task 11: Final compile, vsix rebuild, and README

**Files:**
- Modify: `vscode-frost/README.md`

- [ ] **Step 1: Run full Python test suite**

Run: `pytest`
Expected: All tests pass (468+ existing + 7 new resources tests).

- [ ] **Step 2: Final TypeScript compile**

Run: `cd vscode-frost && npm run compile`
Expected: Clean compile, no errors or warnings.

- [ ] **Step 3: Rebuild the vsix**

Run: `cd vscode-frost && npx @vscode/vsce package --no-dependencies`
Expected: `frost-snowflake-0.1.0.vsix` produced.

- [ ] **Step 4: Update README**

Append to the "Features" section of `vscode-frost/README.md`, after the existing "Diagnostics" block and before the "Data Loading" block:

```markdown
### Configuration
- Read-only view of `frost-config.yml` at the top of the sidebar
- Shows project folders, connection details, authentication, and deploy tracking
- Auto-refreshes when the config file changes on disk

### Resources
- Live view of Snowflake resources in the configured database
- Grouped by Schema → Type → Resource (tables, views, tasks, streams, pipes, etc.)
- Right-click a resource to show its lineage or open its SQL file
- Loads on-demand — no impact on activation time
```

- [ ] **Step 5: Commit**

```bash
git add vscode-frost/README.md vscode-frost/frost-snowflake-0.1.0.vsix
git commit -m "docs(vscode): update README and rebuild vsix with sidebar sections"
```

---

## Task 12: Final verification against spec success criteria

- [ ] **Step 1: Run full Python test suite**

Run: `pytest`
Expected: All tests pass.

- [ ] **Step 2: Compile the extension**

Run: `cd vscode-frost && npm run compile`
Expected: No TypeScript errors.

- [ ] **Step 3: Verify spec §8 success criteria**

Cross-check against `docs/superpowers/specs/2026-04-17-sidebar-sections-design.md` §8:

1. Configuration panel at top with 13 items from config → verified by ConfigTree implementation.
2. Config auto-refresh on file change → verified by watcher wiring in extension.ts.
3. Resources panel loads within 10 s → manual test with Snowflake connection.
4. Resources grouped Schema → Type → Resource with counts → verified by buildTree implementation.
5. Local resource "Show Lineage" → Phase 1 panel → verified by showResourceLineage command.
6. Remote resource → browser lineage → verified by else branch in showResourceLineage.
7. Deploy panel target header → verified by TargetItem in deployTree.
8. Activation time unaffected → Resources uses lazy-init, not activation load.
9. All tests pass → Step 1.

- [ ] **Step 4: Final commit if anything was fixed**

If no fixes needed, skip. Otherwise:

```bash
git add -u
git commit -m "fix: resolve issues found during final verification"
```
