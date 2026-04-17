# Sidebar Sections — Configuration, Resource Explorer & Deploy Target

**Date:** 2026-04-17
**Status:** Design approved, ready for implementation plan

## 1. Problem Statement & Goals

### Problem

The Frost VSCode extension shows Objects, Data, Variables, Streamlit, and
Deploy panels — but a developer opening a workspace for the first time
cannot answer three basic questions without leaving VSCode:

1. **Where is frost pointed?** The target database, role, warehouse, and
   auth method are buried in `frost-config.yml`.
2. **What's actually deployed in Snowflake?** The Objects panel shows local
   SQL definitions, not what's live. Tasks, streams, pipes, materialized
   views, alerts, event tables, and sequences are invisible. Drift between
   the local project and the deployed state is undetectable.
3. **Am I about to deploy to the right place?** The Deploy panel's buttons
   have no indication of the target database or role.

### Goals

- A new **Configuration** panel at the top of the sidebar shows the parsed
  `frost-config.yml` at a glance: project folders, connection details, auth
  type, and tracking table. Read-only (v1).
- A new **Resources** panel lists live Snowflake objects in the configured
  database, grouped by Schema → Type → Resource. Per-resource lineage is
  one click away (Phase 1 focused subgraph for local resources; existing
  remote lineage for non-local).
- The **Deploy** panel gains a "Target: DATABASE (ROLE)" header so you never
  accidentally deploy to the wrong place.
- No impact on activation time — Resources loads on-demand (first panel
  expand), not on activation.
- No new authentication modes, no multi-database scanning, no config
  editing from the panel.

### Non-Goals

- Editing `frost-config.yml` from the Configuration panel (users edit the
  YAML directly; a form-based editor is a separate feature).
- New authentication modes (only RSA key-pair today; displaying the type
  is in scope, adding OAuth/SSO/password is not).
- Multi-database resource scanning (v1 shows only the database from config).
- Deep resource actions beyond "Show Lineage" and "Open SQL File".
- Column-level lineage.
- Changes to the existing Objects, Data, Variables, or Streamlit panels.

## 2. Architecture

Three coordinated, independently testable changes plus a shared utility.

### 2a. Configuration Panel

A new `TreeDataProvider` that reads `frost-config.yml` and renders a
structured, read-only view of the project's configuration.

**Data source:** Pure local file read — no Snowflake connection required.

**Tree structure — 4 groups, 13 items:**

```
Configuration
├── Project
│   ├── Objects folder:  objects/
│   ├── Data folder:     data/
│   └── Data schema:     PUBLIC
├── Connection
│   ├── Account:         <value>
│   ├── User:            <value>
│   ├── Role:            SYSADMIN
│   ├── Warehouse:       COMPUTE_WH
│   └── Database:        MY_DB
├── Authentication
│   ├── Type:            RSA key-pair
│   ├── Key path:        ~/.ssh/snowflake_key.p8
│   └── Passphrase:      ✓ set  (or "not set")
└── Deploy tracking
    └── History table:   FROST.DEPLOY_HISTORY
```

**Refresh behaviour:**
- Loads on activation (cheap — no network call).
- Auto-refreshes when `frost-config.yml` changes on disk via the existing
  FileSystemWatcher.
- Manual "Refresh" button in view title bar.

**Actions:**
- View-title: **Open frost-config.yml** (`$(gear)` icon) — opens the file
  in an editor tab.
- View-title: **Refresh** (`$(refresh)` icon).

**Edge cases:**
- Config file missing or unparseable → single item:
  "⚠ frost-config.yml not found — click to create".
- Null/unset values where an env-var override exists → show
  "(env: `SNOWFLAKE_DATABASE`)" hint.

### 2b. Resource Explorer

A new `TreeDataProvider` that queries live Snowflake for what's actually
deployed in the configured database.

#### Python CLI — new `frost resources --json` command

Connects via the existing `SnowflakeConnector`, discovers schemas
(`SHOW SCHEMAS IN DATABASE`, skipping `INFORMATION_SCHEMA`), and fans out
`SHOW` queries per schema for the following types:

| Type | Query |
|---|---|
| TABLE | `SHOW TABLES IN SCHEMA {schema}` |
| VIEW | `SHOW VIEWS IN SCHEMA {schema}` |
| PROCEDURE | `SHOW PROCEDURES IN SCHEMA {schema}` |
| FUNCTION | `SHOW USER FUNCTIONS IN SCHEMA {schema}` |
| DYNAMIC TABLE | `SHOW DYNAMIC TABLES IN SCHEMA {schema}` |
| FILE FORMAT | `SHOW FILE FORMATS IN SCHEMA {schema}` |
| STAGE | `SHOW STAGES IN SCHEMA {schema}` |
| TASK | `SHOW TASKS IN SCHEMA {schema}` |
| TAG | `SHOW TAGS IN SCHEMA {schema}` |
| STREAM | `SHOW STREAMS IN SCHEMA {schema}` |
| PIPE | `SHOW PIPES IN SCHEMA {schema}` |
| MATERIALIZED VIEW | `SHOW MATERIALIZED VIEWS IN SCHEMA {schema}` |
| ALERT | `SHOW ALERTS IN SCHEMA {schema}` |
| EVENT TABLE | `SHOW EVENT TABLES IN SCHEMA {schema}` |
| SEQUENCE | `SHOW SEQUENCES IN SCHEMA {schema}` |

**Output JSON schema:**

```json
{
  "database": "MY_DB",
  "resources": [
    {
      "schema": "PUBLIC",
      "type": "TASK",
      "name": "DAILY_LOAD",
      "fqn": "PUBLIC.DAILY_LOAD",
      "created_on": "2025-11-01T...",
      "owner": "SYSADMIN",
      "comment": "Runs the daily ETL"
    }
  ],
  "warnings": [
    "Could not list ALERT in schema PUBLIC: insufficient privileges"
  ]
}
```

Flat `resources` list — the TypeScript side groups for the tree. Every
resource gets `fqn` = `SCHEMA.NAME` for matching against the local graph.

**Error handling:**
- Connection failure → exit 1, `{"error": "..."}`. Panel shows error banner.
- Individual `SHOW` failures (insufficient privileges) → logged, skipped.
  Other types still returned. A `"warnings"` array lists what failed.

**Implementation:** New file `src/frost/resources.py` (~80–100 lines) with
`fetch_resources(connector, database) -> dict`. `cli.py` gets a new
`_cmd_resources` handler + argparse subcommand.

#### Extension — Tree Provider

New `vscode-frost/src/resourcesTree.ts` (~200 lines). Follows the Objects
tree pattern exactly:

```
Resources
├── PUBLIC
│   ├── Tasks (3)
│   │   ├── DAILY_LOAD
│   │   ├── HOURLY_SYNC
│   │   └── CLEANUP_JOB
│   ├── Streams (1)
│   │   └── ORDERS_STREAM
│   └── Tables (12)
│       ├── ORDERS
│       └── ...
├── ANALYTICS
│   ├── Materialized Views (2)
│   │   └── ...
```

**Loading behaviour:**
- Does NOT load on extension activation. No activation event registered
  for this view.
- Loads on first panel expand (lazy-init inside `getChildren()`).
- "Loading..." placeholder via `withProgress`.
- Manual refresh button in view title bar.

#### Lineage integration

Each resource gets a context menu with:

1. **Show Lineage** — checks if the resource's FQN exists in the local
   Objects cache (`objectsProvider.getAllFqns()`):
   - **Yes** → opens the Phase 1 lineage panel with that FQN pre-selected
     (calls `LineagePanel.show()` then posts a `pickObject` message).
   - **No** → runs the existing remote lineage command (`frost.lineage`).

2. **Open SQL File** — only visible if the FQN exists locally. Opens the
   source file via the existing `frost.openFile` command.

No new lineage backend work. Phase 1 subgraph for local; existing
browser-based remote lineage for everything else.

### 2c. Deploy Panel — Target Header

The existing `FrostDeployProvider` gains a non-clickable header as its
first tree item:

```
Deploy
├── Target: MY_DB (SYSADMIN)    ← new, grey text, $(database) icon
├── Plan
├── Deploy
├── Deploy (Force)
├── Lineage (remote)
└── Lineage (local)
```

Database and role come from the shared config reader (same source as
the Configuration panel — no duplicate YAML parsing).

### 2d. Shared Config Reader

Both the Configuration panel and Deploy subtitle need parsed config.
Rather than duplicate YAML reads:

- New `vscode-frost/src/configReader.ts` (~60 lines).
- Reads + parses `frost-config.yml`, caches the result.
- Exposes typed getters: `getDatabase()`, `getRole()`,
  `getObjectsFolder()`, etc.
- Invalidates on file-change events from the existing FileSystemWatcher.
- `configTree.ts`, `deployTree.ts`, and optionally `resourcesTree.ts`
  (for the tree root label) all consume this.

## 3. Sidebar Order

After this change:

```
Configuration    ← new
Objects          (existing, unchanged)
Resources        ← new
Data             (existing, unchanged)
Variables        (existing, unchanged)
Streamlit        (existing, unchanged)
Deploy           (existing, minor change)
```

Defined in `vscode-frost/package.json` → `contributes.views.frost-explorer`.

## 4. Components, Interfaces & Error Handling

### Python — CLI Contract

**`frost resources --json`:**

- Requires a valid `frost-config.yml` with connection credentials.
- Connects to Snowflake, runs `SHOW SCHEMAS`, then `SHOW <TYPE>` per
  schema.
- Output: see JSON schema in §2b.
- Exit 0 on success (even with warnings). Exit 1 on connection failure
  with `{"error": "..."}` on stdout.

### Extension — New Components

| Component | File | Responsibility |
|---|---|---|
| `ConfigReader` | `configReader.ts` | Parse + cache `frost-config.yml` |
| `FrostConfigProvider` | `configTree.ts` | Configuration panel tree |
| `FrostResourcesProvider` | `resourcesTree.ts` | Resources panel tree + lineage routing |
| `FrostDeployProvider` (modify) | `deployTree.ts` | Add target header |
| `FrostRunner` (modify) | `frostRunner.ts` | `resourcesJson()` helper |
| `LineagePanel` (modify) | `lineagePanel.ts` | Accept pre-selected FQN |
| Extension (modify) | `extension.ts` | Register providers, wire commands |

### Extension — Error Handling

- **Config missing:** Configuration panel shows a single actionable item.
  Resources panel shows "Configuration required — open frost-config.yml".
  Deploy shows target as "Not configured".
- **Connection failure on resource load:** Resources panel shows error
  banner inside the tree (same pattern as Objects panel). User can retry
  via Refresh.
- **Partial query failure:** Resources panel renders what succeeded. A
  collapsible "Warnings" group at the bottom lists types that couldn't
  be fetched.
- **Lineage for non-local resource:** Silent fallback to remote lineage
  in the browser. No error state.

### FrostRunner Additions

```typescript
interface ResourceItem {
  schema: string;
  type: string;
  name: string;
  fqn: string;
  created_on: string;
  owner: string;
  comment: string;
}

interface ResourcesPayload {
  database: string;
  resources: ResourceItem[];
  warnings?: string[];
}
```

Method: `resourcesJson(): Promise<ResourcesPayload>` — calls
`frost resources --json`.

### LineagePanel Addition

New host-to-webview message type `pickObject` (sent via
`panel.webview.postMessage` from the extension side):

```json
{ "type": "pickObject", "fqn": "PUBLIC.DAILY_LOAD" }
```

When received, the webview populates the search box with the given FQN
and auto-triggers the subgraph fetch — same as if the user had typed
and selected it manually. This enables Resources → "Show Lineage" to
open the panel with a pre-selected object.

## 5. Testing

### Python

- Unit tests for `fetch_resources()`: mock connector returning canned
  `SHOW` results, verify output shape.
- Test that individual query failures are caught and returned as
  `warnings`, not fatal.
- CLI integration test: `frost resources --json` against a mocked
  connection, verify JSON output shape.

### Extension

- `configReader.ts` — unit tests for YAML parsing, missing-file
  handling, null-value env-var hinting.
- `resourcesTree.ts` — verify tree building from a canned
  `ResourcesPayload`.
- Lineage integration — verify FQN-in-local-graph check routes to
  Phase 1 panel vs. remote lineage.

### Manual Verification (added to README)

1. Open extension on a workspace with `frost-config.yml` → Configuration
   panel shows all values.
2. Edit `frost-config.yml` → Configuration panel updates automatically.
3. Open Resources panel → "Loading..." appears, then Snowflake resources
   populate grouped by Schema → Type.
4. Right-click a locally-managed resource → "Show Lineage" opens focused
   subgraph.
5. Right-click a remote-only resource → "Show Lineage" opens
   browser-based lineage.
6. Deploy panel shows "Target: DATABASE (ROLE)" as first line.

## 6. Rollout & Compatibility

### Backwards Compatibility

- **CLI:** `frost resources --json` is additive. No existing commands
  change.
- **Extension:** New views are additive. Existing views unchanged.
  The Deploy panel gains one item at the top but all existing buttons
  remain in the same order.
- **Python library API:** `fetch_resources` is additive. No removals.
- **Packaging:** No new media assets. `configReader.ts` and tree
  providers compile to `out/` which is already in the vsix `files` list.

### Rollout

Single change-set. No feature flag. The Configuration panel loads on
activation (cheap), Resources loads on-demand (no activation cost).

## 7. New Files Summary

| File | Lines (est.) | Purpose |
|---|---|---|
| `src/frost/resources.py` | ~100 | `fetch_resources()` + query fanout |
| `src/frost/cli.py` (modify) | +30 | `_cmd_resources` + argparse wiring |
| `vscode-frost/src/configReader.ts` | ~60 | Shared YAML config reader + cache |
| `vscode-frost/src/configTree.ts` | ~120 | Configuration panel tree provider |
| `vscode-frost/src/resourcesTree.ts` | ~200 | Resource Explorer tree provider |
| `vscode-frost/src/deployTree.ts` (modify) | +15 | Add target subtitle |
| `vscode-frost/src/frostRunner.ts` (modify) | +15 | `resourcesJson()` helper |
| `vscode-frost/src/extension.ts` (modify) | +20 | Register providers, wire commands |
| `vscode-frost/src/lineagePanel.ts` (modify) | +10 | Accept pre-selected FQN |
| `vscode-frost/package.json` (modify) | +20 | New views, commands, menus |
| `tests/test_resources.py` | ~60 | Unit + integration tests |

## 8. Success Criteria

1. Opening the sidebar shows Configuration at the top with all 13 items
   populated from `frost-config.yml`.
2. Editing `frost-config.yml` auto-refreshes the Configuration panel.
3. Expanding the Resources panel loads live Snowflake objects within 10 s
   (connection + queries) for a typical database with < 50 schemas.
4. Resources are grouped Schema → Type → Resource with counts.
5. Right-clicking a local resource and choosing "Show Lineage" opens the
   Phase 1 focused subgraph panel.
6. Right-clicking a remote-only resource opens browser-based lineage.
7. Deploy panel shows "Target: DATABASE (ROLE)" as its first entry.
8. Extension activation time is unaffected (Resources does not load on
   activation).
9. All existing tests keep passing; new tests cover `fetch_resources`,
   `ConfigReader`, and the lineage routing logic.
