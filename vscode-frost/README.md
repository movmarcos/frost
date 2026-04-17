# Frost – VS Code Extension ❄️

Browse, plan, deploy and visualise Snowflake objects managed by [frost-ddl](https://github.com/movmarcos/frost).

## Features

### 🗂️ Object Explorer (Sidebar)
- Tree view grouped by **Schema → Type → Object**
- Shows column names + data types for tables and views
- Click any object to open its SQL file
- Right-click to deploy a specific target

### 🚀 Deploy Commands
- **Plan** – preview execution order in the terminal
- **Deploy** – deploy changes to Snowflake
- **Deploy (Force)** – redeploy all objects ignoring checksums
- **Deploy Target** – deploy a specific object and its dependents

### 🔗 Lineage Visualisation
- **Local lineage** – renders the interactive D3.js DAG inside VS Code
- **Remote lineage** – opens the full lineage from Snowflake in a browser

### Configuration
- Read-only view of `frost-config.yml` at the top of the sidebar
- Shows project folders, connection details, authentication, and deploy tracking
- Auto-refreshes when the config file changes on disk

### Resources
- Live view of Snowflake resources in the configured database
- Grouped by Schema → Type → Resource (tables, views, tasks, streams, pipes, etc.)
- Right-click a resource to show its lineage or open its SQL file
- Loads on-demand — no impact on activation time

### ⚠️ Diagnostics
- Self-dependency detection (inline error)
- Missing/external dependency warnings in the Problems panel

### � Data Loading
- **Data** tree view shows all CSV files with column counts and row counts
- Click any CSV to open it
- **Load Data** – push all CSVs to Snowflake via `frost load`
- **Load CSV…** – pick a CSV file from disk, copy it into your `data/` folder
- Auto-refreshes when CSV files are added/changed

### �📊 Status Bar
- Shows a Frost icon; click to run `frost plan`

## Requirements

- Python 3.10+ with `frost-ddl` installed
- A `frost-config.yml` in the workspace (or a subdirectory)
- SQL files in the configured `objects_folder`

## Quick Install

### Option A – Pre-built `.vsix` (fastest)

```bash
code --install-extension vscode-frost/frost-snowflake-0.1.0.vsix
```

### Option B – Build from source

```bash
cd vscode-frost
npm install
npm run compile
npx @vscode/vsce package --no-dependencies
code --install-extension frost-snowflake-0.1.0.vsix
```

### Install `frost-ddl` (required)

From the repo root:

```bash
pip install -e .
```

## Extension Settings

| Setting            | Default              | Description                                      |
|--------------------|----------------------|--------------------------------------------------|
| `frost.pythonPath` | `python3`            | Python interpreter with frost-ddl installed       |
| `frost.configPath` | `frost-config.yml`   | Path to frost config file                         |
| `frost.autoRefresh`| `true`               | Refresh trees when SQL/CSV files change           |

## Getting Started

1. Install the extension
2. Open a workspace that contains `frost-config.yml`
3. The Frost sidebar icon (❄️) appears in the activity bar
4. Objects load automatically from your local SQL files

## Building from Source

```bash
cd vscode-frost
npm install
npm run compile
# Press F5 in VS Code to launch Extension Development Host
```

## Manual Verification — Large Projects

Use these steps to verify the Phase 1 lineage panel on a 1700-object
workspace. All steps must pass.

1. Install the freshly built vsix:
   `code --install-extension vscode-frost/frost-snowflake-0.1.0.vsix`
2. Open a workspace with ≥ 1000 managed objects. Confirm activation
   completes (Objects tree appears, even if still "Loading…") within a
   few seconds and VSCode does not crash.
3. Once the Objects tree has populated, run the command
   **Frost: Lineage (local)**. Confirm the panel opens with the
   search/picker visible in under one second.
4. Type a partial FQN into the picker. Confirm the dropdown suggests
   matching objects instantly (no subprocess call).
5. Click a suggestion. Confirm the subgraph renders within 5 s (cold
   scan) and that memory use in VSCode's process explorer stays below
   the level that previously caused the crash.
6. Move the depth slider and change the direction selector. Confirm the
   subgraph re-renders each time.
7. Click **Show full graph…**. Confirm a modal appears warning about
   the object count. Click **Continue** and observe either a successful
   full-graph render or a graceful error (no crash).
