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

### ⚠️ Diagnostics
- Self-dependency detection (inline error)
- Missing/external dependency warnings in the Problems panel

### 📊 Status Bar
- Shows a Frost icon; click to run `frost plan`

## Requirements

- Python 3.10+ with `frost-ddl` installed (`pip install frost-ddl`)
- A `frost-config.yml` in the workspace root
- SQL files in the configured `objects_folder`

## Extension Settings

| Setting            | Default              | Description                                      |
|--------------------|----------------------|--------------------------------------------------|
| `frost.pythonPath` | `python3`            | Python interpreter with frost-ddl installed       |
| `frost.configPath` | `frost-config.yml`   | Path to frost config file                         |
| `frost.autoRefresh`| `true`               | Refresh tree when SQL files change                |

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

## Packaging

```bash
cd vscode-frost
npm run package
# Produces frost-snowflake-0.1.0.vsix
```

Install the `.vsix` with:
```bash
code --install-extension frost-snowflake-0.1.0.vsix
```
