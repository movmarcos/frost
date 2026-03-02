/**
 * Frost VS Code Extension – entry point.
 *
 * Activates when a workspace contains frost-config.yml and provides:
 *   • Sidebar tree listing all Snowflake objects grouped by schema/type
 *   • Commands: Plan, Deploy, Deploy (Force), Deploy Target, Lineage
 *   • Inline diagnostics from frost plan --json
 *   • Status bar showing the active database
 *   • Auto-refresh on SQL file changes
 */

import * as vscode from "vscode";
import { FrostObjectsProvider } from "./objectsTree";
import { FrostDeployProvider } from "./deployTree";
import { FrostDataProvider } from "./dataTree";
import { FrostVariablesProvider } from "./variablesTree";
import { FrostRunner } from "./frostRunner";
import { LineagePanel } from "./lineagePanel";
import { FrostDiagnostics } from "./diagnostics";

let statusBar: vscode.StatusBarItem;

export function activate(context: vscode.ExtensionContext): void {
  const runner = new FrostRunner();
  const objectsProvider = new FrostObjectsProvider(runner);
  const deployProvider = new FrostDeployProvider();
  const dataProvider = new FrostDataProvider(runner);
  const variablesProvider = new FrostVariablesProvider(runner);
  const diagnostics = new FrostDiagnostics(runner);

  // ── Tree views ──────────────────────────────────────────────
  const objectsTree = vscode.window.createTreeView("frostObjects", {
    treeDataProvider: objectsProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(objectsTree);

  const deployTree = vscode.window.createTreeView("frostDeployHistory", {
    treeDataProvider: deployProvider,
  });
  context.subscriptions.push(deployTree);

  const dataTree = vscode.window.createTreeView("frostData", {
    treeDataProvider: dataProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(dataTree);

  const variablesTree = vscode.window.createTreeView("frostVariables", {
    treeDataProvider: variablesProvider,
    showCollapseAll: true,
  });
  context.subscriptions.push(variablesTree);

  // ── Commands ────────────────────────────────────────────────
  context.subscriptions.push(
    vscode.commands.registerCommand("frost.refresh", () => {
      objectsProvider.refresh();
      diagnostics.run();
    }),
    vscode.commands.registerCommand("frost.plan", () =>
      runner.runInTerminal("plan")
    ),
    vscode.commands.registerCommand("frost.deploy", () =>
      runner.runInTerminal("deploy")
    ),
    vscode.commands.registerCommand("frost.deployForce", () =>
      runner.runInTerminal("deploy --force")
    ),
    vscode.commands.registerCommand("frost.deployTarget", async (item) => {
      const fqn =
        item?.fqn ??
        (await vscode.window.showInputBox({
          prompt: "Object FQN to deploy (e.g. PUBLIC.MY_VIEW)",
        }));
      if (fqn) {
        runner.runInTerminal(`deploy --target ${fqn}`);
      }
    }),
    vscode.commands.registerCommand("frost.lineage", () =>
      runner.runInTerminal("lineage")
    ),
    vscode.commands.registerCommand("frost.lineageLocal", () => {
      LineagePanel.show(context.extensionUri, runner);
    }),
    vscode.commands.registerCommand("frost.openFile", (itemOrPath: unknown) => {
      // Accepts either a file path string or an object with filePath
      const filePath =
        typeof itemOrPath === "string"
          ? itemOrPath
          : (itemOrPath as any)?.filePath;
      if (filePath) {
        const uri = vscode.Uri.file(filePath);
        vscode.window.showTextDocument(uri);
      }
    }),
    vscode.commands.registerCommand("frost.refreshData", () => {
      dataProvider.refresh();
    }),
    vscode.commands.registerCommand("frost.loadData", () => {
      runner.runInTerminal("load");
    }),
    vscode.commands.registerCommand("frost.loadDataFile", (item) => {
      if (item?.fqn) {
        // frost load loads all CSVs; show the terminal so user sees progress
        runner.runInTerminal("load");
      }
    }),
    vscode.commands.registerCommand("frost.refreshVariables", () => {
      variablesProvider.refresh();
    }),
    vscode.commands.registerCommand("frost.editVariable", async (item) => {
      const varName = item?.varName ?? item?.label;
      if (!varName) { return; }
      const currentValue = item?.varValue ?? item?.description ?? "";
      const newValue = await vscode.window.showInputBox({
        prompt: `New value for {{${varName}}}`,
        value: currentValue,
      });
      if (newValue === undefined) { return; } // cancelled
      // Update the config file
      const configName = vscode.workspace
        .getConfiguration("frost")
        .get<string>("configPath", "frost-config.yml");
      const configFile = require("path").resolve(runner.cwd, configName);
      try {
        const fs = require("fs");
        let content: string = fs.readFileSync(configFile, "utf-8");
        // Replace the existing key: value line
        const escaped = varName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const re = new RegExp(`(^\\s+${escaped}\\s*:\\s*)(.*)$`, "m");
        if (re.test(content)) {
          content = content.replace(re, `$1${newValue}`);
        } else {
          // Variable not found inline; append under variables:
          content = content.replace(
            /(variables\s*:\s*\n)/,
            `$1  ${varName}: ${newValue}\n`
          );
        }
        fs.writeFileSync(configFile, content, "utf-8");
        variablesProvider.refresh();
        vscode.window.showInformationMessage(
          `Updated {{${varName}}} = ${newValue}`
        );
      } catch (err: any) {
        vscode.window.showErrorMessage(
          `Could not update variable: ${err.message}`
        );
      }
    }),
    vscode.commands.registerCommand("frost.addVariable", async () => {
      const varName = await vscode.window.showInputBox({
        prompt: "Variable name (used as {{name}} in SQL)",
        placeHolder: "e.g. database",
      });
      if (!varName) { return; }
      const varValue = await vscode.window.showInputBox({
        prompt: `Value for {{${varName}}}`,
        placeHolder: "e.g. PROD_DB",
      });
      if (varValue === undefined) { return; }
      const configName = vscode.workspace
        .getConfiguration("frost")
        .get<string>("configPath", "frost-config.yml");
      const configFile = require("path").resolve(runner.cwd, configName);
      try {
        const fs = require("fs");
        let content: string = fs.readFileSync(configFile, "utf-8");
        if (/^\s*variables\s*:/m.test(content)) {
          // Append under existing variables block
          content = content.replace(
            /(variables\s*:\s*\n)/,
            `$1  ${varName}: ${varValue}\n`
          );
        } else {
          // Add a variables section at end of file
          content += `\nvariables:\n  ${varName}: ${varValue}\n`;
        }
        fs.writeFileSync(configFile, content, "utf-8");
        variablesProvider.refresh();
        vscode.window.showInformationMessage(
          `Added {{${varName}}} = ${varValue}`
        );
      } catch (err: any) {
        vscode.window.showErrorMessage(
          `Could not add variable: ${err.message}`
        );
      }
    }),
    vscode.commands.registerCommand("frost.copyVariablePlaceholder", (item) => {
      const varName = item?.varName ?? item?.label;
      if (varName) {
        vscode.env.clipboard.writeText(`{{${varName}}}`);
        vscode.window.showInformationMessage(
          `Copied {{${varName}}} to clipboard`
        );
      }
    }),
    vscode.commands.registerCommand("frost.selectCsv", async () => {
      const uris = await vscode.window.showOpenDialog({
        canSelectMany: false,
        filters: { "CSV Files": ["csv"] },
        openLabel: "Select CSV to load",
      });
      if (uris && uris.length > 0) {
        const csvPath = uris[0].fsPath;
        // Copy CSV to the data folder and refresh
        const dataFolder = require("path").join(runner.cwd, "data");
        const destFile = require("path").join(
          dataFolder,
          require("path").basename(csvPath)
        );
        try {
          await vscode.workspace.fs.createDirectory(vscode.Uri.file(dataFolder));
        } catch {
          /* already exists */
        }
        await vscode.workspace.fs.copy(
          vscode.Uri.file(csvPath),
          vscode.Uri.file(destFile),
          { overwrite: true }
        );
        dataProvider.refresh();
        vscode.window.showInformationMessage(
          `Copied ${require("path").basename(csvPath)} to data/ folder. Use Load Data to push to Snowflake.`
        );
      }
    })
  );

  // ── Status bar ──────────────────────────────────────────────
  statusBar = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Left,
    50
  );
  statusBar.text = "$(snowflake) Frost";
  statusBar.tooltip = "Frost – Snowflake DDL Manager";
  statusBar.command = "frost.plan";
  statusBar.show();
  context.subscriptions.push(statusBar);

  // ── Settings change listener ─────────────────────────────────
  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration("frost.pythonPath") || e.affectsConfiguration("python.defaultInterpreterPath")) {
        runner.resetPython();
        objectsProvider.refresh();
        diagnostics.run();
      }
      if (e.affectsConfiguration("frost.configPath")) {
        runner.resetProjectRoot();
        runner.resetPython();
        objectsProvider.refresh();
        variablesProvider.refresh();
        diagnostics.run();
      }
    })
  );

  // ── File watcher ────────────────────────────────────────────
  const cfg = vscode.workspace.getConfiguration("frost");
  if (cfg.get<boolean>("autoRefresh", true)) {
    const watcher = vscode.workspace.createFileSystemWatcher("**/*.sql");
    watcher.onDidChange(() => objectsProvider.refresh());
    watcher.onDidCreate(() => objectsProvider.refresh());
    watcher.onDidDelete(() => objectsProvider.refresh());
    context.subscriptions.push(watcher);

    const csvWatcher = vscode.workspace.createFileSystemWatcher("**/*.csv");
    csvWatcher.onDidChange(() => dataProvider.refresh());
    csvWatcher.onDidCreate(() => dataProvider.refresh());
    csvWatcher.onDidDelete(() => dataProvider.refresh());
    context.subscriptions.push(csvWatcher);

    const configWatcher = vscode.workspace.createFileSystemWatcher("**/frost-config*.yml");
    configWatcher.onDidChange(() => variablesProvider.refresh());
    configWatcher.onDidCreate(() => variablesProvider.refresh());
    configWatcher.onDidDelete(() => variablesProvider.refresh());
    context.subscriptions.push(configWatcher);
  }

  // Initial load — auto-install frost-ddl if needed, then refresh
  runner.ensureFrostInstalled().then((ok) => {
    if (ok) {
      objectsProvider.refresh();
      dataProvider.refresh();
      variablesProvider.refresh();
      diagnostics.run();
    }
  });

  vscode.window.showInformationMessage("Frost extension activated ❄️");
}

export function deactivate(): void {
  statusBar?.dispose();
}
