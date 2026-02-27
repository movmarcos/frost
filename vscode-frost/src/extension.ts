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
import { FrostRunner } from "./frostRunner";
import { LineagePanel } from "./lineagePanel";
import { FrostDiagnostics } from "./diagnostics";

let statusBar: vscode.StatusBarItem;

export function activate(context: vscode.ExtensionContext): void {
  const runner = new FrostRunner();
  const objectsProvider = new FrostObjectsProvider(runner);
  const deployProvider = new FrostDeployProvider();
  const dataProvider = new FrostDataProvider(runner);
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
    vscode.commands.registerCommand("frost.openFile", (item) => {
      if (item?.filePath) {
        const uri = vscode.Uri.file(item.filePath);
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
  }

  // Initial load
  objectsProvider.refresh();
  dataProvider.refresh();
  diagnostics.run();

  vscode.window.showInformationMessage("Frost extension activated ❄️");
}

export function deactivate(): void {
  statusBar?.dispose();
}
