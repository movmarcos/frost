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
import { FrostRunner } from "./frostRunner";
import { LineagePanel } from "./lineagePanel";
import { FrostDiagnostics } from "./diagnostics";

let statusBar: vscode.StatusBarItem;

export function activate(context: vscode.ExtensionContext): void {
  const runner = new FrostRunner();
  const objectsProvider = new FrostObjectsProvider(runner);
  const deployProvider = new FrostDeployProvider();
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
  }

  // Initial load
  objectsProvider.refresh();
  diagnostics.run();

  vscode.window.showInformationMessage("Frost extension activated ❄️");
}

export function deactivate(): void {
  statusBar?.dispose();
}
