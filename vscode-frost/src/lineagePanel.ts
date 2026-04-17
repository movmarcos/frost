/**
 * LineagePanel — focused lineage viewer.
 *
 * Opens a webview with an object picker. When the user picks an object,
 * the panel calls `frost lineage --local --json --object FQN --depth N`
 * and renders the small subgraph. A "Show full graph" button fetches the
 * full-graph JSON behind a size confirmation.
 */

import * as vscode from "vscode";
import { FrostRunner, SubgraphPayload } from "./frostRunner";
import { FrostObjectsProvider } from "./objectsTree";

const FULL_GRAPH_WARNING_THRESHOLD = 300;

export class LineagePanel {
  private static currentPanel: LineagePanel | undefined;
  private readonly panel: vscode.WebviewPanel;
  private disposed = false;

  private constructor(
    panel: vscode.WebviewPanel,
    private readonly extensionUri: vscode.Uri,
    private readonly runner: FrostRunner,
    private readonly objectsProvider: FrostObjectsProvider,
  ) {
    this.panel = panel;
    this.panel.webview.html = this.buildHtml();
    this.panel.onDidDispose(() => {
      this.disposed = true;
      LineagePanel.currentPanel = undefined;
    });
    this.panel.webview.onDidReceiveMessage((msg) => this.onMessage(msg));
  }

  static show(
    extensionUri: vscode.Uri,
    runner: FrostRunner,
    objectsProvider: FrostObjectsProvider,
  ): void {
    if (LineagePanel.currentPanel) {
      LineagePanel.currentPanel.panel.reveal(vscode.ViewColumn.One);
      return;
    }
    const mediaRoot = vscode.Uri.joinPath(extensionUri, "media", "lineage");
    const panel = vscode.window.createWebviewPanel(
      "frostLineage",
      "Frost · Lineage",
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [mediaRoot],
      },
    );
    LineagePanel.currentPanel = new LineagePanel(
      panel, extensionUri, runner, objectsProvider,
    );
  }

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

  // --- Message dispatch --------------------------------------------
  private async onMessage(msg: any): Promise<void> {
    if (this.disposed) return;
    switch (msg?.type) {
      case "ready":
        this.postObjectList();
        return;
      case "fetchSubgraph":
        await this.handleSubgraph(msg.fqn, msg.depth, msg.direction);
        return;
      case "fetchFullGraph":
        await this.handleFullGraph();
        return;
    }
  }

  private postObjectList(): void {
    const fqns = this.objectsProvider.getAllFqns();
    this.panel.webview.postMessage({ type: "objectList", fqns });
  }

  private async handleSubgraph(
    fqn: string, depth: number, direction: "up" | "down" | "both"
  ): Promise<void> {
    try {
      const payload = await this.runner.lineageSubgraph(fqn, depth, direction);
      if (this.disposed) return;
      this.panel.webview.postMessage({ type: "subgraph", payload });
    } catch (err: any) {
      if (this.disposed) return;
      this.postError(`Could not load lineage for ${fqn}: ${err.message}`);
    }
  }

  private async handleFullGraph(): Promise<void> {
    const count = this.objectsProvider.getAllFqns().length;
    if (count > FULL_GRAPH_WARNING_THRESHOLD) {
      const choice = await vscode.window.showWarningMessage(
        `This project has ${count} objects. Rendering the full graph ` +
          `may be slow and use significant memory. Continue?`,
        { modal: true },
        "Continue",
      );
      if (this.disposed) return;
      if (choice !== "Continue") return;
    }
    try {
      const payload = await this.runner.lineageFullJson();
      if (this.disposed) return;
      this.panel.webview.postMessage({ type: "subgraph", payload });
    } catch (err: any) {
      if (this.disposed) return;
      this.postError(`Could not load full lineage: ${err.message}`);
    }
  }

  private postError(message: string): void {
    this.panel.webview.postMessage({ type: "error", message });
  }

  // --- HTML shell --------------------------------------------------
  private buildHtml(): string {
    const mediaRoot = vscode.Uri.joinPath(this.extensionUri, "media", "lineage");
    const indexUri = vscode.Uri.joinPath(mediaRoot, "index.html");
    const cssUri = this.panel.webview.asWebviewUri(
      vscode.Uri.joinPath(mediaRoot, "lineage.css"),
    );
    const jsUri = this.panel.webview.asWebviewUri(
      vscode.Uri.joinPath(mediaRoot, "lineage.js"),
    );
    // D3 v7 is bundled locally to avoid CSP/CDN issues; see Task 8.
    const d3Uri = this.panel.webview.asWebviewUri(
      vscode.Uri.joinPath(mediaRoot, "d3.min.js"),
    );
    const tpl = require("fs").readFileSync(indexUri.fsPath, "utf-8") as string;
    return tpl
      .replace(/\$\{webview\.cspSource\}/g, this.panel.webview.cspSource)
      .replace(/\$\{cssUri\}/g, cssUri.toString())
      .replace(/\$\{jsUri\}/g, jsUri.toString())
      .replace(/\$\{d3Uri\}/g, d3Uri.toString());
  }
}
