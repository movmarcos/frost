/**
 * LineagePanel – renders the frost lineage HTML inside a VS Code WebviewPanel.
 *
 * Reuses the same D3.js visualisation that `frost lineage --local` produces,
 * but embedded inside the editor instead of opening a browser.
 */

import * as vscode from "vscode";
import { FrostRunner } from "./frostRunner";

export class LineagePanel {
  private static currentPanel: LineagePanel | undefined;
  private readonly panel: vscode.WebviewPanel;
  private disposed = false;

  private constructor(
    panel: vscode.WebviewPanel,
    private readonly runner: FrostRunner
  ) {
    this.panel = panel;

    this.panel.onDidDispose(() => {
      this.disposed = true;
      LineagePanel.currentPanel = undefined;
    });

    this.loadContent();
  }

  static show(
    extensionUri: vscode.Uri,
    runner: FrostRunner
  ): void {
    if (LineagePanel.currentPanel) {
      LineagePanel.currentPanel.panel.reveal(vscode.ViewColumn.One);
      LineagePanel.currentPanel.loadContent();
      return;
    }

    const panel = vscode.window.createWebviewPanel(
      "frostLineage",
      "Frost · Lineage",
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
      }
    );

    LineagePanel.currentPanel = new LineagePanel(panel, runner);
  }

  private async loadContent(): Promise<void> {
    try {
      this.panel.webview.html = this.loadingHtml();
      const html = await this.runner.lineageHtml();
      if (!this.disposed) {
        this.panel.webview.html = html;
      }
    } catch (err: any) {
      if (!this.disposed) {
        this.panel.webview.html = this.errorHtml(err.message);
      }
    }
  }

  private loadingHtml(): string {
    return `<!DOCTYPE html>
<html><head><style>
  body { display:flex; align-items:center; justify-content:center;
         height:100vh; font-family:system-ui; color:#888; background:#0d1117; }
</style></head>
<body><p>Loading lineage…</p></body></html>`;
  }

  private errorHtml(msg: string): string {
    return `<!DOCTYPE html>
<html><head><style>
  body { display:flex; align-items:center; justify-content:center;
         height:100vh; font-family:system-ui; color:#f85149; background:#0d1117; }
  code { background:#1c1c1c; padding:8px 16px; border-radius:6px; }
</style></head>
<body><div style="text-align:center">
  <h3>Could not load lineage</h3>
  <code>${msg.replace(/</g, "&lt;")}</code>
  <p style="color:#888; margin-top:1em">Make sure frost-ddl is installed and frost-config.yml is present.</p>
</div></body></html>`;
  }
}
