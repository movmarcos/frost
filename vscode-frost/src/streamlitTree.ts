/**
 * FrostStreamlitProvider – sidebar tree for Streamlit apps.
 *
 * Discovers Streamlit apps by running `frost streamlit list --json`
 * and displays them in a tree grouped by directory.
 */

import * as vscode from "vscode";
import { FrostRunner } from "./frostRunner";

/** A Streamlit app from `frost streamlit list --json`. */
export interface StreamlitAppInfo {
  name: string;
  directory: string;
  main_file: string;
  stage: string;
  schema: string;
  warehouse: string;
  query_warehouse: string;
  title: string;
  definition_file: string;
  comment: string;
  external_access_integrations: string[];
  imports: string[];
}

/** Payload from `frost streamlit list --json`. */
interface StreamlitPayload {
  apps: StreamlitAppInfo[];
  snow_cli: string;
}

export class FrostStreamlitProvider
  implements vscode.TreeDataProvider<StreamlitTreeItem>
{
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  private apps: StreamlitAppInfo[] = [];
  private snowCli: string = "";
  private error: string = "";

  constructor(private runner: FrostRunner) {}

  refresh(): void {
    this._onDidChange.fire();
  }

  getTreeItem(el: StreamlitTreeItem): vscode.TreeItem {
    return el;
  }

  async getChildren(
    parent?: StreamlitTreeItem
  ): Promise<StreamlitTreeItem[]> {
    if (!parent) {
      return this.getRootItems();
    }
    if (parent.contextValue === "frostStreamlitApp") {
      return this.getAppDetails(parent.appInfo!);
    }
    return [];
  }

  private async getRootItems(): Promise<StreamlitTreeItem[]> {
    try {
      const payload = await this.runner.streamlitJson();
      this.apps = payload.apps;
      this.snowCli = payload.snow_cli;
      this.error = "";
    } catch (err: any) {
      this.apps = [];
      this.snowCli = "";
      this.error = err.message || String(err);
    }

    const items: StreamlitTreeItem[] = [];

    // Snow CLI status
    if (this.snowCli) {
      const cliItem = new StreamlitTreeItem(
        "snow CLI",
        vscode.TreeItemCollapsibleState.None
      );
      cliItem.description = this.snowCli;
      cliItem.iconPath = new vscode.ThemeIcon("check");
      cliItem.contextValue = "frostStreamlitInfo";
      items.push(cliItem);
    } else {
      const cliItem = new StreamlitTreeItem(
        "snow CLI not found",
        vscode.TreeItemCollapsibleState.None
      );
      cliItem.description = "pip install snowflake-cli-labs";
      cliItem.iconPath = new vscode.ThemeIcon("warning");
      cliItem.contextValue = "frostStreamlitInfo";
      items.push(cliItem);
    }

    if (this.error) {
      const errItem = new StreamlitTreeItem(
        "Error loading apps",
        vscode.TreeItemCollapsibleState.None
      );
      errItem.description = this.error;
      errItem.iconPath = new vscode.ThemeIcon("error");
      items.push(errItem);
      return items;
    }

    if (this.apps.length === 0) {
      const emptyItem = new StreamlitTreeItem(
        "No Streamlit apps found",
        vscode.TreeItemCollapsibleState.None
      );
      emptyItem.description = "Add snowflake.yml with streamlit definition";
      emptyItem.iconPath = new vscode.ThemeIcon("info");
      items.push(emptyItem);
      return items;
    }

    // App items
    for (const app of this.apps) {
      const item = new StreamlitTreeItem(
        app.name,
        vscode.TreeItemCollapsibleState.Collapsed
      );
      item.description = app.schema;
      item.tooltip = `${app.name} — ${app.main_file}\nDir: ${app.directory}`;
      item.iconPath = new vscode.ThemeIcon("browser");
      item.contextValue = "frostStreamlitApp";
      item.appInfo = app;
      items.push(item);
    }

    return items;
  }

  private getAppDetails(app: StreamlitAppInfo): StreamlitTreeItem[] {
    const details: StreamlitTreeItem[] = [];

    const addDetail = (label: string, value: string, icon: string) => {
      if (!value) { return; }
      const item = new StreamlitTreeItem(
        label,
        vscode.TreeItemCollapsibleState.None
      );
      item.description = value;
      item.iconPath = new vscode.ThemeIcon(icon);
      item.contextValue = "frostStreamlitDetail";
      details.push(item);
    };

    addDetail("Main file", app.main_file, "file-code");
    addDetail("Schema", app.schema, "database");
    addDetail("Stage", app.stage, "package");
    addDetail("Warehouse", app.warehouse, "server");
    addDetail("Query WH", app.query_warehouse, "server-process");
    addDetail("Title", app.title, "tag");
    addDetail("Directory", app.directory, "folder");

    if (app.comment) {
      addDetail("Comment", app.comment, "comment");
    }

    // Definition file — clickable
    const defItem = new StreamlitTreeItem(
      "snowflake.yml",
      vscode.TreeItemCollapsibleState.None
    );
    defItem.description = app.definition_file;
    defItem.iconPath = new vscode.ThemeIcon("file-symlink-file");
    defItem.contextValue = "frostStreamlitFile";
    defItem.command = {
      command: "frost.openFile",
      title: "Open",
      arguments: [app.definition_file],
    };
    details.push(defItem);

    return details;
  }
}

export class StreamlitTreeItem extends vscode.TreeItem {
  appInfo?: StreamlitAppInfo;
}
