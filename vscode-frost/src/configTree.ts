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
