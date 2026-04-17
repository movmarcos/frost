/**
 * FrostDeployProvider – shows deploy target + quick-access action buttons.
 */

import * as vscode from "vscode";
import { ConfigReader } from "./configReader";

type TreeItem = TargetItem | ActionItem;

class TargetItem extends vscode.TreeItem {
  constructor(database: string, role: string) {
    const label = database
      ? `Target: ${database} (${role})`
      : "Target: not configured";
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon("database");
    this.description = "";
    this.contextValue = "frostDeployTarget";
  }
}

class ActionItem extends vscode.TreeItem {
  constructor(
    label: string,
    icon: string,
    private readonly cmdId: string,
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
    this.command = { command: cmdId, title: label };
  }
}

export class FrostDeployProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(private readonly configReader: ConfigReader) {}

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(): TreeItem[] {
    const data = this.configReader.data;
    const database = data?.database ?? "";
    const role = data?.role ?? "SYSADMIN";

    return [
      new TargetItem(database, role),
      new ActionItem("Plan", "list-ordered", "frost.plan"),
      new ActionItem("Deploy", "cloud-upload", "frost.deploy"),
      new ActionItem("Deploy (Force)", "sync", "frost.deployForce"),
      new ActionItem("Lineage (remote)", "type-hierarchy", "frost.lineage"),
      new ActionItem("Lineage (local)", "type-hierarchy-sub", "frost.lineageLocal"),
    ];
  }
}
