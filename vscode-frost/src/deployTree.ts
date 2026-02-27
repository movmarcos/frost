/**
 * FrostDeployProvider – simple tree showing recent deploy actions
 * and quick-access buttons for Plan / Deploy.
 */

import * as vscode from "vscode";

class ActionItem extends vscode.TreeItem {
  constructor(
    label: string,
    icon: string,
    private readonly cmdId: string
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon(icon);
    this.command = { command: cmdId, title: label };
  }
}

export class FrostDeployProvider
  implements vscode.TreeDataProvider<ActionItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    ActionItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  getTreeItem(el: ActionItem): vscode.TreeItem {
    return el;
  }

  getChildren(): ActionItem[] {
    return [
      new ActionItem("Plan", "list-ordered", "frost.plan"),
      new ActionItem("Deploy", "cloud-upload", "frost.deploy"),
      new ActionItem("Deploy (Force)", "sync", "frost.deployForce"),
      new ActionItem("Lineage (remote)", "type-hierarchy", "frost.lineage"),
      new ActionItem("Lineage (local)", "type-hierarchy-sub", "frost.lineageLocal"),
    ];
  }
}
