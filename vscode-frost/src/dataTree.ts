/**
 * FrostDataProvider – sidebar tree showing CSV data files
 * available for loading into Snowflake.
 *
 * Structure:
 *   └─ countries.csv   (3 cols, 10 rows)  →  PUBLIC.COUNTRIES
 *   └─ products.csv    (5 cols, 42 rows)  →  PUBLIC.PRODUCTS
 */

import * as vscode from "vscode";
import * as path from "path";
import { FrostRunner, DataFileInfo } from "./frostRunner";

// ── Tree items ────────────────────────────────────────────

type TreeItem = DataFileItem | DataColumnItem;

class DataFileItem extends vscode.TreeItem {
  public readonly filePath: string;
  public readonly fqn: string;
  public readonly columns: string[];

  constructor(info: DataFileInfo, workspaceRoot: string) {
    const colCount = info.columns.length;
    const state =
      colCount > 0
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None;
    super(path.basename(info.file_path), state);

    this.filePath = path.isAbsolute(info.file_path)
      ? info.file_path
      : path.join(workspaceRoot, info.file_path);
    this.fqn = info.fqn;
    this.columns = info.columns;

    this.description = `${colCount} cols, ${info.row_count} rows`;
    this.tooltip = new vscode.MarkdownString(
      `**${info.fqn}**  \nFile: \`${info.file_path}\`  \n` +
        `Columns: ${colCount}  \nRows: ${info.row_count}  \n` +
        `Checksum: \`${info.checksum}\``
    );
    this.contextValue = "frostDataFile";
    this.iconPath = new vscode.ThemeIcon("file");

    // Click to open the CSV file
    this.command = {
      command: "frost.openFile",
      title: "Open CSV File",
      arguments: [this.filePath],
    };
  }
}

class DataColumnItem extends vscode.TreeItem {
  constructor(colName: string, colType: string) {
    super(colName, vscode.TreeItemCollapsibleState.None);
    this.description = colType || "VARCHAR";
    this.iconPath = new vscode.ThemeIcon("symbol-field");
    this.contextValue = "frostDataColumn";
  }
}

// ── Provider ──────────────────────────────────────────────

export class FrostDataProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private tree: DataFileItem[] = [];

  constructor(private runner: FrostRunner) {}

  refresh(): void {
    this.loadData();
  }

  // ── TreeDataProvider ────────────────────────────────────

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(el?: TreeItem): TreeItem[] {
    if (!el) {
      return this.tree;
    }
    if (el instanceof DataFileItem) {
      return el.columns.map((c) => new DataColumnItem(c, ""));
    }
    return [];
  }

  // ── internal ────────────────────────────────────────────

  private async loadData(): Promise<void> {
    try {
      const payload = await this.runner.loadJson();
      this.tree = this.buildTree(payload.files);
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Frost: could not load data files – ${err.message}`
      );
      this.tree = [];
    }
    this._onDidChangeTreeData.fire();
  }

  private buildTree(files: DataFileInfo[]): DataFileItem[] {
    const root =
      vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? "";
    return files
      .map((f) => new DataFileItem(f, root))
      .sort((a, b) => a.label!.toString().localeCompare(b.label!.toString()));
  }
}
