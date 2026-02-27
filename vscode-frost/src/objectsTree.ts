/**
 * FrostObjectsProvider – sidebar tree showing all Snowflake objects
 * grouped by schema, then by object type.
 *
 * Structure:
 *   └─ PUBLIC
 *       ├─ Tables
 *       │    ├─ SAMPLE_TABLE   (8 columns)
 *       │    └─ ORDERS         (6 columns)
 *       ├─ Views
 *       │    ├─ VW_ACTIVE_SAMPLES
 *       │    └─ VW_ORDER_SUMMARY
 *       └─ Procedures
 *            └─ SP_GET_SAMPLE_COUNT
 */

import * as vscode from "vscode";
import * as path from "path";
import { FrostRunner, FrostNode } from "./frostRunner";

// ── Tree items ────────────────────────────────────────────

type TreeItem = SchemaItem | TypeGroupItem | ObjectItem | ColumnItem;

class SchemaItem extends vscode.TreeItem {
  constructor(
    public readonly schemaName: string,
    public readonly children: TypeGroupItem[]
  ) {
    super(schemaName, vscode.TreeItemCollapsibleState.Expanded);
    this.iconPath = new vscode.ThemeIcon("database");
    this.contextValue = "frostSchema";
  }
}

class TypeGroupItem extends vscode.TreeItem {
  constructor(
    public readonly groupLabel: string,
    public readonly children: ObjectItem[]
  ) {
    super(groupLabel, vscode.TreeItemCollapsibleState.Expanded);
    this.description = `${children.length}`;
    this.iconPath = new vscode.ThemeIcon(TypeGroupItem.iconFor(groupLabel));
    this.contextValue = "frostTypeGroup";
  }

  private static iconFor(group: string): string {
    const g = group.toUpperCase();
    if (g.includes("TABLE")) { return "layout"; }
    if (g.includes("VIEW"))  { return "eye"; }
    if (g.includes("PROC"))  { return "symbol-method"; }
    if (g.includes("FUNC"))  { return "symbol-function"; }
    if (g.includes("SCHEMA")){ return "folder"; }
    if (g.includes("GRANT")) { return "shield"; }
    return "symbol-misc";
  }
}

class ObjectItem extends vscode.TreeItem {
  public readonly fqn: string;
  public readonly filePath: string;
  public readonly columns: { name: string; type: string }[];

  constructor(node: FrostNode, workspaceRoot: string) {
    const colCount = node.columns?.length ?? 0;
    const state =
      colCount > 0
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None;
    super(node.name, state);

    this.fqn = node.fqn;
    this.filePath = path.isAbsolute(node.file_path)
      ? node.file_path
      : path.join(workspaceRoot, node.file_path);
    this.columns = node.columns ?? [];
    this.description = colCount > 0 ? `${colCount} cols` : "";
    this.tooltip = new vscode.MarkdownString(
      `**${node.fqn}**  \nType: \`${node.object_type}\`  \n` +
        `File: \`${node.file_path}\`  \n` +
        (node.dependencies.length
          ? `Deps: ${node.dependencies.join(", ")}`
          : "No dependencies")
    );
    this.contextValue = "frostObject";
    this.iconPath = new vscode.ThemeIcon(
      ObjectItem.iconFor(node.object_type)
    );

    // Click to open the SQL file
    this.command = {
      command: "frost.openFile",
      title: "Open SQL File",
      arguments: [this],
    };
  }

  private static iconFor(objType: string): string {
    const t = objType.toUpperCase();
    if (t.includes("TABLE")) { return "layout"; }
    if (t.includes("VIEW"))  { return "eye"; }
    if (t.includes("PROC"))  { return "symbol-method"; }
    if (t.includes("FUNC"))  { return "symbol-function"; }
    if (t.includes("SCHEMA")){ return "folder"; }
    if (t.includes("GRANT")) { return "shield"; }
    return "symbol-misc";
  }
}

class ColumnItem extends vscode.TreeItem {
  constructor(col: { name: string; type: string }) {
    super(col.name, vscode.TreeItemCollapsibleState.None);
    this.description = col.type || "";
    this.iconPath = new vscode.ThemeIcon("symbol-field");
    this.contextValue = "frostColumn";
  }
}

// ── Provider ──────────────────────────────────────────────

export class FrostObjectsProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private tree: SchemaItem[] = [];

  constructor(private runner: FrostRunner) {}

  refresh(): void {
    this.loadGraph();
  }

  // ── TreeDataProvider ────────────────────────────────────

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(el?: TreeItem): TreeItem[] {
    if (!el) {
      return this.tree;
    }
    if (el instanceof SchemaItem) {
      return el.children;
    }
    if (el instanceof TypeGroupItem) {
      return el.children;
    }
    if (el instanceof ObjectItem) {
      return el.columns.map((c) => new ColumnItem(c));
    }
    return [];
  }

  // ── internal ────────────────────────────────────────────

  private async loadGraph(): Promise<void> {
    try {
      const payload = await this.runner.graphJson();
      this.tree = this.buildTree(payload.nodes);
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Frost: could not load objects – ${err.message}`
      );
      this.tree = [];
    }
    this._onDidChangeTreeData.fire();
  }

  private buildTree(nodes: FrostNode[]): SchemaItem[] {
    const root =
      vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? "";

    // Group by schema, then by type
    const bySchema = new Map<string, Map<string, ObjectItem[]>>();
    for (const node of nodes) {
      const schema = node.schema || "(default)";
      const typeName = this.friendlyType(node.object_type);
      if (!bySchema.has(schema)) {
        bySchema.set(schema, new Map());
      }
      const typeMap = bySchema.get(schema)!;
      if (!typeMap.has(typeName)) {
        typeMap.set(typeName, []);
      }
      typeMap.get(typeName)!.push(new ObjectItem(node, root));
    }

    const schemas: SchemaItem[] = [];
    for (const [schema, typeMap] of [...bySchema.entries()].sort()) {
      const groups: TypeGroupItem[] = [];
      for (const [typeName, items] of [...typeMap.entries()].sort()) {
        items.sort((a, b) => a.label!.toString().localeCompare(b.label!.toString()));
        groups.push(new TypeGroupItem(typeName, items));
      }
      schemas.push(new SchemaItem(schema, groups));
    }
    return schemas;
  }

  private friendlyType(objType: string): string {
    const map: Record<string, string> = {
      TABLE: "Tables",
      "DYNAMIC TABLE": "Dynamic Tables",
      VIEW: "Views",
      "SECURE VIEW": "Secure Views",
      "MATERIALIZED VIEW": "Materialized Views",
      PROCEDURE: "Procedures",
      FUNCTION: "Functions",
      SCHEMA: "Schemas",
      GRANT: "Grants",
      SEQUENCE: "Sequences",
      STREAM: "Streams",
      TASK: "Tasks",
      PIPE: "Pipes",
      STAGE: "Stages",
      "FILE FORMAT": "File Formats",
    };
    return map[objType.toUpperCase()] ?? objType;
  }
}
