/**
 * FrostObjectsProvider – sidebar tree showing all Snowflake objects
 * grouped by schema, then by object type.
 *
 * Performance-optimised for large projects (1 000+ files):
 *   • Schemas and type groups start **collapsed** so VS Code only
 *     renders a handful of top-level items.
 *   • A concurrency guard prevents overlapping Python processes.
 *   • A debounce helper is exposed for the file-watcher layer.
 *   • A "Loading…" placeholder is shown while the graph is built.
 *
 * Structure:
 *   └─ PUBLIC
 *       ├─ Tables  (120)
 *       │    ├─ SAMPLE_TABLE   (8 cols)
 *       │    └─ ORDERS         (6 cols)
 *       ├─ Views  (45)
 *       │    └─ VW_ACTIVE_SAMPLES
 *       └─ Procedures  (30)
 *            └─ SP_GET_SAMPLE_COUNT
 */

import * as vscode from "vscode";
import * as path from "path";
import { FrostRunner, FrostNode } from "./frostRunner";

// ── Tree items ────────────────────────────────────────────

type TreeItem = SchemaItem | TypeGroupItem | ObjectItem | ColumnItem | PlaceholderItem;

/** Placeholder shown while the graph is loading. */
class PlaceholderItem extends vscode.TreeItem {
  constructor(label: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon("sync~spin");
    this.contextValue = "frostPlaceholder";
  }
}

class SchemaItem extends vscode.TreeItem {
  constructor(
    public readonly schemaName: string,
    public readonly children: TypeGroupItem[]
  ) {
    // Start collapsed so VS Code doesn't render all children immediately
    super(schemaName, vscode.TreeItemCollapsibleState.Collapsed);
    const total = children.reduce((n, g) => n + g.children.length, 0);
    this.description = `${total} objects`;
    this.iconPath = new vscode.ThemeIcon("database");
    this.contextValue = "frostSchema";
  }
}

class TypeGroupItem extends vscode.TreeItem {
  constructor(
    public readonly groupLabel: string,
    public readonly children: ObjectItem[]
  ) {
    // Start collapsed — user expands on demand
    super(groupLabel, vscode.TreeItemCollapsibleState.Collapsed);
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
      arguments: [this.filePath],
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

  private tree: TreeItem[] = [];
  /** Flat cache of the most recent FrostNode[] for the lineage picker. */
  private _flatNodes: FrostNode[] = [];

  /** Prevents overlapping `frost graph --json` processes. */
  private _loading = false;

  /** Debounce timer for `refresh()`. */
  private _debounceTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(private runner: FrostRunner) {}

  /** Flat list of every known FQN in the current graph. Empty while loading. */
  public getAllFqns(): string[] {
    return this._flatNodes.map((n) => n.fqn);
  }

  /** Look up the file path for a given FQN (case-insensitive). */
  public getFilePath(fqn: string): string | undefined {
    const upper = fqn.toUpperCase();
    const node = this._flatNodes.find((n) => n.fqn.toUpperCase() === upper);
    return node?.file_path;
  }

  /**
   * Debounced refresh — waits `delayMs` before actually reloading.
   * Repeated calls within the window reset the timer.
   * Pass 0 (default) for an immediate (non-debounced) refresh.
   */
  refresh(delayMs = 0): void {
    if (this._debounceTimer) {
      clearTimeout(this._debounceTimer);
      this._debounceTimer = undefined;
    }
    if (delayMs <= 0) {
      this.loadGraph();
    } else {
      this._debounceTimer = setTimeout(() => {
        this._debounceTimer = undefined;
        this.loadGraph();
      }, delayMs);
    }
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
    // Concurrency guard — skip if we're already loading
    if (this._loading) {
      return;
    }
    this._loading = true;

    // Show "Loading…" placeholder immediately
    this.tree = [new PlaceholderItem("Loading objects…")];
    this._onDidChangeTreeData.fire();

    try {
      const payload = await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Window,
          title: "Frost: loading objects…",
        },
        () => this.runner.graphJson()
      );

      this._flatNodes = payload.nodes;
      this.tree = this.buildTree(payload.nodes);
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Frost: could not load objects – ${err.message}`
      );
      this._flatNodes = [];
      this.tree = [];
    } finally {
      this._loading = false;
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
