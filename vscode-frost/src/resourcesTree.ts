/**
 * FrostResourcesProvider — sidebar tree listing live Snowflake resources
 * in the configured database, grouped by Schema → Type → Resource.
 *
 * Loads on-demand (first panel expand), not on activation.
 */

import * as vscode from "vscode";
import { FrostRunner, ResourceItem, ResourcesPayload } from "./frostRunner";
import { ConfigReader } from "./configReader";
import { FrostObjectsProvider } from "./objectsTree";

// ── Tree items ────────────────────────────────────────────

type TreeItem = SchemaItem | TypeGroupItem | ResourceTreeItem | PlaceholderItem | WarningGroupItem | WarningItem;

class SchemaItem extends vscode.TreeItem {
  constructor(
    public readonly schemaName: string,
    public readonly children: TypeGroupItem[],
  ) {
    super(schemaName, vscode.TreeItemCollapsibleState.Collapsed);
    this.iconPath = new vscode.ThemeIcon("database");
    this.contextValue = "frostResourceSchema";
  }
}

class TypeGroupItem extends vscode.TreeItem {
  constructor(
    public readonly typeName: string,
    public readonly children: ResourceTreeItem[],
  ) {
    super(typeName, vscode.TreeItemCollapsibleState.Collapsed);
    this.description = `${children.length}`;
    this.iconPath = new vscode.ThemeIcon("symbol-class");
    this.contextValue = "frostResourceTypeGroup";
  }
}

class ResourceTreeItem extends vscode.TreeItem {
  public readonly fqn: string;
  public readonly resourceType: string;

  constructor(resource: ResourceItem, private readonly isLocal: boolean) {
    super(resource.name, vscode.TreeItemCollapsibleState.None);
    this.fqn = resource.fqn;
    this.resourceType = resource.type;
    this.description = resource.owner;
    this.tooltip = new vscode.MarkdownString(
      `**${resource.fqn}**  \nType: ${resource.type}  \nOwner: ${resource.owner}` +
      (resource.comment ? `  \nComment: ${resource.comment}` : "") +
      (resource.created_on ? `  \nCreated: ${resource.created_on}` : ""),
    );
    this.iconPath = new vscode.ThemeIcon(
      isLocal ? "symbol-method" : "symbol-reference",
    );
    this.contextValue = isLocal ? "frostResourceLocal" : "frostResourceRemote";
  }
}

class PlaceholderItem extends vscode.TreeItem {
  constructor(label: string) {
    super(label, vscode.TreeItemCollapsibleState.None);
  }
}

class WarningGroupItem extends vscode.TreeItem {
  constructor(public readonly children: WarningItem[]) {
    super("Warnings", vscode.TreeItemCollapsibleState.Collapsed);
    this.description = `${children.length}`;
    this.iconPath = new vscode.ThemeIcon("warning");
  }
}

class WarningItem extends vscode.TreeItem {
  constructor(message: string) {
    super(message, vscode.TreeItemCollapsibleState.None);
    this.iconPath = new vscode.ThemeIcon("info");
  }
}

// ── Provider ──────────────────────────────────────────────

export class FrostResourcesProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private tree: TreeItem[] = [];
  private _loaded = false;
  private _loading = false;

  constructor(
    private readonly runner: FrostRunner,
    private readonly configReader: ConfigReader,
    private readonly objectsProvider: FrostObjectsProvider,
  ) {}

  refresh(): void {
    this._loaded = false;
    this.loadResources();
  }

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(el?: TreeItem): TreeItem[] {
    if (!el) {
      // Lazy-init: trigger load on first expand
      if (!this._loaded && !this._loading) {
        this.loadResources();
      }
      return this.tree;
    }
    if (el instanceof SchemaItem) {
      return el.children;
    }
    if (el instanceof TypeGroupItem) {
      return el.children;
    }
    if (el instanceof WarningGroupItem) {
      return el.children;
    }
    return [];
  }

  /** Look up whether a resource FQN exists in the local Objects tree. */
  isLocalFqn(fqn: string): boolean {
    const localFqns = this.objectsProvider.getAllFqns();
    const upper = fqn.toUpperCase();
    return localFqns.some((f) => f.toUpperCase() === upper);
  }

  // ── internal ────────────────────────────────────────────

  private async loadResources(): Promise<void> {
    if (this._loading) {
      return;
    }
    this._loading = true;

    this.tree = [new PlaceholderItem("Loading resources...")];
    this._onDidChangeTreeData.fire();

    try {
      const payload = await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Window,
          title: "Frost: loading resources...",
        },
        () => this.runner.resourcesJson(),
      );

      this.tree = this.buildTree(payload);
      this._loaded = true;
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Frost: could not load resources – ${err.message}`,
      );
      this.tree = [new PlaceholderItem("Could not connect to Snowflake")];
    } finally {
      this._loading = false;
    }
    this._onDidChangeTreeData.fire();
  }

  private buildTree(payload: ResourcesPayload): TreeItem[] {
    const localFqns = new Set(
      this.objectsProvider.getAllFqns().map((f) => f.toUpperCase()),
    );

    // Group: schema → type → resources
    const bySchema = new Map<string, Map<string, ResourceTreeItem[]>>();

    for (const r of payload.resources) {
      if (!bySchema.has(r.schema)) {
        bySchema.set(r.schema, new Map());
      }
      const byType = bySchema.get(r.schema)!;
      // Pluralise the type for display
      const displayType = pluralise(r.type);
      if (!byType.has(displayType)) {
        byType.set(displayType, []);
      }
      const isLocal = localFqns.has(r.fqn.toUpperCase());
      byType.get(displayType)!.push(new ResourceTreeItem(r, isLocal));
    }

    const items: TreeItem[] = [];

    // Sort schemas alphabetically
    const sortedSchemas = Array.from(bySchema.keys()).sort();
    for (const schema of sortedSchemas) {
      const byType = bySchema.get(schema)!;
      const typeGroups: TypeGroupItem[] = [];
      const sortedTypes = Array.from(byType.keys()).sort();
      for (const typeName of sortedTypes) {
        const resources = byType.get(typeName)!;
        resources.sort((a, b) => a.label!.toString().localeCompare(b.label!.toString()));
        typeGroups.push(new TypeGroupItem(typeName, resources));
      }
      items.push(new SchemaItem(schema, typeGroups));
    }

    // Append warnings group if any
    if (payload.warnings && payload.warnings.length > 0) {
      const warningItems = payload.warnings.map((w) => new WarningItem(w));
      items.push(new WarningGroupItem(warningItems));
    }

    if (items.length === 0) {
      return [new PlaceholderItem("No resources found")];
    }

    return items;
  }
}

/** Pluralised display labels for resource types. */
const PLURAL_LABELS: Record<string, string> = {
  "TABLE": "Tables",
  "VIEW": "Views",
  "PROCEDURE": "Procedures",
  "FUNCTION": "Functions",
  "DYNAMIC TABLE": "Dynamic Tables",
  "FILE FORMAT": "File Formats",
  "STAGE": "Stages",
  "TASK": "Tasks",
  "TAG": "Tags",
  "STREAM": "Streams",
  "PIPE": "Pipes",
  "MATERIALIZED VIEW": "Materialized Views",
  "ALERT": "Alerts",
  "EVENT TABLE": "Event Tables",
  "SEQUENCE": "Sequences",
};

function pluralise(type: string): string {
  return PLURAL_LABELS[type.toUpperCase()] ?? type + "s";
}
