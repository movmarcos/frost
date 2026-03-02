/**
 * FrostVariablesProvider – sidebar tree showing all template variables
 * defined in frost-config.yml plus any from the FROST_VARS environment.
 *
 * Structure:
 *   └─ admin_role = SYSADMIN        (from config)
 *   └─ read_role  = READ_ONLY_ROLE  (from config)
 *   └─ database   = STAGING_DB      (from FROST_VARS)
 */

import * as vscode from "vscode";
import * as fs from "fs";
import * as path from "path";
import { FrostRunner } from "./frostRunner";

// ── Tree items ────────────────────────────────────────────

type TreeItem = VariableSourceItem | VariableItem;

class VariableSourceItem extends vscode.TreeItem {
  constructor(
    public readonly sourceLabel: string,
    public readonly variables: VariableItem[]
  ) {
    super(sourceLabel, vscode.TreeItemCollapsibleState.Expanded);
    this.description = `${variables.length}`;
    this.iconPath = new vscode.ThemeIcon(
      sourceLabel === "Config File" ? "file-code" : "terminal"
    );
    this.contextValue = "frostVariableSource";
  }
}

class VariableItem extends vscode.TreeItem {
  constructor(
    public readonly varName: string,
    public readonly varValue: string,
    public readonly source: string
  ) {
    super(varName, vscode.TreeItemCollapsibleState.None);
    this.description = varValue;
    this.tooltip = new vscode.MarkdownString(
      `**\`{{${varName}}}\`**  \nValue: \`${varValue}\`  \nSource: ${source}`
    );
    this.iconPath = new vscode.ThemeIcon("symbol-variable");
    this.contextValue = "frostVariable";
  }
}

// ── Provider ──────────────────────────────────────────────

export class FrostVariablesProvider
  implements vscode.TreeDataProvider<TreeItem>
{
  private _onDidChangeTreeData = new vscode.EventEmitter<
    TreeItem | undefined | null | void
  >();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  private tree: TreeItem[] = [];

  constructor(private runner: FrostRunner) {
    this.loadVariables();
  }

  refresh(): void {
    this.loadVariables();
  }

  // ── TreeDataProvider ────────────────────────────────────

  getTreeItem(el: TreeItem): vscode.TreeItem {
    return el;
  }

  getChildren(el?: TreeItem): TreeItem[] {
    if (!el) {
      return this.tree;
    }
    if (el instanceof VariableSourceItem) {
      return el.variables;
    }
    return [];
  }

  // ── internal ────────────────────────────────────────────

  private loadVariables(): void {
    const configVars = this.readConfigVariables();
    const envVars = this.readEnvVariables();

    const items: TreeItem[] = [];

    if (configVars.length > 0 || envVars.length === 0) {
      items.push(new VariableSourceItem("Config File", configVars));
    }
    if (envVars.length > 0) {
      items.push(new VariableSourceItem("FROST_VARS", envVars));
    }

    this.tree = items;
    this._onDidChangeTreeData.fire();
  }

  /** Parse variables from frost-config.yml. */
  private readConfigVariables(): VariableItem[] {
    try {
      const configName = vscode.workspace
        .getConfiguration("frost")
        .get<string>("configPath", "frost-config.yml");
      const configFile = path.resolve(this.runner.cwd, configName);

      if (!fs.existsSync(configFile)) {
        return [];
      }

      const content = fs.readFileSync(configFile, "utf-8");
      const vars = this.parseYamlVariables(content);
      return Object.entries(vars).map(
        ([k, v]) => new VariableItem(k, String(v), "frost-config.yml")
      );
    } catch {
      return [];
    }
  }

  /** Parse FROST_VARS environment variable (JSON string). */
  private readEnvVariables(): VariableItem[] {
    try {
      const raw = process.env.FROST_VARS;
      if (!raw) {
        return [];
      }
      const parsed = JSON.parse(raw);
      if (typeof parsed !== "object" || parsed === null) {
        return [];
      }
      return Object.entries(parsed).map(
        ([k, v]) => new VariableItem(k, String(v), "FROST_VARS")
      );
    } catch {
      return [];
    }
  }

  /**
   * Lightweight YAML parser for the variables section only.
   * Avoids adding a js-yaml dependency.
   * Extracts key-value pairs under `variables:` or `vars:`.
   */
  private parseYamlVariables(content: string): Record<string, string> {
    const result: Record<string, string> = {};
    const lines = content.split("\n");
    let inVarsBlock = false;
    let blockIndent = -1;

    for (const line of lines) {
      // Skip blank lines and comments
      if (/^\s*$/.test(line) || /^\s*#/.test(line)) {
        if (inVarsBlock) { continue; }
        continue;
      }

      const indentMatch = line.match(/^(\s*)/);
      const indent = indentMatch ? indentMatch[1].length : 0;

      // Check if this is the variables/vars section header
      if (/^\s*(variables|vars)\s*:\s*$/.test(line)) {
        inVarsBlock = true;
        blockIndent = -1; // will be set by first child
        continue;
      }

      if (inVarsBlock) {
        // End of block when we encounter a line at root indent (0) or less
        // than the block indent that is not a comment/blank
        if (blockIndent >= 0 && indent <= blockIndent - 2) {
          inVarsBlock = false;
          continue;
        }
        if (indent === 0) {
          inVarsBlock = false;
          continue;
        }

        // First indented child sets the expected indent level
        if (blockIndent < 0) {
          blockIndent = indent;
        }

        // Parse key: value
        const kvMatch = line.match(/^\s+(\w[\w.-]*)\s*:\s*(.*)$/);
        if (kvMatch) {
          const key = kvMatch[1];
          let value = kvMatch[2].trim();
          // Strip quotes
          if (
            (value.startsWith('"') && value.endsWith('"')) ||
            (value.startsWith("'") && value.endsWith("'"))
          ) {
            value = value.slice(1, -1);
          }
          result[key] = value;
        }
      }
    }

    return result;
  }
}
