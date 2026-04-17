/**
 * ConfigReader — parses frost-config.yml and caches the result.
 *
 * Consumed by ConfigTree (full display), DeployTree (target header),
 * and ResourcesTree (root label). Uses a lightweight regex-based parser
 * to avoid adding a js-yaml dependency.
 */

import * as vscode from "vscode";
import * as fs from "fs";
import * as path from "path";

export interface FrostConfigData {
  // Project
  objectsFolder: string;
  dataFolder: string;
  dataSchema: string;
  // Connection
  account: string;
  user: string;
  role: string;
  warehouse: string;
  database: string;
  // Authentication
  privateKeyPath: string;
  hasPassphrase: boolean;
  // Deploy tracking
  trackingSchema: string;
  trackingTable: string;
}

const DEFAULTS: FrostConfigData = {
  objectsFolder: "objects",
  dataFolder: "data",
  dataSchema: "PUBLIC",
  account: "",
  user: "",
  role: "SYSADMIN",
  warehouse: "COMPUTE_WH",
  database: "",
  privateKeyPath: "",
  hasPassphrase: false,
  trackingSchema: "FROST",
  trackingTable: "DEPLOY_HISTORY",
};

/** Map from YAML key (kebab-case) → FrostConfigData field name. */
const KEY_MAP: Record<string, keyof FrostConfigData> = {
  "objects-folder": "objectsFolder",
  "objects_folder": "objectsFolder",
  "data-folder": "dataFolder",
  "data_folder": "dataFolder",
  "data-schema": "dataSchema",
  "data_schema": "dataSchema",
  "account": "account",
  "user": "user",
  "role": "role",
  "warehouse": "warehouse",
  "database": "database",
  "private-key-path": "privateKeyPath",
  "private_key_path": "privateKeyPath",
  "private-key-passphrase": "hasPassphrase",
  "private_key_passphrase": "hasPassphrase",
  "tracking-schema": "trackingSchema",
  "tracking_schema": "trackingSchema",
  "tracking-table": "trackingTable",
  "tracking_table": "trackingTable",
};

/** Env-var overrides for each config field (shown as hints for null values). */
const ENV_HINTS: Partial<Record<keyof FrostConfigData, string>> = {
  account: "SNOWFLAKE_ACCOUNT",
  user: "SNOWFLAKE_USER",
  role: "SNOWFLAKE_ROLE",
  warehouse: "SNOWFLAKE_WAREHOUSE",
  database: "SNOWFLAKE_DATABASE",
  privateKeyPath: "SNOWFLAKE_PRIVATE_KEY_PATH",
};

export class ConfigReader {
  private _data: FrostConfigData | undefined;
  private _error: string | undefined;

  constructor(private readonly cwd: string) {}

  /** Resolve the config file path using the frost.configPath setting. */
  get configPath(): string {
    const name = vscode.workspace
      .getConfiguration("frost")
      .get<string>("configPath", "frost-config.yml");
    return path.resolve(this.cwd, name);
  }

  /** Returns cached config or re-reads from disk. */
  get data(): FrostConfigData | undefined {
    if (!this._data) {
      this.reload();
    }
    return this._data;
  }

  /** Returns the parse error, if any. */
  get error(): string | undefined {
    if (!this._data && !this._error) {
      this.reload();
    }
    return this._error;
  }

  /** Force re-read from disk. */
  reload(): void {
    this._data = undefined;
    this._error = undefined;

    const cfgPath = this.configPath;
    if (!fs.existsSync(cfgPath)) {
      this._error = "frost-config.yml not found";
      return;
    }

    try {
      const content = fs.readFileSync(cfgPath, "utf-8");
      this._data = this.parse(content);
    } catch (err: any) {
      this._error = `Could not parse config: ${err.message}`;
    }
  }

  /** Invalidate the cache so the next read re-parses. */
  invalidate(): void {
    this._data = undefined;
    this._error = undefined;
  }

  /** Get the env-var hint for a config field, if any. */
  envHint(field: keyof FrostConfigData): string | undefined {
    return ENV_HINTS[field];
  }

  // ── parsing ──────────────────────────────────────────

  private parse(content: string): FrostConfigData {
    const data: FrostConfigData = { ...DEFAULTS };
    const lines = content.split("\n");

    for (const line of lines) {
      // Skip comments and blank lines
      if (/^\s*#/.test(line) || /^\s*$/.test(line)) {
        continue;
      }
      // Only top-level keys (no leading whitespace)
      const match = line.match(/^([a-z][a-z0-9_-]*)\s*:\s*(.*)$/i);
      if (!match) {
        continue;
      }
      const yamlKey = match[1].toLowerCase();
      const rawValue = match[2].trim();
      const field = KEY_MAP[yamlKey];
      if (!field) {
        continue;
      }

      // Strip quotes
      let value = rawValue;
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }

      // Handle null
      if (value === "null" || value === "~" || value === "") {
        continue; // keep default
      }

      // Special case: passphrase is a boolean flag
      if (field === "hasPassphrase") {
        data.hasPassphrase = true;
        continue;
      }

      (data as any)[field] = value;
    }

    return data;
  }
}
