/**
 * FrostRunner – executes frost CLI commands and parses JSON output.
 */

import * as vscode from "vscode";
import { execFile, execFileSync } from "child_process";
import * as path from "path";
import * as fs from "fs";

/** A single Snowflake object from `frost graph --json`. */
export interface FrostNode {
  fqn: string;
  object_type: string;
  file_path: string;
  schema: string;
  name: string;
  columns: { name: string; type: string }[];
  dependencies: string[];
}

/** A single data file from `frost load --json`. */
export interface DataFileInfo {
  fqn: string;
  file_path: string;
  table_name: string;
  schema: string;
  columns: string[];
  column_types: Record<string, string>;
  row_count: number;
  checksum: string;
}

/** Full output of `frost load --json`. */
export interface LoadPayload {
  files: DataFileInfo[];
}

/** Full output of `frost graph --json`. */
export interface GraphPayload {
  nodes: FrostNode[];
  edges: {
    source: string;
    target: string;
    type: string;
    object_type: string;
  }[];
  node_types: Record<string, string>;
  node_columns: Record<string, { name: string; type: string }[]>;
}

/**
 * Recursively search `dir` (up to `maxDepth` levels) for a file named `name`.
 * Returns the full path to the first match, or undefined.
 */
function findFileUp(dir: string, name: string, maxDepth: number): string | undefined {
  // Check current directory
  const candidate = path.join(dir, name);
  if (fs.existsSync(candidate)) {
    return candidate;
  }
  if (maxDepth <= 0) {
    return undefined;
  }
  // Recurse into subdirectories
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory() && !entry.name.startsWith(".") && entry.name !== "node_modules") {
        const found = findFileUp(path.join(dir, entry.name), name, maxDepth - 1);
        if (found) { return found; }
      }
    }
  } catch {
    // permission error etc.
  }
  return undefined;
}

/**
 * Try a list of Python candidates and return the first one
 * that can successfully `import frost`.
 */
function detectPython(cwd: string): string {
  const candidates = [
    // 1. User-configured value
    vscode.workspace.getConfiguration("frost").get<string>("pythonPath", ""),
    // 2. VS Code Python extension's interpreter
    vscode.workspace.getConfiguration("python").get<string>("defaultInterpreterPath", ""),
    // 3. Common paths
    "python3",
    "/opt/homebrew/bin/python3",
    "/opt/homebrew/bin/python3.11",
    "/opt/homebrew/bin/python3.12",
    "/opt/homebrew/bin/python3.13",
    "/usr/local/bin/python3",
    "/usr/bin/python3",
    "python",
  ];

  for (const candidate of candidates) {
    if (!candidate) { continue; }
    try {
      execFileSync(candidate, ["-c", "import frost"], {
        cwd,
        timeout: 5000,
        stdio: "pipe",
      });
      return candidate; // frost importable – use this one
    } catch {
      // try next
    }
  }
  // fallback – let it fail with a clear error later
  return "python3";
}

export class FrostRunner {
  // ── settings helpers ──────────────────────────────────

  /** Resolved Python path (cached after first successful detection). */
  private _pythonPath: string | undefined;

  /** Resolved project root (directory containing frost-config.yml). */
  private _projectRoot: string | undefined;

  private get pythonPath(): string {
    if (!this._pythonPath) {
      this._pythonPath = detectPython(this.cwd);
    }
    return this._pythonPath;
  }

  /** Force re-detection (e.g. after settings change). */
  resetPython(): void {
    this._pythonPath = undefined;
  }

  private get configPath(): string {
    return vscode.workspace
      .getConfiguration("frost")
      .get<string>("configPath", "frost-config.yml");
  }

  /**
   * Working directory for frost commands.
   * Auto-discovers the folder containing frost-config.yml
   * by searching the workspace recursively.
   */
  get cwd(): string {
    if (this._projectRoot) {
      return this._projectRoot;
    }

    const wsRoot =
      vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd();

    // 1. Check explicit configPath (may be relative to workspace root)
    const explicitConfig = path.resolve(wsRoot, this.configPath);
    if (fs.existsSync(explicitConfig)) {
      this._projectRoot = path.dirname(explicitConfig);
      return this._projectRoot;
    }

    // 2. Search common locations for frost-config.yml
    const configName = path.basename(this.configPath);
    const found = findFileUp(wsRoot, configName, 4);
    if (found) {
      this._projectRoot = path.dirname(found);
      return this._projectRoot;
    }

    // 3. Fallback to workspace root
    this._projectRoot = wsRoot;
    return this._projectRoot;
  }

  /** Reset cached project root (e.g. after settings change). */
  resetProjectRoot(): void {
    this._projectRoot = undefined;
  }

  // ── execute frost and capture stdout ──────────────────

  exec(args: string): Promise<string> {
    return new Promise((resolve, reject) => {
      const cmd = this.pythonPath;
      const fullArgs = [
        "-m",
        "frost",
        "-c",
        this.configPath,
        ...args.split(/\s+/),
      ];
      execFile(
        cmd,
        fullArgs,
        { cwd: this.cwd, maxBuffer: 10 * 1024 * 1024, timeout: 60_000 },
        (err, stdout, stderr) => {
          if (err) {
            reject(new Error(stderr || err.message));
          } else {
            resolve(stdout);
          }
        }
      );
    });
  }

  /** Run a frost command in the integrated terminal (visible to user). */
  runInTerminal(subCommand: string): void {
    const terminal =
      vscode.window.terminals.find((t) => t.name === "Frost") ??
      vscode.window.createTerminal({ name: "Frost", cwd: this.cwd });
    terminal.show();
    terminal.sendText(
      `cd ${this.cwd} && ${this.pythonPath} -m frost -c ${this.configPath} ${subCommand}`
    );
  }

  // ── typed helpers ─────────────────────────────────────

  /**
   * Extract the first valid JSON object/array from a string
   * that may contain leading log lines.
   */
  private extractJson(raw: string): string {
    // Find the first '{' or '[' which starts JSON
    const objIdx = raw.indexOf("{");
    const arrIdx = raw.indexOf("[");
    let start = -1;
    if (objIdx >= 0 && arrIdx >= 0) {
      start = Math.min(objIdx, arrIdx);
    } else if (objIdx >= 0) {
      start = objIdx;
    } else if (arrIdx >= 0) {
      start = arrIdx;
    }
    if (start < 0) {
      throw new Error("No JSON found in output: " + raw.slice(0, 200));
    }
    return raw.slice(start);
  }

  async graphJson(): Promise<GraphPayload> {
    const raw = await this.exec("graph --json");
    return JSON.parse(this.extractJson(raw)) as GraphPayload;
  }

  async planJson(): Promise<{ objects: FrostNode[]; total: number }> {
    const raw = await this.exec("plan --json");
    return JSON.parse(this.extractJson(raw));
  }

  /** Generate lineage HTML and return its contents. */
  async lineageHtml(): Promise<string> {
    // Generate to a temp file, read it, return content.
    const tmpPath = path.join(this.cwd, ".frost-lineage-tmp.html");
    await this.exec(`lineage --local --output ${tmpPath}`);
    const uri = vscode.Uri.file(tmpPath);
    const bytes = await vscode.workspace.fs.readFile(uri);
    // Clean up
    try {
      await vscode.workspace.fs.delete(uri);
    } catch {
      /* ignore */
    }
    return Buffer.from(bytes).toString("utf-8");
  }

  async loadJson(): Promise<LoadPayload> {
    const raw = await this.exec("load --dry-run --json");
    return JSON.parse(this.extractJson(raw)) as LoadPayload;
  }
}
