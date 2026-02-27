/**
 * FrostRunner – executes frost CLI commands and parses JSON output.
 */

import * as vscode from "vscode";
import { execFile, execFileSync } from "child_process";
import * as path from "path";

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

  private get cwd(): string {
    return (
      vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd()
    );
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
      vscode.window.createTerminal("Frost");
    terminal.show();
    terminal.sendText(
      `${this.pythonPath} -m frost -c ${this.configPath} ${subCommand}`
    );
  }

  // ── typed helpers ─────────────────────────────────────

  async graphJson(): Promise<GraphPayload> {
    const raw = await this.exec("graph --json");
    return JSON.parse(raw) as GraphPayload;
  }

  async planJson(): Promise<{ objects: FrostNode[]; total: number }> {
    const raw = await this.exec("plan --json");
    return JSON.parse(raw);
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
}
