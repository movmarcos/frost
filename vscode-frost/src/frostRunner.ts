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

/** A Streamlit app from `frost streamlit list --json`. */
export interface StreamlitAppInfo {
  name: string;
  directory: string;
  main_file: string;
  stage: string;
  schema: string;
  warehouse: string;
  query_warehouse: string;
  title: string;
  definition_file: string;
  comment: string;
  external_access_integrations: string[];
  imports: string[];
}

/** Payload from `frost streamlit list --json`. */
export interface StreamlitPayload {
  apps: StreamlitAppInfo[];
  snow_cli: string;
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

/** A node in a lineage subgraph (from `frost lineage --json`). */
export interface SubgraphNode {
  fqn: string;
  object_type: string;
  file_path: string;
  columns: { name: string; type: string }[];
}

/** An edge in a lineage subgraph. */
export interface SubgraphEdge {
  source: string;
  target: string;
  type: "dependency" | "reads" | "writes";
  object_type: string;
}

/** Response from `frost lineage --json`. */
export interface SubgraphPayload {
  focus: string | null;
  depth: number | null;
  direction: "up" | "down" | "both" | null;
  nodes: SubgraphNode[];
  edges: SubgraphEdge[];
  truncated: boolean;
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
 * Find the virtual environment's Python executable in common locations
 * relative to the workspace root.
 */
function findVenvPython(wsRoot: string): string[] {
  const venvDirs = [".venv", "venv", "env", ".env"];
  // Windows and Unix bin locations
  const binPaths = [
    path.join("Scripts", "python.exe"),   // Windows venv
    path.join("Scripts", "python"),        // Windows
    path.join("bin", "python3"),           // Unix venv
    path.join("bin", "python"),            // Unix venv
  ];

  const results: string[] = [];
  for (const vdir of venvDirs) {
    for (const bin of binPaths) {
      const candidate = path.join(wsRoot, vdir, bin);
      if (fs.existsSync(candidate)) {
        results.push(candidate);
      }
    }
  }
  return results;
}

/**
 * Try a list of Python candidates and return the first one
 * that can successfully `import frost`.
 * If a Python is found but frost is not installed, returns it
 * and sets `needsInstall` on the result.
 */
interface PythonDetection {
  pythonPath: string;
  frostInstalled: boolean;
}

function detectPython(cwd: string): PythonDetection {
  const wsRoot =
    vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? cwd;

  // Discover venv Pythons from workspace
  const venvPythons = findVenvPython(wsRoot);
  // Also check if the cwd itself has a venv (e.g. examples/ subfolder)
  if (cwd !== wsRoot) {
    venvPythons.push(...findVenvPython(cwd));
  }

  const candidates = [
    // 1. User-configured value (highest priority)
    vscode.workspace.getConfiguration("frost").get<string>("pythonPath", ""),
    // 2. VS Code Python extension's selected interpreter
    vscode.workspace.getConfiguration("python").get<string>("defaultInterpreterPath", ""),
    // 3. Virtual environment Pythons discovered in workspace
    ...venvPythons,
    // 4. Common system paths
    "python3",
    "/opt/homebrew/bin/python3",
    "/opt/homebrew/bin/python3.11",
    "/opt/homebrew/bin/python3.12",
    "/opt/homebrew/bin/python3.13",
    "/usr/local/bin/python3",
    "/usr/bin/python3",
    "python",
  ];

  let firstWorkingPython: string | undefined;

  for (const candidate of candidates) {
    if (!candidate) { continue; }
    try {
      execFileSync(candidate, ["-c", "import frost"], {
        cwd,
        timeout: 5000,
        stdio: "pipe",
      });
      return { pythonPath: candidate, frostInstalled: true };
    } catch {
      // Check if Python itself works (just not frost)
      if (!firstWorkingPython) {
        try {
          execFileSync(candidate, ["-c", "print(1)"], {
            cwd,
            timeout: 5000,
            stdio: "pipe",
          });
          firstWorkingPython = candidate;
        } catch {
          // Python not available, try next
        }
      }
    }
  }

  // Python found but frost not installed
  if (firstWorkingPython) {
    return { pythonPath: firstWorkingPython, frostInstalled: false };
  }
  // No Python found at all
  return { pythonPath: "python3", frostInstalled: false };
}

export class FrostRunner {
  // ── settings helpers ──────────────────────────────────

  /** Resolved Python path (cached after first successful detection). */
  private _pythonPath: string | undefined;
  private _frostInstalled: boolean = false;

  /** Resolved project root (directory containing frost-config.yml). */
  private _projectRoot: string | undefined;

  private get pythonPath(): string {
    if (!this._pythonPath) {
      const result = detectPython(this.cwd);
      this._pythonPath = result.pythonPath;
      this._frostInstalled = result.frostInstalled;
    }
    return this._pythonPath;
  }

  /** Force re-detection (e.g. after settings change). */
  resetPython(): void {
    this._pythonPath = undefined;
    this._frostInstalled = false;
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

  /**
   * Check if frost-ddl is installed and auto-install it if not.
   * Looks for pyproject.toml in the workspace to run `pip install -e .`.
   * Returns true if frost is (now) available.
   */
  async ensureFrostInstalled(): Promise<boolean> {
    // Trigger detection
    const _ = this.pythonPath;

    if (this._frostInstalled) {
      return true;
    }

    // Find pyproject.toml (the frost-ddl package root)
    const wsRoot =
      vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.cwd();
    const pyproject = findFileUp(wsRoot, "pyproject.toml", 4);

    if (!pyproject) {
      vscode.window.showErrorMessage(
        "Frost: frost-ddl is not installed and pyproject.toml was not found. " +
        "Run `pip install frost-ddl` manually."
      );
      return false;
    }

    const pkgDir = path.dirname(pyproject);
    const answer = await vscode.window.showInformationMessage(
      "Frost: frost-ddl is not installed. Install it now?",
      "Install",
      "Cancel"
    );

    if (answer !== "Install") {
      return false;
    }

    // Run pip install -e . in a visible terminal
    const terminal =
      vscode.window.terminals.find((t) => t.name === "Frost Setup") ??
      vscode.window.createTerminal({ name: "Frost Setup", cwd: pkgDir });
    terminal.show();
    terminal.sendText(`"${this._pythonPath}" -m pip install -e "${pkgDir}"`);

    // Wait for installation then verify
    const installed = await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: "Installing frost-ddl…",
        cancellable: false,
      },
      async () => {
        // Poll until frost is importable (up to 60s)
        for (let i = 0; i < 30; i++) {
          await new Promise((r) => setTimeout(r, 2000));
          try {
            execFileSync(this._pythonPath!, ["-c", "import frost"], {
              cwd: pkgDir,
              timeout: 5000,
              stdio: "pipe",
            });
            return true;
          } catch {
            // still installing
          }
        }
        return false;
      }
    );

    if (installed) {
      this._frostInstalled = true;
      vscode.window.showInformationMessage("frost-ddl installed successfully ✅");
      return true;
    } else {
      vscode.window.showErrorMessage(
        "frost-ddl installation timed out. Check the Frost Setup terminal for details."
      );
      return false;
    }
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
        { cwd: this.cwd, maxBuffer: 10 * 1024 * 1024, timeout: 180_000 },
        (err, stdout, stderr) => {
          if (err) {
            const wrapped = new Error(stderr || err.message);
            // Preserve stdout so callers (e.g. JSON-mode subcommands that
            // emit {"error": "..."} on stdout with a non-zero exit) can
            // surface a structured error instead of the generic exec failure.
            (wrapped as Error & { stdout?: string }).stdout = stdout;
            reject(wrapped);
          } else {
            resolve(stdout);
          }
        }
      );
    });
  }

  /** Run a frost command in the integrated terminal (visible to user). */
  runInTerminal(subCommand: string): void {
    // Always create a new terminal with the correct cwd to avoid
    // shell-specific issues (PowerShell does not support '&&').
    let terminal = vscode.window.terminals.find((t) => t.name === "Frost");
    if (!terminal) {
      terminal = vscode.window.createTerminal({ name: "Frost", cwd: this.cwd });
    }
    terminal.show();
    // Quote the path for Windows paths with spaces
    const py = this.pythonPath.includes(" ")
      ? `"${this.pythonPath}"`
      : this.pythonPath;
    terminal.sendText(
      `cd "${this.cwd}"`
    );
    terminal.sendText(
      `${py} -m frost -c ${this.configPath} ${subCommand}`
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

  /** Fetch a focused subgraph around a single object. */
  async lineageSubgraph(
    fqn: string,
    depth: number,
    direction: "up" | "down" | "both" = "both"
  ): Promise<SubgraphPayload> {
    const raw = await this.execLineageJson(
      `lineage --local --json --object ${fqn} --depth ${depth} --direction ${direction}`
    );
    return JSON.parse(this.extractJson(raw)) as SubgraphPayload;
  }

  /** Fetch the full lineage graph as JSON (opt-in, large). */
  async lineageFullJson(): Promise<SubgraphPayload> {
    const raw = await this.execLineageJson("lineage --local --json");
    return JSON.parse(this.extractJson(raw)) as SubgraphPayload;
  }

  /**
   * Run a `lineage --json` command, translating the CLI's structured
   * `{"error": "..."}` stdout response (emitted with a non-zero exit
   * code, e.g. for "object not found") into a clean Error message so
   * the panel can surface a specific hint instead of "command failed".
   */
  private async execLineageJson(args: string): Promise<string> {
    try {
      return await this.exec(args);
    } catch (err) {
      const stdout = (err as Error & { stdout?: string }).stdout ?? "";
      let structured: string | undefined;
      try {
        const parsed = JSON.parse(this.extractJson(stdout)) as { error?: string };
        if (parsed && typeof parsed.error === "string") {
          structured = parsed.error;
        }
      } catch {
        /* no parseable JSON on stdout; fall through to the original error */
      }
      if (structured !== undefined) {
        throw new Error(structured);
      }
      throw err;
    }
  }

  async loadJson(): Promise<LoadPayload> {
    const raw = await this.exec("load --dry-run --json");
    return JSON.parse(this.extractJson(raw)) as LoadPayload;
  }

  async streamlitJson(): Promise<StreamlitPayload> {
    const raw = await this.exec("streamlit list --json");
    return JSON.parse(this.extractJson(raw)) as StreamlitPayload;
  }
}
