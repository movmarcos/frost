/**
 * FrostDiagnostics – runs `frost plan --json` in the background and
 * maps any errors / warnings to VS Code DiagnosticCollection so they
 * appear as inline squiggles and in the Problems panel.
 */

import * as vscode from "vscode";
import { FrostRunner } from "./frostRunner";

export class FrostDiagnostics {
  private collection: vscode.DiagnosticCollection;

  constructor(private runner: FrostRunner) {
    this.collection =
      vscode.languages.createDiagnosticCollection("frost");
  }

  async run(): Promise<void> {
    this.collection.clear();

    try {
      const payload = await this.runner.planJson();
      // Currently `frost plan --json` succeeds – no errors.
      // Future: parse errors from deployer and map to file/line.
      // For now we validate that every object has a valid file path.
      const root =
        vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? "";

      for (const obj of payload.objects) {
        if (!obj.file_path) {
          continue;
        }
        // Check for circular dependency hints
        if (obj.dependencies.includes(obj.fqn)) {
          const uri = vscode.Uri.file(
            obj.file_path.startsWith("/")
              ? obj.file_path
              : `${root}/${obj.file_path}`
          );
          const diag = new vscode.Diagnostic(
            new vscode.Range(0, 0, 0, 0),
            `Object ${obj.fqn} depends on itself`,
            vscode.DiagnosticSeverity.Error
          );
          diag.source = "frost";
          this.collection.set(uri, [diag]);
        }

        // Check for missing dependencies (referenced but not in the plan)
        const allFqns = new Set(payload.objects.map((o) => o.fqn));
        for (const dep of obj.dependencies) {
          if (!allFqns.has(dep)) {
            const uri = vscode.Uri.file(
              obj.file_path.startsWith("/")
                ? obj.file_path
                : `${root}/${obj.file_path}`
            );
            const existing = this.collection.get(uri) ?? [];
            const diag = new vscode.Diagnostic(
              new vscode.Range(0, 0, 0, 0),
              `Dependency "${dep}" not found in project – will be treated as EXTERNAL`,
              vscode.DiagnosticSeverity.Warning
            );
            diag.source = "frost";
            this.collection.set(uri, [...existing, diag]);
          }
        }
      }
    } catch {
      // Plan failed — frost itself shows errors in the terminal.
    }
  }
}
