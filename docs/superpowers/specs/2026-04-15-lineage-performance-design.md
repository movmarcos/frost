# Lineage Performance — Focused, On-Demand Subgraphs

**Date:** 2026-04-15
**Status:** Design approved, ready for implementation plan
**Scope:** Phase 1 only. Phase 2 (disk cache) and a potential Rust/PyO3 hot-path
optimisation are designed-around but not built here.

## 1. Problem Statement & Goals

### Problem

In projects with 1000+ managed objects (our reference workspace has 1700+), the
Frost VSCode extension consumes excessive memory and crashes. Two cooperating
causes:

1. **Background scan memory pressure.** The Objects tree's activation flow runs
   `frost graph --json`. Today this parses the whole project **and**
   unconditionally runs `LineageScanner.scan()` over every procedure / function
   / task / stream body (~10–30 s total, ~200–500 MB Python heap) even though
   the graph command only needs dependency edges — the lineage scan is purely
   waste on this code path. The UI stays interactive thanks to the existing
   "Loading…" placeholder, but the parsed graph and lineage entries live in
   memory alongside everything else.
2. **Lineage render cost.** The "Lineage (local)" command renders *every* node
   and edge into a single D3 SVG embedded in a VSCode webview. Electron /
   Chromium cannot render a 1700-node SVG without exhausting memory.

The actual crash happens in the webview (phase 3 below) once (1) has already
inflated the process's memory baseline:

| Phase | Work | Rough cost at 1700 objects |
|---|---|---|
| 1. Python scan | Read SQL files, run regexes, build graph | 10–30 s, 200–500 MB Python heap |
| 2. HTML generation | Serialise all nodes/edges/columns into HTML | 5–50 MB output |
| 3. Webview render | D3 lays out and draws the SVG | **Crashes** |

### Goals (Phase 1)

- The Lineage panel no longer crashes the extension on a 1700-object workspace.
  (This is the primary goal.)
- The extension's UI (sidebars, commands) stays interactive during activation.
  The existing "Loading…" placeholder in the Objects tree is preserved; the
  underlying scan cost is unchanged in Phase 1 and is addressed in Phase 2.
- Opening the Lineage panel shows an object picker in < 500 ms.
- Selecting an object with default depth returns and renders its subgraph in
  < 5 s (on a cold Python scan; Phase 2 will make subsequent calls fast).
- The default lineage UX is **focused**: pick an object, see its neighbourhood
  (upstream + downstream to a configurable depth).
- A "Show full graph" view remains available but is **opt-in** with a size
  warning for workspaces with more than 300 objects.
- No change to the pip-install / CI-CD story. Pure Python + TypeScript changes.
- Existing `frost lineage --local [--output]` CLI behaviour is preserved
  byte-for-byte for external consumers.

### Non-Goals

- Disk-cached incremental scanning. *(Deferred to Phase 2.)*
- Rust / PyO3 rewrite. *(Deferred, conditional on post-Phase-2 benchmarks.)*
- Changes to the remote-Snowflake lineage path (`frost lineage` without
  `--local`).
- Column-level lineage.
- UI changes outside the lineage panel.

### Why Rust Is Deferred

Rust would accelerate the Python scan (phase 1) by ~10–50× and cut its memory
~5×. But the scan is not what crashes — the webview render is. Rust cannot help
DOM/SVG rendering inside Electron. A full port also breaks the single `pip
install` deployment story for CI/CD. The pragmatic sequence is:

1. Fix the actual crash by not shipping a 1700-node graph to the webview
   (this spec).
2. Amortise scan cost with a disk cache (Phase 2).
3. *If and only if* profiling then shows the regex/parse loop is still the
   dominant cost, introduce a narrow PyO3 module via `maturin`, preserving the
   `pip install` story.

## 2. Architecture

Three coordinated, independently testable changes.

### 2a. Python CLI — New Focused Endpoint

Add a subgraph mode to the existing `lineage` command:

```
frost lineage --local --subgraph <FQN> --depth <N> --direction <up|down|both> --json
```

- Reuses the existing `Deployer._scan_and_parse()` + `DependencyGraph` — no
  rewrite of parsing logic.
- After the full scan, traverses outward from `FQN` using the graph's existing
  `get_dependencies()` / `get_dependents()` BFS methods, bounded by `--depth`.
- Emits **only the nodes and edges inside the subgraph** as JSON (never HTML in
  this mode).
- `--direction` controls whether traversal is upstream, downstream, or both.

The existing `frost lineage --local` (full-graph HTML) path remains unchanged
for scripting users. It gains a new `--json` option so the extension can fetch
the same data as JSON instead of pre-baked HTML.

The scan is still full-project because knowing "who depends on X" requires the
complete reverse-dependency graph. The win is that we only **ship** the
subgraph to the extension. Phase 2 will make the scan itself incremental; the
CLI contract will not change.

### 2b. Extension — New Lineage Panel UX

Replace the current "dump full HTML into webview" flow with a proper
`WebviewView` that:

1. **On open**, shows an **object picker** — a searchable list populated from
   the already-loaded Objects tree. No Python call required.
2. **On pick**, calls `frost lineage --local --subgraph FQN --depth 1 --json`,
   receives a small JSON payload, renders the subgraph with D3 in the webview.
3. **In-panel controls:** depth slider (1–5), direction toggle
   (upstream / downstream / both), "Expand neighbours" button on each node.
4. **"Show full graph" button** (top-right). On click, shows a modal warning:
   *"This project has N objects; rendering the full graph may be slow. Continue?"*
   Only after explicit confirmation does the extension fetch the full graph.
   The warning is shown whenever N > 300.

The D3 rendering code is ported from `visualizer.py`'s embedded JavaScript into
a static webview asset. Same node/edge shape; same visual design.

### 2c. Extension Activation — Remove Lineage Work from `graph --json`

Today `extension.ts:358-366` runs `objectsProvider.refresh()` on activation,
which calls `frost graph --json`. That in turn calls `Deployer._build_graph()`,
which unconditionally runs `LineageScanner.scan()` over every procedure /
function / task / stream body — even though the graph command only needs
dependency edges. On a 1700-object workspace this is several seconds and tens
of megabytes of waste per activation.

Changes in this spec:

- `Deployer._build_graph()` gains an `include_lineage: bool = True` keyword
  argument. Default `True` preserves behaviour for `deploy`, `plan`, and
  `lineage` callers.
- `_cmd_graph` calls `_build_graph(include_lineage=False)`. `_cmd_plan`,
  `_cmd_deploy`, and `_cmd_lineage` continue to use the default.
- Integration test: assert that running `_cmd_graph` does not invoke
  `LineageScanner.scan()`. This locks the behaviour in so no future refactor
  can accidentally push lineage work back onto the activation hot path.
- Documentation in `_cmd_graph`'s docstring and in `extension.ts` comments
  states the invariant.

This is a modest but real activation-time win, and it is orthogonal to the
Phase 2 disk cache (which will further reduce the remaining parse cost).

### Data Flow — Before vs After

**Before:**

```
click Lineage
  → Python scans all SQL (10–30 s)
  → emit 5–50 MB HTML with full graph
  → webview loads & D3 renders 1700 nodes
  → CRASH
```

**After (Phase 1):**

```
click Lineage
  → picker appears instantly (data from Objects tree cache)
  → user picks object
  → Python scans all SQL (10–30 s; will become fast in Phase 2)
  → extract subgraph (BFS, ~20 nodes)
  → emit small JSON to webview
  → D3 renders a small, responsive subgraph
```

## 3. Components, Interfaces & Error Handling

### Python — CLI Contract

**Input (CLI args):**

- `--subgraph FQN` — required when this mode is used. Same FQN format as the
  graph (case-insensitive match against `DependencyGraph` keys).
- `--depth N` — integer ≥ 1, default 1, capped at 10.
- `--direction {up,down,both}` — default `both`.
- `--json` — required; non-JSON output is not supported for subgraph mode.

**Output JSON schema:**

```json
{
  "focus": "SCHEMA.ORDERS_VIEW",
  "depth": 1,
  "direction": "both",
  "nodes": [
    {
      "fqn": "...",
      "object_type": "VIEW",
      "file_path": "...",
      "columns": [{"name": "...", "type": "..."}]
    }
  ],
  "edges": [
    {
      "source": "...",
      "target": "...",
      "type": "dependency|reads|writes",
      "object_type": "..."
    }
  ],
  "truncated": false
}
```

`truncated: true` is set when the BFS hit the depth cap while neighbours still
remained, letting the UI hint "there's more, expand depth".

**Error cases:**

- FQN not found → exit code 2, stdout JSON
  `{"error": "object not found", "fqn": "..."}`. The extension surfaces this
  as a warning toast; the picker stays open.
- Parse errors in unrelated files → logged, do not fail the subgraph request
  (matches the existing `PolicyError` swallow in `_cmd_lineage`).
- Config / connection errors → exit code 1, stdout JSON `{"error": "..."}`.

### Python — Implementation Locations

- **`src/frost/graph.py`** — add `extract_subgraph(graph, fqn, depth, direction)`.
  BFS over the existing `_deps` / `_rdeps` / `_lineage` structures. Returns
  `(nodes, edges, truncated)`. All graph logic stays in `graph.py`.
- **`src/frost/visualizer.py`** — add `nodes_and_edges_as_json(nodes, edges,
  focus, depth, direction, truncated)`. Same shape used for both subgraph and
  full-graph JSON output. `generate_html(...)` stays in place for CLI-driven
  HTML output.
- **`src/frost/cli.py::_cmd_lineage`** — add a new
  `if args.subgraph:` branch before the existing `if local:` branch. Full-graph
  `--json` is a separate branch that emits the same schema with no
  `focus`/`depth`/`direction` fields (nulls) and `truncated=false`.

### Extension — Components

- **`vscode-frost/src/lineagePanel.ts`** — rewritten. Owns the webview,
  renders picker + graph, handles `postMessage` traffic to and from the webview
  script.
- **`vscode-frost/src/frostRunner.ts`** — add typed helpers:
  - `lineageSubgraph(fqn: string, depth: number, direction: "up"|"down"|"both"): Promise<SubgraphPayload>`
  - `lineageFullJson(): Promise<SubgraphPayload>`
- **`vscode-frost/media/lineage/`** — new static assets: `index.html`,
  `lineage.js`, `lineage.css`. Loaded via `asWebviewUri` once at panel open;
  data arrives via `postMessage`. This avoids re-parsing megabytes of HTML on
  every open.

### Extension — Error Handling

- Picker shows a disabled state + error banner if `frostRunner` rejects.
- Webview shows an error card inside the panel shell (same style as today's
  `errorHtml`), rather than replacing the panel — so the user can try another
  object without reopening.
- The full-graph modal always shows the current object count (fetched from the
  Objects tree cache; no Python call needed), producing a specific warning:
  *"1723 objects — may take several seconds and use significant memory."*

### Testing

**Python:**

- Unit tests for `extract_subgraph`: `depth=1`, `depth=N`, each direction,
  unknown FQN, empty graph, cycle.
- CLI integration test: run `frost lineage --local --subgraph FQN --json`
  against `tests/` fixtures and assert the shape.
- Activation-safety test: assert no lineage code path is imported or invoked
  by `frost graph --json`.

**Extension:**

- Message-protocol tests between `LineagePanel` and the webview script (mock
  `FrostRunner`).
- Manual test checklist added to the extension README covering a 1700-object
  fixture.

## 4. Rollout, Compatibility & Phase 2 Seam

### Backwards Compatibility

- **CLI.** Existing `frost lineage --local [--output path.html]` is preserved
  byte-for-byte. New behaviour is opt-in via `--subgraph` or `--json`.
- **Extension.** The command id `frost.lineageLocal` keeps its name and remains
  the only user-facing entry point. The internal implementation changes; the
  user-visible name does not.
- **Python library API.** `extract_subgraph` and `nodes_and_edges_as_json` are
  additive. No removals.
- **Packaging.** New webview assets under `vscode-frost/media/lineage/` must be
  declared in the extension's `package.json` so they ship in the `.vsix`.

### Rollout

Single change-set ships Phase 1. No feature flag, no dual code path — the old
"dump full HTML" path is replaced because its current behaviour on large
projects is a crash, so there is no regression surface to preserve. Users with
small projects get the same content, reached via picker → depth=1 → "Show full
graph" in two clicks instead of one. We accept this minor UX regression in
exchange for the memory fix.

### Phase 2 Seam (designed now, built later)

The Phase 1 Python contract is deliberately shaped so Phase 2 can slot in
**without extension changes**:

- Phase 2 will add a `LineageCache` class in `src/frost/lineage.py`, backed by
  a SQLite file at `.frost-cache/lineage.db` (git-ignored). Schema: one row per
  SQL file with its sha256, parsed `ObjectDefinition`, and `LineageEntry`. A
  reverse index keyed by referenced FQN enables fast downstream queries.
- `Deployer._scan_and_parse()` and `LineageScanner.scan()` become cache-aware:
  on startup, load cached rows; for each SQL file, recompute only if its hash
  changed.
- The CLI and JSON contract are identical to Phase 1. The extension benefits
  automatically.
- Rust/PyO3 is re-evaluated only after Phase 2 lands, using benchmarks on a
  warm cache. If the regex/parse stage is still >1 s for 1700 files, a narrow
  PyO3 extension module replaces the hot loop, shipped via `maturin` so the
  `pip install` story is preserved.

### Out of Scope (Explicit)

- Remote-lineage mode (`frost lineage` without `--local`).
- Column-level lineage.
- Changes to `deploy` / `plan` / `graph` commands other than the single new
  `--json` option on `lineage`.
- UI work outside the lineage panel.

### Success Criteria

1. On a 1700-object workspace, opening the Lineage panel no longer crashes the
   extension.
2. Opening the Lineage panel shows the picker in < 500 ms (picker is populated
   from the Objects tree cache; no Python call).
3. Selecting an object with default depth (1) on a cold Python scan returns
   and renders the subgraph in < 5 s.
4. The "Show full graph" warning is shown for any workspace with > 300 objects
   and only proceeds after explicit confirmation.
5. The activation-safety test passes: running `_cmd_graph` does not invoke
   `LineageScanner.scan()`. `_build_graph(include_lineage=False)` returns a
   graph with zero lineage entries.
6. Existing unit/integration tests keep passing; new tests cover
   `extract_subgraph` and the subgraph CLI branch.
