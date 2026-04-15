# Lineage Performance (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the VSCode Lineage panel crash on 1700+ object workspaces by shipping focused, on-demand subgraphs to the webview instead of the full graph.

**Architecture:** Extend the existing `frost lineage --local` CLI with a JSON output mode and an object-scoped traversal that emits only the subgraph around a chosen FQN. Rewrite the VSCode lineage panel as a static webview shell that shows an object picker, calls the new CLI mode on selection, and renders small subgraphs with D3 via `postMessage`. A "Show full graph" button remains available behind a size warning for workspaces over 300 objects.

**Tech Stack:** Python 3.10+ (pytest, argparse), TypeScript (VSCode Extension API, D3.js v7 via CDN-embedded script in webview).

**Spec:** `docs/superpowers/specs/2026-04-15-lineage-performance-design.md`

**Refinement vs spec:** The spec proposed a new `--subgraph FQN` CLI flag. The existing `frost lineage` parser already carries `--object FQN` and `--depth N` (used today as focus hints for HTML). This plan reuses `--object` + `--depth` together with a new `--json` + `--direction` as the trigger for subgraph mode. This is purely a surface naming choice — behaviour, JSON schema, and all other aspects match the spec.

---

## File Structure

**Python (new + modified):**
- Modify: `src/frost/graph.py` — add `extract_subgraph` top-level function + `GraphSubset` dataclass.
- Modify: `src/frost/visualizer.py` — add `nodes_and_edges_as_json` helper.
- Modify: `src/frost/cli.py` — add `--json` and `--direction` args to `lineage`; new subgraph and full-JSON branches in `_cmd_lineage`.
- Create: `tests/test_graph_subgraph.py` — unit tests for `extract_subgraph`.
- Create: `tests/test_cli_lineage_json.py` — CLI integration tests.
- Create: `tests/test_activation_safety.py` — lockdown test that `graph --json` does not touch lineage.

**Extension (new + modified):**
- Modify: `vscode-frost/src/frostRunner.ts` — typed helpers `lineageSubgraph()`, `lineageFullJson()`; `SubgraphPayload` interface.
- Rewrite: `vscode-frost/src/lineagePanel.ts` — picker + WebviewView + postMessage protocol.
- Create: `vscode-frost/media/lineage/index.html` — static webview shell.
- Create: `vscode-frost/media/lineage/lineage.js` — D3 subgraph renderer + message handler.
- Create: `vscode-frost/media/lineage/lineage.css` — panel styles.
- Modify: `vscode-frost/package.json` — declare `media/` for vsix inclusion.
- Modify: `vscode-frost/README.md` — manual test checklist for large projects.

---

## Task 1: Add `extract_subgraph` to `graph.py`

**Files:**
- Modify: `src/frost/graph.py`
- Test: `tests/test_graph_subgraph.py` (new)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_graph_subgraph.py` with the full content below.

```python
"""Tests for frost.graph.extract_subgraph -- BFS traversal around a focus FQN."""

import pytest

from frost.graph import DependencyGraph, extract_subgraph
from frost.lineage import LineageEntry
from frost.parser import ObjectDefinition


def _obj(fqn: str, deps=None, obj_type: str = "TABLE") -> ObjectDefinition:
    parts = fqn.split(".")
    name = parts[-1]
    schema = parts[-2] if len(parts) >= 2 else None
    db = parts[-3] if len(parts) >= 3 else None
    return ObjectDefinition(
        file_path=f"{name.lower()}.sql",
        object_type=obj_type,
        database=db,
        schema=schema,
        name=name,
        raw_sql="",
        resolved_sql="",
        dependencies=set(deps or []),
    )


def _linear_graph():
    """A -> B -> C (A depends on B, B depends on C)."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A", deps=["PUBLIC.B"], obj_type="VIEW"))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.C"], obj_type="VIEW"))
    g.add_object(_obj("PUBLIC.C", obj_type="TABLE"))
    g.build()
    return g


def test_unknown_fqn_returns_none():
    g = _linear_graph()
    result = extract_subgraph(g, "PUBLIC.MISSING", depth=1, direction="both")
    assert result is None


def test_depth_zero_rejected():
    g = _linear_graph()
    with pytest.raises(ValueError):
        extract_subgraph(g, "PUBLIC.B", depth=0, direction="both")


def test_depth_one_both_directions():
    g = _linear_graph()
    result = extract_subgraph(g, "PUBLIC.B", depth=1, direction="both")
    assert result is not None
    fqns = {n["fqn"] for n in result.nodes}
    assert fqns == {"PUBLIC.A", "PUBLIC.B", "PUBLIC.C"}
    edge_pairs = {(e["source"], e["target"]) for e in result.edges}
    # DependencyGraph stores deps as "source depends on target" edges.
    assert ("PUBLIC.A", "PUBLIC.B") in edge_pairs
    assert ("PUBLIC.B", "PUBLIC.C") in edge_pairs
    assert result.truncated is False


def test_direction_up_only():
    """Upstream = traverse dependencies (what this object depends on)."""
    g = _linear_graph()
    result = extract_subgraph(g, "PUBLIC.A", depth=2, direction="up")
    fqns = {n["fqn"] for n in result.nodes}
    assert fqns == {"PUBLIC.A", "PUBLIC.B", "PUBLIC.C"}


def test_direction_down_only():
    """Downstream = traverse dependents (what depends on this object)."""
    g = _linear_graph()
    result = extract_subgraph(g, "PUBLIC.C", depth=2, direction="down")
    fqns = {n["fqn"] for n in result.nodes}
    assert fqns == {"PUBLIC.A", "PUBLIC.B", "PUBLIC.C"}


def test_truncated_flag_set_when_depth_limits_traversal():
    g = _linear_graph()
    # Focus on A, depth=1, direction=up -> reaches B but not C, and C is a
    # neighbour of B that we never enqueued, so truncated=True.
    result = extract_subgraph(g, "PUBLIC.A", depth=1, direction="up")
    fqns = {n["fqn"] for n in result.nodes}
    assert fqns == {"PUBLIC.A", "PUBLIC.B"}
    assert result.truncated is True


def test_includes_lineage_edges():
    """Lineage 'reads' and 'writes' entries should appear as subgraph edges."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.SRC", obj_type="TABLE"))
    g.add_object(_obj("PUBLIC.TGT", obj_type="TABLE"))
    g.add_object(_obj("PUBLIC.PROC", obj_type="PROCEDURE"))
    g.build()
    g.add_lineage(LineageEntry(
        object_fqn="PUBLIC.PROC",
        file_path="proc.sql",
        sources=["PUBLIC.SRC"],
        targets=["PUBLIC.TGT"],
    ))

    result = extract_subgraph(g, "PUBLIC.PROC", depth=1, direction="both")
    fqns = {n["fqn"] for n in result.nodes}
    assert fqns == {"PUBLIC.PROC", "PUBLIC.SRC", "PUBLIC.TGT"}
    edge_types = {(e["source"], e["target"], e["type"]) for e in result.edges}
    assert ("PUBLIC.PROC", "PUBLIC.SRC", "reads") in edge_types
    assert ("PUBLIC.PROC", "PUBLIC.TGT", "writes") in edge_types


def test_cycle_is_handled_without_infinite_loop():
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A", deps=["PUBLIC.B"]))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.build()
    result = extract_subgraph(g, "PUBLIC.A", depth=5, direction="both")
    fqns = {n["fqn"] for n in result.nodes}
    assert fqns == {"PUBLIC.A", "PUBLIC.B"}


def test_node_includes_type_and_file_path():
    g = _linear_graph()
    result = extract_subgraph(g, "PUBLIC.A", depth=0 + 1, direction="up")
    focus = next(n for n in result.nodes if n["fqn"] == "PUBLIC.A")
    assert focus["object_type"] == "VIEW"
    assert focus["file_path"] == "a.sql"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_graph_subgraph.py -v`
Expected: All tests fail with `ImportError: cannot import name 'extract_subgraph' from 'frost.graph'`.

- [ ] **Step 3: Implement `extract_subgraph` and `GraphSubset` in `graph.py`**

At the bottom of `src/frost/graph.py`, add:

```python
from dataclasses import dataclass


@dataclass
class GraphSubset:
    """A subset of a DependencyGraph centred on a focus FQN."""
    focus: str
    depth: int
    direction: str
    nodes: list[dict]
    edges: list[dict]
    truncated: bool


def extract_subgraph(
    graph: "DependencyGraph",
    focus_fqn: str,
    depth: int,
    direction: str,
) -> GraphSubset | None:
    """BFS outward from *focus_fqn* up to *depth* hops.

    Parameters
    ----------
    graph : DependencyGraph
        A built DependencyGraph (``graph.build()`` must have been called).
    focus_fqn : str
        Case-insensitive FQN of the focus object.
    depth : int
        Maximum number of hops (>= 1).
    direction : str
        One of "up" (dependencies / reads), "down" (dependents),
        or "both".

    Returns
    -------
    GraphSubset or None
        ``None`` if *focus_fqn* is not a managed object. Otherwise, a
        ``GraphSubset`` containing every node reached within *depth*
        hops and every edge among those nodes. ``truncated=True`` when
        the BFS stopped at the depth limit while unexplored neighbours
        remained.
    """
    if depth < 1:
        raise ValueError(f"depth must be >= 1, got {depth}")
    if direction not in ("up", "down", "both"):
        raise ValueError(
            f"direction must be 'up', 'down', or 'both', got {direction!r}"
        )

    focus = focus_fqn.upper()
    if focus not in graph._objects:
        return None

    go_up = direction in ("up", "both")
    go_down = direction in ("down", "both")

    # BFS
    visited: set[str] = {focus}
    frontier: list[str] = [focus]
    truncated = False
    for _ in range(depth):
        next_frontier: list[str] = []
        for fqn in frontier:
            neighbours: set[str] = set()
            if go_up:
                neighbours |= graph._deps.get(fqn, set())
                entry = graph._lineage.get(fqn)
                if entry:
                    neighbours |= set(entry.sources)
                    neighbours |= set(entry.targets)
            if go_down:
                neighbours |= graph._rdeps.get(fqn, set())
                # Downstream lineage: any object whose lineage entry
                # writes/reads *this* fqn.
                for other_fqn, other_entry in graph._lineage.items():
                    if fqn in other_entry.sources or fqn in other_entry.targets:
                        neighbours.add(other_fqn)
            for n in neighbours:
                if n not in visited:
                    visited.add(n)
                    next_frontier.append(n)
        frontier = next_frontier
        if not frontier:
            break

    # If BFS terminated because of the depth cap (not because it ran
    # out of neighbours), and there are still unexplored neighbours
    # beyond the frontier, flag truncated.
    if frontier:
        for fqn in frontier:
            extras: set[str] = set()
            if go_up:
                extras |= graph._deps.get(fqn, set())
            if go_down:
                extras |= graph._rdeps.get(fqn, set())
            if extras - visited:
                truncated = True
                break

    # Build node and edge payloads.
    nodes: list[dict] = []
    for fqn in sorted(visited):
        obj = graph._objects.get(fqn)
        if obj is not None:
            columns = [
                {"name": c["name"], "type": c["type"]}
                for c in (obj.columns or [])
            ]
            nodes.append({
                "fqn": fqn,
                "object_type": obj.object_type,
                "file_path": obj.file_path,
                "columns": columns,
            })
        else:
            # External object referenced via lineage.
            nodes.append({
                "fqn": fqn,
                "object_type": "EXTERNAL",
                "file_path": "",
                "columns": [],
            })

    edges: list[dict] = []
    for src in visited:
        obj_type = (
            graph._objects[src].object_type if src in graph._objects else "UNKNOWN"
        )
        # dependency edges
        for tgt in graph._deps.get(src, set()):
            if tgt in visited:
                edges.append({
                    "source": src, "target": tgt,
                    "type": "dependency", "object_type": obj_type,
                })
        # lineage edges
        entry = graph._lineage.get(src)
        if entry:
            for tgt in entry.sources:
                if tgt in visited:
                    edges.append({
                        "source": src, "target": tgt,
                        "type": "reads", "object_type": obj_type,
                    })
            for tgt in entry.targets:
                if tgt in visited:
                    edges.append({
                        "source": src, "target": tgt,
                        "type": "writes", "object_type": obj_type,
                    })

    return GraphSubset(
        focus=focus,
        depth=depth,
        direction=direction,
        nodes=nodes,
        edges=edges,
        truncated=truncated,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_graph_subgraph.py -v`
Expected: 9 tests pass.

- [ ] **Step 5: Run the full graph test suite to confirm no regressions**

Run: `pytest tests/test_graph.py tests/test_graph_subgraph.py -v`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/frost/graph.py tests/test_graph_subgraph.py
git commit -m "feat(graph): add extract_subgraph for focused lineage queries"
```

---

## Task 2: Add `nodes_and_edges_as_json` helper to `visualizer.py`

**Files:**
- Modify: `src/frost/visualizer.py`
- Test: `tests/test_visualizer.py` (existing — append tests)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_visualizer.py`:

```python
from frost.visualizer import nodes_and_edges_as_json


def test_nodes_and_edges_as_json_shape():
    nodes = [
        {"fqn": "PUBLIC.A", "object_type": "VIEW", "file_path": "a.sql", "columns": []},
    ]
    edges = [
        {"source": "PUBLIC.A", "target": "PUBLIC.B",
         "type": "dependency", "object_type": "VIEW"},
    ]
    payload = nodes_and_edges_as_json(
        nodes=nodes, edges=edges,
        focus="PUBLIC.A", depth=1, direction="both", truncated=False,
    )
    assert payload["focus"] == "PUBLIC.A"
    assert payload["depth"] == 1
    assert payload["direction"] == "both"
    assert payload["truncated"] is False
    assert payload["nodes"] == nodes
    assert payload["edges"] == edges


def test_nodes_and_edges_as_json_full_graph_nulls():
    """Full-graph mode passes focus=None, depth=None, direction=None."""
    payload = nodes_and_edges_as_json(
        nodes=[], edges=[],
        focus=None, depth=None, direction=None, truncated=False,
    )
    assert payload["focus"] is None
    assert payload["depth"] is None
    assert payload["direction"] is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_visualizer.py -k nodes_and_edges_as_json -v`
Expected: ImportError for `nodes_and_edges_as_json`.

- [ ] **Step 3: Implement helper in `visualizer.py`**

Add to the "Public helpers" section of `src/frost/visualizer.py` (around line 33):

```python
def nodes_and_edges_as_json(
    nodes: List[dict],
    edges: List[dict],
    focus: Optional[str],
    depth: Optional[int],
    direction: Optional[str],
    truncated: bool,
) -> dict:
    """Build the standard JSON payload used by the VSCode lineage panel.

    Used both for subgraph responses (focus/depth/direction populated)
    and full-graph responses (all three are ``None``).
    """
    return {
        "focus": focus,
        "depth": depth,
        "direction": direction,
        "nodes": nodes,
        "edges": edges,
        "truncated": truncated,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_visualizer.py -k nodes_and_edges_as_json -v`
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/frost/visualizer.py tests/test_visualizer.py
git commit -m "feat(visualizer): add nodes_and_edges_as_json helper"
```

---

## Task 3: Add `--json` and `--direction` CLI args + new `_cmd_lineage` branches

**Files:**
- Modify: `src/frost/cli.py` — `_build_parser` lineage section + `_cmd_lineage` body.
- Test: `tests/test_cli_lineage_json.py` (new).

- [ ] **Step 1: Write the failing CLI tests**

Create `tests/test_cli_lineage_json.py` with:

```python
"""Tests for `frost lineage --json` subgraph and full-graph CLI branches."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "data" / "lineage_fixture"


def _run_cli(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Run `python -m frost ...` and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "frost", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


@pytest.fixture()
def fixture_project(tmp_path: Path) -> Path:
    """Minimal frost project with 3 objects: VIEW_A -> TABLE_B."""
    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "table_b.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.TABLE_B (id INT);\n"
    )
    (objects / "view_a.sql").write_text(
        "CREATE OR ALTER VIEW PUBLIC.VIEW_A AS SELECT id FROM PUBLIC.TABLE_B;\n"
    )
    (tmp_path / "frost-config.yml").write_text(
        "account: x\nuser: x\nrole: x\nwarehouse: x\ndatabase: DB\n"
        "objects_folder: objects\n"
    )
    return tmp_path


def test_subgraph_json_focus(fixture_project: Path):
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--json",
         "--object", "PUBLIC.VIEW_A", "--depth", "1", "--direction", "both"],
        cwd=fixture_project,
    )
    assert result.returncode == 0, result.stderr
    # Strip any leading log lines before the first '{'.
    stdout = result.stdout
    stdout = stdout[stdout.index("{"):]
    payload = json.loads(stdout)
    assert payload["focus"] == "PUBLIC.VIEW_A"
    assert payload["depth"] == 1
    assert payload["direction"] == "both"
    fqns = {n["fqn"] for n in payload["nodes"]}
    assert fqns == {"PUBLIC.VIEW_A", "PUBLIC.TABLE_B"}


def test_subgraph_unknown_fqn_exits_2(fixture_project: Path):
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--json",
         "--object", "PUBLIC.NO_SUCH", "--depth", "1"],
        cwd=fixture_project,
    )
    assert result.returncode == 2
    stdout = result.stdout
    stdout = stdout[stdout.index("{"):]
    payload = json.loads(stdout)
    assert payload["error"] == "object not found"
    assert payload["fqn"] == "PUBLIC.NO_SUCH"


def test_full_graph_json(fixture_project: Path):
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--json"],
        cwd=fixture_project,
    )
    assert result.returncode == 0, result.stderr
    stdout = result.stdout
    stdout = stdout[stdout.index("{"):]
    payload = json.loads(stdout)
    assert payload["focus"] is None
    assert payload["depth"] is None
    assert payload["direction"] is None
    fqns = {n["fqn"] for n in payload["nodes"]}
    assert fqns == {"PUBLIC.VIEW_A", "PUBLIC.TABLE_B"}


def test_html_output_still_works(fixture_project: Path, tmp_path: Path):
    """Existing `frost lineage --local --output X.html` behaviour preserved."""
    out = fixture_project / "lineage.html"
    result = _run_cli(
        ["-c", "frost-config.yml", "lineage", "--local", "--output", str(out)],
        cwd=fixture_project,
    )
    assert result.returncode == 0, result.stderr
    assert out.exists()
    html = out.read_text()
    assert "<html" in html.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cli_lineage_json.py -v`
Expected: Failures — `--json` and `--direction` flags unknown to argparse.

- [ ] **Step 3: Add CLI args in `_build_parser`**

In `src/frost/cli.py`, within the `# lineage` block near line 751, after the existing `--depth` argument, add:

```python
    lineage_parser.add_argument(
        "--direction",
        choices=["up", "down", "both"],
        default="both",
        help="Subgraph traversal direction when used with --object --json "
             "(default: both)",
    )
    lineage_parser.add_argument(
        "--json",
        action="store_true",
        help="Output lineage as JSON (enables subgraph mode when combined "
             "with --object)",
    )
```

- [ ] **Step 4: Rewrite `_cmd_lineage` to handle JSON branches**

Replace the entire `_cmd_lineage` function body in `src/frost/cli.py` (currently at line 300–400) with the following. Keep the function signature the same.

```python
def _cmd_lineage(config, args):
    """Generate an interactive HTML lineage visualisation or JSON payload."""
    output = getattr(args, "output", "lineage.html")
    local = getattr(args, "local", False)
    focus_object = getattr(args, "object", None)
    initial_depth = getattr(args, "depth", 1)
    direction = getattr(args, "direction", "both")
    json_mode = getattr(args, "json", False)

    # JSON mode always implies --local; querying Snowflake for subgraphs
    # is not a supported use case in Phase 1.
    if json_mode and not local:
        log.error("--json currently requires --local")
        sys.exit(2)

    if json_mode:
        _cmd_lineage_json(config, focus_object, initial_depth, direction)
        return

    # Existing HTML path -- unchanged behaviour.
    if local:
        from frost.deployer import Deployer
        deployer = Deployer(config)
        try:
            deployer._scan_and_parse()
        except PolicyError:
            pass
        deployer._build_graph()
        edges = deployer._graph.get_all_edges()
        node_types = deployer._graph.get_node_types()
        node_columns = deployer._graph.get_node_columns()
        if not edges:
            print("No edges found -- nothing to visualise.")
            return
        html = generate_html(edges, title="frost · Lineage (local)",
                             focus_object=focus_object,
                             node_types=node_types,
                             initial_depth=initial_depth,
                             node_columns=node_columns)
    else:
        # Remote Snowflake path -- unchanged from current implementation.
        from frost.connector import ConnectionConfig, SnowflakeConnector
        conn_cfg = ConnectionConfig(
            account=config.account,
            user=config.user,
            role=config.role,
            warehouse=config.warehouse,
            database=config.database,
            private_key_path=config.private_key_path,
            private_key_passphrase=config.private_key_passphrase,
        )
        schema = config.tracking_schema or "FROST"
        table = f"{schema}.OBJECT_LINEAGE"
        connector = SnowflakeConnector(conn_cfg)
        with connector:
            rows = connector.execute(f"SELECT * FROM {table} ORDER BY object_fqn")
            if not rows:
                print(f"No lineage data in {table} -- run 'frost deploy' first.")
                return
            edges = edges_from_rows(rows)

            history_schema = config.tracking_schema or "FROST"
            history_table = f"{history_schema}.DEPLOY_HISTORY"
            type_rows = connector.execute(f"""
                SELECT object_fqn, object_type
                FROM {history_table}
                WHERE status = 'SUCCESS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY object_fqn ORDER BY deployed_at DESC
                ) = 1
            """)
            node_types = {r[0]: r[1] for r in type_rows} if type_rows else {}

            node_columns: dict = {}
            try:
                col_rows = connector.execute(f"""
                    SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME,
                           COLUMN_NAME,
                           DATA_TYPE
                    FROM {config.database}.INFORMATION_SCHEMA.COLUMNS
                    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """)
                if col_rows:
                    for r in col_rows:
                        fqn3 = r[0].upper()
                        col = {"name": r[1], "type": r[2]}
                        node_columns.setdefault(fqn3, []).append(col)
                        parts = fqn3.split('.', 1)
                        fqn2 = parts[1] if len(parts) == 2 else fqn3
                        node_columns.setdefault(fqn2, []).append(col)
            except Exception as exc:
                print(f"Warning: could not fetch column metadata: {exc}")

        html = generate_html(edges, title="frost · Lineage",
                             focus_object=focus_object,
                             node_types=node_types,
                             initial_depth=initial_depth,
                             node_columns=node_columns)

    path = write_and_open(html, output)
    print(f"Lineage visual opened: {path}")


def _cmd_lineage_json(config, focus_object, depth, direction):
    """Emit a subgraph or full-graph JSON payload to stdout (no Snowflake)."""
    from frost.deployer import Deployer
    from frost.graph import extract_subgraph
    from frost.visualizer import nodes_and_edges_as_json

    deployer = Deployer(config)
    try:
        deployer._scan_and_parse()
    except PolicyError:
        pass
    deployer._build_graph()
    graph = deployer._graph

    if focus_object:
        subset = extract_subgraph(
            graph, focus_object, depth=depth, direction=direction,
        )
        if subset is None:
            print(json.dumps({
                "error": "object not found",
                "fqn": focus_object.upper(),
            }))
            sys.exit(2)
        payload = nodes_and_edges_as_json(
            nodes=subset.nodes,
            edges=subset.edges,
            focus=subset.focus,
            depth=subset.depth,
            direction=subset.direction,
            truncated=subset.truncated,
        )
    else:
        # Full-graph JSON: reuse existing edge/node gathering logic.
        edges = graph.get_all_edges()
        node_types = graph.get_node_types()
        node_columns = graph.get_node_columns()
        fqns = set(node_types) | {e["source"] for e in edges} | {e["target"] for e in edges}
        nodes = []
        for fqn in sorted(fqns):
            nodes.append({
                "fqn": fqn,
                "object_type": node_types.get(fqn, "EXTERNAL"),
                "file_path": (
                    graph._objects[fqn].file_path if fqn in graph._objects else ""
                ),
                "columns": node_columns.get(fqn, []),
            })
        payload = nodes_and_edges_as_json(
            nodes=nodes,
            edges=edges,
            focus=None, depth=None, direction=None, truncated=False,
        )

    print(json.dumps(payload))
```

- [ ] **Step 5: Run JSON CLI tests to verify they pass**

Run: `pytest tests/test_cli_lineage_json.py -v`
Expected: 4 tests pass.

- [ ] **Step 6: Run the full CLI test suite to confirm no regressions**

Run: `pytest tests/test_cli.py tests/test_cli_lineage_json.py -v`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add src/frost/cli.py tests/test_cli_lineage_json.py
git commit -m "feat(cli): add --json and --direction to lineage for subgraph output"
```

---

## Task 4: Remove lineage scan from `graph --json` and lock it down with a test

**Why this is needed:** Today `Deployer._build_graph()` unconditionally calls
`LineageScanner.scan()` at `src/frost/deployer.py:504-505`. `_cmd_graph` goes
through `deployer.plan()` → `_build_graph()`, so **every** `frost graph --json`
call (one per extension activation) pays the full lineage-regex cost, even
though the graph command never uses lineage edges. Fixing this is a modest but
real activation-time win and is the main behaviour change in this task.

**Files:**
- Modify: `src/frost/deployer.py` — `_build_graph` gains `include_lineage`.
- Modify: `src/frost/cli.py` — `_cmd_graph` passes `include_lineage=False`.
- Test: `tests/test_activation_safety.py` (new).

- [ ] **Step 1: Write the failing test**

Create `tests/test_activation_safety.py` with:

```python
"""Lockdown test: `frost graph --json` must not invoke LineageScanner.

The VSCode extension calls `frost graph --json` on activation for every
project. Today the call path goes Deployer.plan -> _build_graph ->
LineageScanner.scan, which scans every procedure / function / task /
stream body for regex matches — pure waste for the graph command, and a
significant contributor to activation memory pressure on 1700-object
workspaces. This test locks the fix in.
"""

from argparse import Namespace
from unittest.mock import patch

from frost import cli
from frost.config import FrostConfig


def _write_config(root, objects_folder):
    (root / "frost-config.yml").write_text(
        "account: x\nuser: x\nrole: x\nwarehouse: x\n"
        "database: DB\nobjects_folder: " + str(objects_folder) + "\n"
    )


def test_graph_command_does_not_invoke_lineage_scanner(tmp_path, capsys):
    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "t.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.T (id INT);\n"
    )
    _write_config(tmp_path, objects)

    cfg = FrostConfig(
        account="x", user="x", role="x", warehouse="x",
        database="DB", objects_folder=str(objects),
    )

    with patch("frost.deployer.LineageScanner") as scanner_cls:
        args = Namespace(json=True)
        cli._cmd_graph(cfg, args)
        scanner_cls.assert_not_called()


def test_build_graph_skip_lineage_produces_empty_lineage(tmp_path):
    """Calling _build_graph with include_lineage=False leaves _lineage empty."""
    from frost.deployer import Deployer

    objects = tmp_path / "objects"
    objects.mkdir()
    (objects / "t.sql").write_text(
        "CREATE OR ALTER TABLE PUBLIC.T (id INT);\n"
    )
    cfg = FrostConfig(
        account="x", user="x", role="x", warehouse="x",
        database="DB", objects_folder=str(objects),
    )
    deployer = Deployer(cfg)
    deployer._scan_and_parse()
    deployer._build_graph(include_lineage=False)
    assert deployer._graph.lineage == {}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_activation_safety.py -v`
Expected: Both tests fail. The first fails because `LineageScanner` **is**
called today; the second fails because `_build_graph` does not accept
`include_lineage`.

- [ ] **Step 3: Add `include_lineage` to `Deployer._build_graph`**

In `src/frost/deployer.py`, replace the `_build_graph` method (currently at
lines 496–512) with:

```python
    def _build_graph(self, include_lineage: bool = True) -> None:
        """Add all parsed objects to the graph, build edges, and (optionally)
        merge lineage entries.

        Parameters
        ----------
        include_lineage : bool, default True
            When False, skip the `LineageScanner.scan()` call entirely. The
            `graph` CLI command uses this to avoid paying the lineage-regex
            cost during VSCode extension activation; see
            `tests/test_activation_safety.py`.
        """
        self._graph = DependencyGraph()
        for obj in self._objects.values():
            self._graph.add_object(obj)
        self._graph.build()

        if not include_lineage:
            return

        # Auto-detect lineage from procedure bodies + YAML overrides
        scanner = LineageScanner(self.config.objects_folder)
        lineage_entries = scanner.scan(parsed_objects=self._objects)

        if lineage_entries:
            # Map file_path -> actual FQN for resolution
            file_to_fqn = {obj.file_path: obj.fqn for obj in self._objects.values()}
            resolved = merge_lineage_with_graph(lineage_entries, file_to_fqn)
            for entry in resolved:
                self._graph.add_lineage(entry)
```

- [ ] **Step 4: Route `_cmd_graph` through `include_lineage=False`**

In `src/frost/cli.py`, `_cmd_graph` currently calls `deployer.plan()`, which
internally calls `_build_graph()` with the default. We need to replace the
`plan()` call path with an explicit scan + graph build that skips lineage.

Replace the top of `_cmd_graph` (the `deployer = Deployer(config)` block down
to the end of the `try/except PolicyError` block — around lines 256–268) with:

```python
def _cmd_graph(config, args):
    """Print the dependency graph (deploy order + dependencies).

    INVARIANT: This command does not invoke LineageScanner. The VSCode
    extension calls this on activation for every project; lineage work
    here would make 1000+ object workspaces pay an unnecessary
    regex-parsing cost per activation. See
    tests/test_activation_safety.py.
    """
    deployer = Deployer(config)
    violations = []

    try:
        deployer._scan_and_parse()
    except PolicyError as exc:
        violations = exc.violations if hasattr(exc, "violations") else []

    deployer._build_graph(include_lineage=False)
    try:
        plan = deployer._graph.visualize()
    except Exception:
        plan = ""
```

Leave the remainder of `_cmd_graph` (the `if getattr(args, "json", False):`
branch and the final `else: print(plan)`) unchanged.

- [ ] **Step 5: Run the activation-safety tests to verify they pass**

Run: `pytest tests/test_activation_safety.py -v`
Expected: Both tests pass.

- [ ] **Step 6: Run the full deployer and CLI tests to confirm no regression**

Run: `pytest tests/test_deployer.py tests/test_cli.py tests/test_lifecycle.py -v`
Expected: All pass. `deploy` / `plan` / `lineage` still run `_build_graph`
with the default (`include_lineage=True`).

- [ ] **Step 7: Add a comment in `extension.ts` noting the invariant**

In `vscode-frost/src/extension.ts`, find the block starting at line 358:

```typescript
  runner.ensureFrostInstalled().then((ok) => {
    if (ok) {
      objectsProvider.refresh();
```

Add a comment directly above `objectsProvider.refresh();`:

```typescript
      // IMPORTANT: this calls `frost graph --json`, which must NOT invoke
      // lineage scanning. The invariant is enforced by
      // tests/test_activation_safety.py in the Python library. If you change
      // Deployer._build_graph or _cmd_graph, re-verify that test.
```

- [ ] **Step 8: Commit**

```bash
git add src/frost/deployer.py src/frost/cli.py tests/test_activation_safety.py vscode-frost/src/extension.ts
git commit -m "perf: skip lineage scan in graph command to speed up activation"
```

---

## Task 5: Add typed helpers to `frostRunner.ts`

**Files:**
- Modify: `vscode-frost/src/frostRunner.ts`.

- [ ] **Step 1: Add the `SubgraphPayload` interface and helpers**

In `vscode-frost/src/frostRunner.ts`, add near the other interface declarations (around line 71, after `GraphPayload`):

```typescript
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
```

- [ ] **Step 2: Add the runner methods**

In the `FrostRunner` class, below the existing `lineageHtml()` method (around line 449), add:

```typescript
  /** Fetch a focused subgraph around a single object. */
  async lineageSubgraph(
    fqn: string,
    depth: number,
    direction: "up" | "down" | "both" = "both"
  ): Promise<SubgraphPayload> {
    const raw = await this.exec(
      `lineage --local --json --object ${fqn} --depth ${depth} --direction ${direction}`
    );
    return JSON.parse(this.extractJson(raw)) as SubgraphPayload;
  }

  /** Fetch the full lineage graph as JSON (opt-in, large). */
  async lineageFullJson(): Promise<SubgraphPayload> {
    const raw = await this.exec("lineage --local --json");
    return JSON.parse(this.extractJson(raw)) as SubgraphPayload;
  }
```

- [ ] **Step 3: Compile the extension**

Run: `cd vscode-frost && npm run compile`
Expected: No TypeScript errors. A warning about unused imports is acceptable only until Task 7 uses them.

- [ ] **Step 4: Commit**

```bash
git add vscode-frost/src/frostRunner.ts
git commit -m "feat(vscode): add lineageSubgraph and lineageFullJson helpers"
```

---

## Task 6: Create static webview assets

**Files:**
- Create: `vscode-frost/media/lineage/index.html`
- Create: `vscode-frost/media/lineage/lineage.js`
- Create: `vscode-frost/media/lineage/lineage.css`

- [ ] **Step 1: Create `index.html`**

Create `vscode-frost/media/lineage/index.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta http-equiv="Content-Security-Policy"
  content="default-src 'none';
           img-src ${webview.cspSource} data:;
           script-src ${webview.cspSource} 'unsafe-inline';
           style-src ${webview.cspSource} 'unsafe-inline';" />
<title>Frost · Lineage</title>
<link rel="stylesheet" href="${cssUri}" />
</head>
<body>
  <header class="toolbar">
    <input id="search" type="text" placeholder="Search for an object (e.g. PUBLIC.MY_VIEW)…" />
    <ul id="suggestions" class="hidden"></ul>
    <span class="sep"></span>
    <label>Depth
      <input id="depth" type="range" min="1" max="5" value="1" />
      <span id="depthVal">1</span>
    </label>
    <label>Direction
      <select id="direction">
        <option value="both">both</option>
        <option value="up">upstream</option>
        <option value="down">downstream</option>
      </select>
    </label>
    <button id="fullGraphBtn">Show full graph…</button>
  </header>

  <main id="canvas-wrap">
    <svg id="canvas"></svg>
    <div id="status" class="status">Pick an object above to see its neighbourhood.</div>
  </main>

  <aside id="details" class="details hidden">
    <h3 id="detailsTitle"></h3>
    <p id="detailsType" class="muted"></p>
    <div id="detailsColumns"></div>
  </aside>

  <script src="${d3Uri}"></script>
  <script src="${jsUri}"></script>
</body>
</html>
```

- [ ] **Step 2: Create `lineage.css`**

Create `vscode-frost/media/lineage/lineage.css`:

```css
:root {
  --bg: var(--vscode-editor-background, #0f172a);
  --fg: var(--vscode-editor-foreground, #e2e8f0);
  --muted: var(--vscode-descriptionForeground, #94a3b8);
  --border: var(--vscode-panel-border, #334155);
  --accent: var(--vscode-focusBorder, #38bdf8);
  --error: var(--vscode-errorForeground, #f87171);
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; background: var(--bg); color: var(--fg);
             font-family: var(--vscode-font-family); font-size: 13px; }

.toolbar {
  display: flex; align-items: center; gap: 8px;
  padding: 6px 10px; border-bottom: 1px solid var(--border);
  position: relative;
}
.toolbar input[type=text] { flex: 0 0 300px; padding: 4px 6px;
  background: var(--bg); color: var(--fg); border: 1px solid var(--border); }
.toolbar .sep { flex: 1 1 auto; }
.toolbar label { display: flex; align-items: center; gap: 4px; color: var(--muted); }
.toolbar button { background: var(--bg); color: var(--fg);
  border: 1px solid var(--border); padding: 4px 10px; cursor: pointer; }
.toolbar button:hover { border-color: var(--accent); }

#suggestions {
  position: absolute; top: 100%; left: 10px; right: 10px;
  max-height: 200px; overflow-y: auto;
  background: var(--bg); border: 1px solid var(--border);
  list-style: none; margin: 0; padding: 0; z-index: 10;
}
#suggestions li { padding: 4px 8px; cursor: pointer; }
#suggestions li:hover, #suggestions li.active { background: var(--border); }
.hidden { display: none !important; }

#canvas-wrap { position: relative; height: calc(100vh - 44px); }
#canvas { width: 100%; height: 100%; }
.status { position: absolute; top: 50%; left: 50%; transform: translate(-50%,-50%);
  color: var(--muted); }
.status.error { color: var(--error); }

.node rect { fill: var(--bg); stroke: var(--border); stroke-width: 1; rx: 4; }
.node.focus rect { stroke: var(--accent); stroke-width: 2; }
.node text { fill: var(--fg); font-size: 11px; pointer-events: none; }
.node:hover rect { stroke: var(--accent); }
.edge { stroke: var(--border); stroke-width: 1; fill: none; marker-end: url(#arrow); }
.edge.reads { stroke: #34d399; }
.edge.writes { stroke: #fb923c; }

.details {
  position: absolute; right: 10px; top: 54px; width: 260px;
  background: var(--bg); border: 1px solid var(--border); padding: 10px;
}
.details h3 { margin: 0 0 4px 0; font-size: 13px; }
.details .muted { color: var(--muted); margin: 0 0 6px 0; }
.details ul { list-style: none; margin: 0; padding: 0; max-height: 240px; overflow-y: auto; }
.details li { display: flex; justify-content: space-between; padding: 2px 0;
  border-bottom: 1px dashed var(--border); font-size: 11px; }
```

- [ ] **Step 3: Create `lineage.js`**

Create `vscode-frost/media/lineage/lineage.js`:

```javascript
// @ts-check
(function () {
  const vscode = acquireVsCodeApi();
  const searchInput = document.getElementById("search");
  const suggestions = document.getElementById("suggestions");
  const depthInput = document.getElementById("depth");
  const depthVal = document.getElementById("depthVal");
  const directionSelect = document.getElementById("direction");
  const fullGraphBtn = document.getElementById("fullGraphBtn");
  const statusEl = document.getElementById("status");
  const detailsEl = document.getElementById("details");
  const detailsTitle = document.getElementById("detailsTitle");
  const detailsType = document.getElementById("detailsType");
  const detailsColumns = document.getElementById("detailsColumns");
  const svg = d3.select("#canvas");

  /** @type {string[]} */
  let objectList = [];
  let currentFocus = null;

  // --- Object picker -------------------------------------------------
  function refreshSuggestions() {
    const q = searchInput.value.trim().toUpperCase();
    suggestions.innerHTML = "";
    if (!q || objectList.length === 0) {
      suggestions.classList.add("hidden");
      return;
    }
    const matches = objectList.filter((f) => f.includes(q)).slice(0, 50);
    if (matches.length === 0) {
      suggestions.classList.add("hidden");
      return;
    }
    for (const fqn of matches) {
      const li = document.createElement("li");
      li.textContent = fqn;
      li.addEventListener("mousedown", (e) => {
        e.preventDefault();
        pickObject(fqn);
      });
      suggestions.appendChild(li);
    }
    suggestions.classList.remove("hidden");
  }
  searchInput.addEventListener("input", refreshSuggestions);
  searchInput.addEventListener("focus", refreshSuggestions);
  searchInput.addEventListener("blur", () => {
    setTimeout(() => suggestions.classList.add("hidden"), 100);
  });

  function pickObject(fqn) {
    searchInput.value = fqn;
    suggestions.classList.add("hidden");
    currentFocus = fqn;
    statusEl.textContent = `Loading ${fqn}…`;
    statusEl.classList.remove("error", "hidden");
    vscode.postMessage({
      type: "fetchSubgraph",
      fqn,
      depth: Number(depthInput.value),
      direction: directionSelect.value,
    });
  }

  depthInput.addEventListener("input", () => {
    depthVal.textContent = depthInput.value;
    if (currentFocus) pickObject(currentFocus);
  });
  directionSelect.addEventListener("change", () => {
    if (currentFocus) pickObject(currentFocus);
  });
  fullGraphBtn.addEventListener("click", () => {
    vscode.postMessage({ type: "fetchFullGraph" });
  });

  // --- Render --------------------------------------------------------
  function render(payload) {
    statusEl.classList.add("hidden");
    svg.selectAll("*").remove();

    // Arrow marker defs
    const defs = svg.append("defs");
    defs.append("marker")
      .attr("id", "arrow")
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 18).attr("refY", 0)
      .attr("markerWidth", 6).attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path").attr("d", "M0,-5L10,0L0,5")
      .attr("fill", "var(--muted)");

    const width = svg.node().clientWidth;
    const height = svg.node().clientHeight;
    const g = svg.append("g");
    svg.call(d3.zoom().on("zoom", (ev) => g.attr("transform", ev.transform)));

    const nodes = payload.nodes.map((n) => ({ ...n, id: n.fqn }));
    const links = payload.edges.map((e) => ({
      source: e.source, target: e.target, type: e.type,
    }));

    const sim = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(links).id((d) => d.id).distance(120))
      .force("charge", d3.forceManyBody().strength(-400))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collide", d3.forceCollide(60));

    const linkSel = g.append("g").selectAll("path").data(links)
      .enter().append("path")
      .attr("class", (d) => `edge ${d.type}`);

    const nodeSel = g.append("g").selectAll("g").data(nodes)
      .enter().append("g")
      .attr("class", (d) => (d.fqn === payload.focus ? "node focus" : "node"))
      .on("click", (_, d) => showDetails(d))
      .call(
        d3.drag()
          .on("start", (ev, d) => { if (!ev.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
          .on("drag", (ev, d) => { d.fx = ev.x; d.fy = ev.y; })
          .on("end", (ev, d) => { if (!ev.active) sim.alphaTarget(0); d.fx = null; d.fy = null; })
      );
    nodeSel.append("rect").attr("width", 160).attr("height", 28).attr("x", -80).attr("y", -14);
    nodeSel.append("text").attr("text-anchor", "middle").attr("dy", "0.35em")
      .text((d) => d.fqn);

    sim.on("tick", () => {
      linkSel.attr("d", (d) => {
        const sx = d.source.x, sy = d.source.y, tx = d.target.x, ty = d.target.y;
        return `M${sx},${sy}L${tx},${ty}`;
      });
      nodeSel.attr("transform", (d) => `translate(${d.x},${d.y})`);
    });
  }

  function showDetails(node) {
    detailsEl.classList.remove("hidden");
    detailsTitle.textContent = node.fqn;
    detailsType.textContent = node.object_type + (node.file_path ? ` · ${node.file_path}` : "");
    if (node.columns && node.columns.length) {
      const ul = document.createElement("ul");
      for (const c of node.columns) {
        const li = document.createElement("li");
        li.innerHTML = `<span>${c.name}</span><span class="muted">${c.type}</span>`;
        ul.appendChild(li);
      }
      detailsColumns.innerHTML = "";
      detailsColumns.appendChild(ul);
    } else {
      detailsColumns.innerHTML = "";
    }
  }

  // --- Message handling ---------------------------------------------
  window.addEventListener("message", (ev) => {
    const msg = ev.data;
    if (msg.type === "objectList") {
      objectList = msg.fqns;
      return;
    }
    if (msg.type === "subgraph") {
      render(msg.payload);
      return;
    }
    if (msg.type === "error") {
      statusEl.textContent = msg.message;
      statusEl.classList.remove("hidden");
      statusEl.classList.add("error");
      return;
    }
  });

  // Tell the extension we're ready for the initial object list.
  vscode.postMessage({ type: "ready" });
})();
```

- [ ] **Step 4: Verify the files exist**

Run: `ls vscode-frost/media/lineage/`
Expected: `index.html  lineage.css  lineage.js`

- [ ] **Step 5: Commit**

```bash
git add vscode-frost/media/lineage/
git commit -m "feat(vscode): add static webview assets for lineage panel"
```

---

## Task 7: Rewrite `LineagePanel` to use picker + message protocol

**Files:**
- Modify (rewrite): `vscode-frost/src/lineagePanel.ts`.
- Modify: `vscode-frost/src/extension.ts` (pass the Objects tree provider to the panel so it can source the FQN list).

- [ ] **Step 1: Replace `lineagePanel.ts`**

Replace the entire contents of `vscode-frost/src/lineagePanel.ts` with:

```typescript
/**
 * LineagePanel — focused lineage viewer.
 *
 * Opens a webview with an object picker. When the user picks an object,
 * the panel calls `frost lineage --local --json --object FQN --depth N`
 * and renders the small subgraph. A "Show full graph" button fetches the
 * full-graph JSON behind a size confirmation.
 */

import * as vscode from "vscode";
import { FrostRunner, SubgraphPayload } from "./frostRunner";
import { FrostObjectsProvider } from "./objectsTree";

const FULL_GRAPH_WARNING_THRESHOLD = 300;

export class LineagePanel {
  private static currentPanel: LineagePanel | undefined;
  private readonly panel: vscode.WebviewPanel;
  private disposed = false;

  private constructor(
    panel: vscode.WebviewPanel,
    private readonly extensionUri: vscode.Uri,
    private readonly runner: FrostRunner,
    private readonly objectsProvider: FrostObjectsProvider,
  ) {
    this.panel = panel;
    this.panel.webview.html = this.buildHtml();
    this.panel.onDidDispose(() => {
      this.disposed = true;
      LineagePanel.currentPanel = undefined;
    });
    this.panel.webview.onDidReceiveMessage((msg) => this.onMessage(msg));
  }

  static show(
    extensionUri: vscode.Uri,
    runner: FrostRunner,
    objectsProvider: FrostObjectsProvider,
  ): void {
    if (LineagePanel.currentPanel) {
      LineagePanel.currentPanel.panel.reveal(vscode.ViewColumn.One);
      return;
    }
    const mediaRoot = vscode.Uri.joinPath(extensionUri, "media", "lineage");
    const panel = vscode.window.createWebviewPanel(
      "frostLineage",
      "Frost · Lineage",
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [mediaRoot],
      },
    );
    LineagePanel.currentPanel = new LineagePanel(
      panel, extensionUri, runner, objectsProvider,
    );
  }

  // --- Message dispatch --------------------------------------------
  private async onMessage(msg: any): Promise<void> {
    if (this.disposed) return;
    switch (msg?.type) {
      case "ready":
        this.postObjectList();
        return;
      case "fetchSubgraph":
        await this.handleSubgraph(msg.fqn, msg.depth, msg.direction);
        return;
      case "fetchFullGraph":
        await this.handleFullGraph();
        return;
    }
  }

  private postObjectList(): void {
    const fqns = this.objectsProvider.getAllFqns();
    this.panel.webview.postMessage({ type: "objectList", fqns });
  }

  private async handleSubgraph(
    fqn: string, depth: number, direction: "up" | "down" | "both"
  ): Promise<void> {
    try {
      const payload = await this.runner.lineageSubgraph(fqn, depth, direction);
      this.panel.webview.postMessage({ type: "subgraph", payload });
    } catch (err: any) {
      this.postError(`Could not load lineage for ${fqn}: ${err.message}`);
    }
  }

  private async handleFullGraph(): Promise<void> {
    const count = this.objectsProvider.getAllFqns().length;
    if (count > FULL_GRAPH_WARNING_THRESHOLD) {
      const choice = await vscode.window.showWarningMessage(
        `This project has ${count} objects. Rendering the full graph ` +
          `may be slow and use significant memory. Continue?`,
        { modal: true },
        "Continue",
      );
      if (choice !== "Continue") return;
    }
    try {
      const payload = await this.runner.lineageFullJson();
      this.panel.webview.postMessage({ type: "subgraph", payload });
    } catch (err: any) {
      this.postError(`Could not load full lineage: ${err.message}`);
    }
  }

  private postError(message: string): void {
    this.panel.webview.postMessage({ type: "error", message });
  }

  // --- HTML shell --------------------------------------------------
  private buildHtml(): string {
    const mediaRoot = vscode.Uri.joinPath(this.extensionUri, "media", "lineage");
    const indexUri = vscode.Uri.joinPath(mediaRoot, "index.html");
    const cssUri = this.panel.webview.asWebviewUri(
      vscode.Uri.joinPath(mediaRoot, "lineage.css"),
    );
    const jsUri = this.panel.webview.asWebviewUri(
      vscode.Uri.joinPath(mediaRoot, "lineage.js"),
    );
    // D3 v7 is bundled locally to avoid CSP/CDN issues; see Task 8.
    const d3Uri = this.panel.webview.asWebviewUri(
      vscode.Uri.joinPath(mediaRoot, "d3.min.js"),
    );
    const tpl = require("fs").readFileSync(indexUri.fsPath, "utf-8") as string;
    return tpl
      .replace(/\$\{webview\.cspSource\}/g, this.panel.webview.cspSource)
      .replace(/\$\{cssUri\}/g, cssUri.toString())
      .replace(/\$\{jsUri\}/g, jsUri.toString())
      .replace(/\$\{d3Uri\}/g, d3Uri.toString());
  }
}
```

- [ ] **Step 2: Cache the raw node list and expose `getAllFqns` on `FrostObjectsProvider`**

In `vscode-frost/src/objectsTree.ts`, the provider currently stores only the
tree (`private tree: TreeItem[] = [];`) and discards the flat `FrostNode[]`
after `buildTree`. We need the flat list for the picker.

1. Add a new private field below the existing `private tree: TreeItem[] = [];`
   declaration (around line 148):

```typescript
  /** Flat cache of the most recent FrostNode[] for the lineage picker. */
  private _flatNodes: FrostNode[] = [];
```

2. Inside `loadGraph()`, just before `this.tree = this.buildTree(payload.nodes);`
   (currently line 222), add:

```typescript
      this._flatNodes = payload.nodes;
```

3. Also set `this._flatNodes = [];` in the `catch` branch of `loadGraph()`
   (currently around line 227, directly before `this.tree = [];`).

4. Add a public method immediately below the constructor (around line 157,
   directly after `constructor(private runner: FrostRunner) {}`):

```typescript
  /** Flat list of every known FQN in the current graph. Empty while loading. */
  public getAllFqns(): string[] {
    return this._flatNodes.map((n) => n.fqn);
  }
```

- [ ] **Step 3: Update the `frost.lineageLocal` command registration**

In `vscode-frost/src/extension.ts`, find the line:

```typescript
    vscode.commands.registerCommand("frost.lineageLocal", () => {
      LineagePanel.show(context.extensionUri, runner);
    }),
```

Replace it with:

```typescript
    vscode.commands.registerCommand("frost.lineageLocal", () => {
      LineagePanel.show(context.extensionUri, runner, objectsProvider);
    }),
```

- [ ] **Step 4: Compile the extension**

Run: `cd vscode-frost && npm run compile`
Expected: No errors. If `getAllFqns` in Step 2 was inserted against the wrong field name, fix it now.

- [ ] **Step 5: Commit**

```bash
git add vscode-frost/src/lineagePanel.ts vscode-frost/src/objectsTree.ts vscode-frost/src/extension.ts
git commit -m "feat(vscode): rewrite LineagePanel around object picker + subgraph"
```

---

## Task 8: Bundle D3.js locally and declare media files in package.json

**Files:**
- Create: `vscode-frost/media/lineage/d3.min.js`
- Modify: `vscode-frost/package.json`.

- [ ] **Step 1: Vendor D3 v7 into the extension**

From the repo root:

```bash
curl -L -o vscode-frost/media/lineage/d3.min.js https://d3js.org/d3.v7.min.js
```

Verify it downloaded:

Run: `ls -l vscode-frost/media/lineage/d3.min.js`
Expected: file size > 200 KB.

- [ ] **Step 2: Ensure media files ship in the `.vsix`**

In `vscode-frost/package.json`, add (or merge into) a `files` array at the top level. If `files` already exists, append `"media/"` to it; otherwise insert this after the `"main"` line:

```json
  "files": [
    "out/",
    "media/",
    "resources/",
    "README.md"
  ],
```

Also verify `media/` is not excluded in `.vscodeignore` — if that file exists, open it and remove any `media/` or `media/**` line. If `.vscodeignore` does not exist, skip this.

- [ ] **Step 3: Rebuild the extension package**

Run:
```bash
cd vscode-frost && npm run compile && npx @vscode/vsce package --no-dependencies
```
Expected: A new `frost-snowflake-0.1.0.vsix` is produced. Verify that the vsix contains the media assets:

Run: `unzip -l vscode-frost/frost-snowflake-0.1.0.vsix | grep media/lineage`
Expected: 4 files listed (`index.html`, `lineage.js`, `lineage.css`, `d3.min.js`).

- [ ] **Step 4: Commit**

```bash
git add vscode-frost/media/lineage/d3.min.js vscode-frost/package.json vscode-frost/frost-snowflake-0.1.0.vsix
git commit -m "build(vscode): vendor D3 v7 and include media in vsix"
```

---

## Task 9: Manual verification checklist and README update

**Files:**
- Modify: `vscode-frost/README.md`.

- [ ] **Step 1: Append the checklist to README.md**

Append the following section to `vscode-frost/README.md`:

```markdown
## Manual Verification — Large Projects

Use these steps to verify the Phase 1 lineage panel on a 1700-object
workspace. All steps must pass.

1. Install the freshly built vsix:
   `code --install-extension vscode-frost/frost-snowflake-0.1.0.vsix`
2. Open a workspace with ≥ 1000 managed objects. Confirm activation
   completes (Objects tree appears, even if still "Loading…") within a
   few seconds and VSCode does not crash.
3. Once the Objects tree has populated, run the command
   **Frost: Lineage (local)**. Confirm the panel opens with the
   search/picker visible in under one second.
4. Type a partial FQN into the picker. Confirm the dropdown suggests
   matching objects instantly (no subprocess call).
5. Click a suggestion. Confirm the subgraph renders within 5 s (cold
   scan) and that memory use in VSCode's process explorer stays below
   the level that previously caused the crash.
6. Move the depth slider and change the direction selector. Confirm the
   subgraph re-renders each time.
7. Click **Show full graph…**. Confirm a modal appears warning about
   the object count. Click **Continue** and observe either a successful
   full-graph render or a graceful error (no crash).
```

- [ ] **Step 2: Run the full Python test suite as a regression check**

Run: `pytest`
Expected: All tests pass, including the three new test files.

- [ ] **Step 3: Commit**

```bash
git add vscode-frost/README.md
git commit -m "docs(vscode): add manual verification checklist for large projects"
```

---

## Task 10: Final verification

- [ ] **Step 1: Run the entire Python test suite**

Run: `pytest`
Expected: All tests pass, coverage for `src/frost/graph.py` increased.

- [ ] **Step 2: Compile the extension one final time**

Run: `cd vscode-frost && npm run compile`
Expected: No TypeScript errors, no warnings for the new files.

- [ ] **Step 3: Walk the manual checklist in `vscode-frost/README.md`**

Expected: All seven manual-checklist steps pass on the 1700-object reference workspace.

- [ ] **Step 4: Verify the design spec's success criteria are met**

Cross-check against `docs/superpowers/specs/2026-04-15-lineage-performance-design.md` §4 *Success Criteria*:

1. Lineage panel does not crash on 1700 objects. (Step 3.5)
2. Picker in < 500 ms. (Step 3.3)
3. Subgraph < 5 s cold. (Step 3.5)
4. > 300 objects triggers warning. (Step 3.7)
5. Activation-safety test passes. (Step 1, `test_activation_safety.py`)
6. All previous tests still pass. (Step 1)

- [ ] **Step 5: Final commit if anything was fixed during verification**

If no fixes were needed, skip. Otherwise:

```bash
git add -u
git commit -m "fix: resolve issues found during final verification"
```
