"""Dependency graph with topological sorting and cycle detection.

Objects are nodes; an edge A -> B means "A depends on B" (B must be
deployed before A).  `resolve_order()` returns a list where every
object appears *after* all of its dependencies.

The graph also holds **lineage** information -- declared source/target
relationships for procedures -- that is shown in ``visualize()`` but
does *not* affect deployment ordering.
"""

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

from frost.lineage import LineageEntry
from frost.parser import ObjectDefinition


class CycleError(Exception):
    """Raised when the graph contains a circular dependency."""

    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(
            "Circular dependency detected:\n  "
            + " -> ".join(cycle)
        )


class DependencyGraph:
    """Directed Acyclic Graph of Snowflake object dependencies."""

    def __init__(self):
        self._objects: Dict[str, ObjectDefinition] = {}
        # fqn -> set of fqns it *depends on*
        self._deps: Dict[str, Set[str]] = defaultdict(set)
        # fqn -> set of fqns that *depend on it*
        self._rdeps: Dict[str, Set[str]] = defaultdict(set)
        # Lineage entries (documentation only -- not used for ordering)
        self._lineage: Dict[str, LineageEntry] = {}  # object_fqn -> entry

    # -- building the graph --------------------------------------------

    def add_object(self, obj: ObjectDefinition) -> None:
        self._objects[obj.fqn] = obj

    def add_lineage(self, entry: LineageEntry) -> None:
        """Register a lineage entry (sources/targets) for an object."""
        self._lineage[entry.object_fqn] = entry

    def build(self) -> None:
        """Create edges from each object's declared dependencies.

        Only edges to *known* objects (i.e. objects in the graph) are kept.
        References to external objects that frost doesn't manage are ignored.
        """
        known = set(self._objects)
        for fqn, obj in self._objects.items():
            for dep in obj.dependencies:
                if dep in known and dep != fqn:
                    self._deps[fqn].add(dep)
                    self._rdeps[dep].add(fqn)

    # -- ordering ------------------------------------------------------

    def resolve_order(self) -> List[ObjectDefinition]:
        """Return objects in safe deployment order (Kahn's algorithm).

        Raises `CycleError` if a circular dependency exists.
        """
        in_degree: Dict[str, int] = {
            fqn: len(self._deps.get(fqn, set())) for fqn in self._objects
        }

        queue: deque[str] = deque(
            fqn for fqn, deg in in_degree.items() if deg == 0
        )
        ordered: List[ObjectDefinition] = []

        while queue:
            fqn = queue.popleft()
            ordered.append(self._objects[fqn])
            for dependent in self._rdeps.get(fqn, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(ordered) != len(self._objects):
            remaining = set(self._objects) - {o.fqn for o in ordered}
            raise CycleError(cycle=self._find_cycle(remaining))

        return ordered

    # -- queries -------------------------------------------------------

    def get_dependents(self, fqn: str) -> Set[str]:
        """All objects that transitively depend on *fqn*."""
        visited: Set[str] = set()
        queue = deque([fqn])
        while queue:
            current = queue.popleft()
            for dep in self._rdeps.get(current, set()):
                if dep not in visited:
                    visited.add(dep)
                    queue.append(dep)
        return visited

    def get_dependencies(self, fqn: str) -> Set[str]:
        """All objects that *fqn* transitively depends on."""
        visited: Set[str] = set()
        queue = deque([fqn])
        while queue:
            current = queue.popleft()
            for dep in self._deps.get(current, set()):
                if dep not in visited:
                    visited.add(dep)
                    queue.append(dep)
        return visited

    # -- visualisation -------------------------------------------------

    def get_node_types(self) -> Dict[str, str]:
        """Return a mapping of FQN -> object_type for every known object."""
        return {fqn: obj.object_type for fqn, obj in self._objects.items()}

    def get_node_columns(self) -> Dict[str, List[dict]]:
        """Return a mapping of FQN -> column dicts for objects that have columns.

        Each column dict has keys ``name`` and ``type``.
        """
        return {fqn: obj.columns for fqn, obj in self._objects.items() if obj.columns}

    def get_all_edges(self) -> List[dict]:
        """Return every edge in the graph as a list of dicts.

        Each dict has keys: ``source``, ``target``, ``type``,
        ``object_type``.
        Types: ``"dependency"`` (parsed), ``"reads"`` (lineage source),
        ``"writes"`` (lineage target).
        """
        edges: List[dict] = []
        for fqn, deps in self._deps.items():
            obj_type = self._objects[fqn].object_type if fqn in self._objects else "UNKNOWN"
            for dep in deps:
                edges.append({"source": fqn, "target": dep, "type": "dependency", "object_type": obj_type})
        for fqn, entry in self._lineage.items():
            obj_type = self._objects[fqn].object_type if fqn in self._objects else "UNKNOWN"
            for src in entry.sources:
                edges.append({"source": fqn, "target": src, "type": "reads", "object_type": obj_type})
            for tgt in entry.targets:
                edges.append({"source": fqn, "target": tgt, "type": "writes", "object_type": obj_type})
        return edges

    @property
    def lineage(self) -> Dict[str, LineageEntry]:
        """All registered lineage entries indexed by object FQN."""
        return dict(self._lineage)

    def visualize(self) -> str:
        """Human-readable text representation of the execution plan."""
        lines = [
            "",
            "Execution Plan",
            "=" * 60,
        ]
        try:
            ordered = self.resolve_order()
        except CycleError as exc:
            lines.append(f"  ERROR: {exc}")
            return "\n".join(lines)

        for i, obj in enumerate(ordered, 1):
            deps = sorted(self._deps.get(obj.fqn, set()))
            deps_str = f"  <- depends on: {', '.join(deps)}" if deps else ""
            lines.append(f"  {i:>3}. [{obj.object_type:<20}] {obj.fqn}{deps_str}")

        lines.append("=" * 60)

        # -- Lineage section (if any) ----------------------------------
        if self._lineage:
            lines.append("")
            lines.append("Procedure Lineage")
            lines.append("-" * 60)
            for fqn, entry in sorted(self._lineage.items()):
                tag = "auto-detected" if entry.auto_detected else "declared"
                lines.append(f"  {fqn}  ({tag})")
                if entry.description:
                    lines.append(f"      {entry.description}")
                if entry.sources:
                    lines.append(f"      reads from : {', '.join(entry.sources)}")
                if entry.targets:
                    lines.append(f"      writes to  : {', '.join(entry.targets)}")
            lines.append("-" * 60)

        return "\n".join(lines)

    # -- internal ------------------------------------------------------

    def _find_cycle(self, nodes: Set[str]) -> List[str]:  # noqa: C901
        """Return one cycle path for error reporting."""
        color: Dict[str, int] = {n: 0 for n in nodes}   # 0=white 1=grey 2=black
        path: List[str] = []

        def dfs(node: str) -> List[str] | None:
            color[node] = 1
            path.append(node)
            for dep in self._deps.get(node, set()):
                if dep not in nodes:
                    continue
                if color[dep] == 1:
                    idx = path.index(dep)
                    return path[idx:] + [dep]
                if color[dep] == 0:
                    result = dfs(dep)
                    if result:
                        return result
            path.pop()
            color[node] = 2
            return None

        for node in nodes:
            if color[node] == 0:
                cycle = dfs(node)
                if cycle:
                    return cycle
        return list(nodes)  # fallback


# ---------------------------------------------------------------------------
# Focused subgraph extraction
# ---------------------------------------------------------------------------

from dataclasses import dataclass as _dataclass  # noqa: E402  (local import)


@_dataclass
class GraphSubset:
    """A subset of a DependencyGraph centred on a focus FQN."""

    focus: str
    depth: int
    direction: str
    nodes: list
    edges: list
    truncated: bool


def extract_subgraph(
    graph: "DependencyGraph",
    focus_fqn: str,
    depth: int,
    direction: str,
) -> "GraphSubset | None":
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
    visited: Set[str] = {focus}
    frontier: List[str] = [focus]
    truncated = False
    for _ in range(depth):
        next_frontier: List[str] = []
        for fqn in frontier:
            neighbours: Set[str] = set()
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
            extras: Set[str] = set()
            if go_up:
                extras |= graph._deps.get(fqn, set())
            if go_down:
                extras |= graph._rdeps.get(fqn, set())
            if extras - visited:
                truncated = True
                break

    # Build node and edge payloads.
    nodes: List[dict] = []
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

    edges: List[dict] = []
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
