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

    def get_all_edges(self) -> List[dict]:
        """Return every edge in the graph as a list of dicts.

        Each dict has keys: ``source``, ``target``, ``type``.
        Types: ``"dependency"`` (parsed), ``"reads"`` (lineage source),
        ``"writes"`` (lineage target).
        """
        edges: List[dict] = []
        for fqn, deps in self._deps.items():
            for dep in deps:
                edges.append({"source": fqn, "target": dep, "type": "dependency"})
        for fqn, entry in self._lineage.items():
            for src in entry.sources:
                edges.append({"source": fqn, "target": src, "type": "reads"})
            for tgt in entry.targets:
                edges.append({"source": fqn, "target": tgt, "type": "writes"})
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
            lines.append("Procedure Lineage (declared)")
            lines.append("-" * 60)
            for fqn, entry in sorted(self._lineage.items()):
                lines.append(f"  {fqn}")
                if entry.description:
                    lines.append(f"      {entry.description}")
                if entry.sources:
                    lines.append(f"      reads from : {', '.join(entry.sources)}")
                if entry.targets:
                    lines.append(f"      writes to  : {', '.join(entry.targets)}")
            lines.append("-" * 60)

        return "\n".join(lines)

    # -- internal ------------------------------------------------------

    def _find_cycle(self, nodes: Set[str]) -> List[str]:
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
