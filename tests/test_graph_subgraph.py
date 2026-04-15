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
    result = extract_subgraph(g, "PUBLIC.A", depth=1, direction="up")
    focus = next(n for n in result.nodes if n["fqn"] == "PUBLIC.A")
    assert focus["object_type"] == "VIEW"
    assert focus["file_path"] == "a.sql"
