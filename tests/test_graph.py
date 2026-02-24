"""Tests for frost.graph -- dependency graph, topological sort, cycle detection."""

import pytest

from frost.graph import DependencyGraph, CycleError
from frost.parser import ObjectDefinition


def _obj(fqn: str, deps=None, obj_type: str = "TABLE") -> ObjectDefinition:
    """Helper to create a minimal ObjectDefinition."""
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
        raw_sql=f"CREATE OR ALTER {obj_type} {fqn} (id INT);",
        resolved_sql=f"CREATE OR ALTER {obj_type} {fqn} (id INT);",
        dependencies=set(deps or []),
    )


# ------------------------------------------------------------------
# Basic ordering
# ------------------------------------------------------------------

def test_single_object():
    """A graph with one object should return it."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.T1"))
    g.build()
    order = g.resolve_order()
    assert len(order) == 1
    assert order[0].fqn == "PUBLIC.T1"


def test_two_objects_no_deps():
    """Two independent objects should both appear (order is stable)."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A"))
    g.add_object(_obj("PUBLIC.B"))
    g.build()
    order = g.resolve_order()
    assert len(order) == 2
    fqns = {o.fqn for o in order}
    assert fqns == {"PUBLIC.A", "PUBLIC.B"}


def test_dependency_chain():
    """A -> B -> C: C must come first, then B, then A."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.C"))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.C"]))
    g.add_object(_obj("PUBLIC.A", deps=["PUBLIC.B"]))
    g.build()
    order = g.resolve_order()

    fqns = [o.fqn for o in order]
    assert fqns.index("PUBLIC.C") < fqns.index("PUBLIC.B")
    assert fqns.index("PUBLIC.B") < fqns.index("PUBLIC.A")


def test_diamond_dependency():
    """Diamond: D depends on B and C; B and C both depend on A."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A"))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.add_object(_obj("PUBLIC.C", deps=["PUBLIC.A"]))
    g.add_object(_obj("PUBLIC.D", deps=["PUBLIC.B", "PUBLIC.C"]))
    g.build()
    order = g.resolve_order()

    fqns = [o.fqn for o in order]
    assert fqns.index("PUBLIC.A") < fqns.index("PUBLIC.B")
    assert fqns.index("PUBLIC.A") < fqns.index("PUBLIC.C")
    assert fqns.index("PUBLIC.B") < fqns.index("PUBLIC.D")
    assert fqns.index("PUBLIC.C") < fqns.index("PUBLIC.D")


# ------------------------------------------------------------------
# Cycle detection
# ------------------------------------------------------------------

def test_simple_cycle_raises():
    """A -> B -> A should raise CycleError."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A", deps=["PUBLIC.B"]))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.build()

    with pytest.raises(CycleError) as exc_info:
        g.resolve_order()

    assert len(exc_info.value.cycle) >= 2


def test_three_node_cycle():
    """A -> B -> C -> A should raise CycleError."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A", deps=["PUBLIC.C"]))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.add_object(_obj("PUBLIC.C", deps=["PUBLIC.B"]))
    g.build()

    with pytest.raises(CycleError):
        g.resolve_order()


# ------------------------------------------------------------------
# External dependencies (not in graph) are ignored
# ------------------------------------------------------------------

def test_external_dependency_ignored():
    """Dependencies on objects not in the graph should be silently ignored."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.V1", deps=["PUBLIC.EXTERNAL_TABLE"], obj_type="VIEW"))
    g.build()
    order = g.resolve_order()
    assert len(order) == 1


# ------------------------------------------------------------------
# get_dependents / get_dependencies
# ------------------------------------------------------------------

def test_get_dependents():
    """get_dependents should return all transitive dependents."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A"))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.add_object(_obj("PUBLIC.C", deps=["PUBLIC.B"]))
    g.build()

    dependents = g.get_dependents("PUBLIC.A")
    assert dependents == {"PUBLIC.B", "PUBLIC.C"}


def test_get_dependencies():
    """get_dependencies should return all transitive dependencies."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A"))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.add_object(_obj("PUBLIC.C", deps=["PUBLIC.B"]))
    g.build()

    deps = g.get_dependencies("PUBLIC.C")
    assert deps == {"PUBLIC.A", "PUBLIC.B"}


# ------------------------------------------------------------------
# Visualise
# ------------------------------------------------------------------

def test_visualize_no_cycle():
    """visualize() should return a string for a valid graph."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.T1"))
    g.add_object(_obj("PUBLIC.V1", deps=["PUBLIC.T1"], obj_type="VIEW"))
    g.build()

    text = g.visualize()
    assert "Execution Plan" in text
    assert "PUBLIC.T1" in text
    assert "PUBLIC.V1" in text


def test_visualize_with_cycle():
    """visualize() should show an error for cyclic graphs."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A", deps=["PUBLIC.B"]))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.build()

    text = g.visualize()
    assert "ERROR" in text


# ------------------------------------------------------------------
# Lineage support
# ------------------------------------------------------------------

from frost.lineage import LineageEntry


def test_add_lineage():
    """add_lineage() stores entries accessible via .lineage property."""
    g = DependencyGraph()
    entry = LineageEntry(
        object_fqn="PUBLIC.MY_PROC",
        file_path="proc.sql",
        sources=["PUBLIC.T1"],
        targets=["PUBLIC.T2"],
        description="test desc",
    )
    g.add_lineage(entry)
    assert "PUBLIC.MY_PROC" in g.lineage
    assert g.lineage["PUBLIC.MY_PROC"].description == "test desc"


def test_lineage_property_is_copy():
    """The .lineage property should return a copy, not the internal dict."""
    g = DependencyGraph()
    entry = LineageEntry(object_fqn="PUBLIC.P", file_path="p.sql", sources=["T1"])
    g.add_lineage(entry)
    copy = g.lineage
    copy["EXTRA"] = entry
    assert "EXTRA" not in g.lineage


def test_get_all_edges_dependency_only():
    """get_all_edges() returns dependency edges when no lineage."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.A"))
    g.add_object(_obj("PUBLIC.B", deps=["PUBLIC.A"]))
    g.build()

    edges = g.get_all_edges()
    assert len(edges) == 1
    assert edges[0]["source"] == "PUBLIC.B"
    assert edges[0]["target"] == "PUBLIC.A"
    assert edges[0]["type"] == "dependency"


def test_get_all_edges_with_lineage():
    """get_all_edges() returns both dependency and lineage edges."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.T1"))
    g.add_object(_obj("PUBLIC.PROC", deps=["PUBLIC.T1"], obj_type="PROCEDURE"))
    g.build()

    entry = LineageEntry(
        object_fqn="PUBLIC.PROC",
        file_path="proc.sql",
        sources=["PUBLIC.INPUT"],
        targets=["PUBLIC.OUTPUT"],
    )
    g.add_lineage(entry)

    edges = g.get_all_edges()
    types = {e["type"] for e in edges}
    assert "dependency" in types
    assert "reads" in types
    assert "writes" in types
    assert len(edges) == 3


def test_get_all_edges_empty_graph():
    """get_all_edges() returns empty list for an empty graph."""
    g = DependencyGraph()
    assert g.get_all_edges() == []


def test_visualize_shows_lineage_auto_detected():
    """visualize() should show '(auto-detected)' tag for auto-detected lineage."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.PROC", obj_type="PROCEDURE"))
    g.build()

    entry = LineageEntry(
        object_fqn="PUBLIC.PROC",
        file_path="proc.sql",
        sources=["PUBLIC.ORDERS"],
        targets=["PUBLIC.SUMMARY"],
        description="Aggregates data",
        auto_detected=True,
    )
    g.add_lineage(entry)

    text = g.visualize()
    assert "Procedure Lineage" in text
    assert "(auto-detected)" in text
    assert "reads from" in text
    assert "writes to" in text
    assert "PUBLIC.ORDERS" in text
    assert "PUBLIC.SUMMARY" in text
    assert "Aggregates data" in text


def test_visualize_shows_lineage_declared():
    """visualize() should show '(declared)' tag for YAML-declared lineage."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.PROC", obj_type="PROCEDURE"))
    g.build()

    entry = LineageEntry(
        object_fqn="PUBLIC.PROC",
        file_path="proc.sql",
        sources=["PUBLIC.T1"],
        auto_detected=False,
    )
    g.add_lineage(entry)

    text = g.visualize()
    assert "(declared)" in text


def test_visualize_no_lineage_section_when_empty():
    """visualize() should NOT include lineage section when no lineage exists."""
    g = DependencyGraph()
    g.add_object(_obj("PUBLIC.T1"))
    g.build()

    text = g.visualize()
    assert "Procedure Lineage" not in text
