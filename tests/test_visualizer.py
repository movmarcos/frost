"""Tests for frost.visualizer -- HTML lineage graph generation."""

import json
import re

import pytest

from frost.visualizer import edges_from_rows, generate_html


# ------------------------------------------------------------------
# edges_from_rows
# ------------------------------------------------------------------

class TestEdgesFromRows:
    """Convert raw OBJECT_LINEAGE rows to the edge list format."""

    def test_basic_conversion(self):
        rows = [
            (1, "PUBLIC.PROC_A", "PROCEDURE", "READS", "PUBLIC.TABLE_X",
             "objects/proc_a.sql", None, "2025-01-01", "ADMIN"),
            (2, "PUBLIC.PROC_A", "PROCEDURE", "WRITES", "PUBLIC.TABLE_Y",
             "objects/proc_a.sql", None, "2025-01-01", "ADMIN"),
        ]
        edges = edges_from_rows(rows)
        assert len(edges) == 2
        assert edges[0] == {
            "source": "PUBLIC.PROC_A",
            "object_type": "PROCEDURE",
            "type": "reads",
            "target": "PUBLIC.TABLE_X",
        }
        assert edges[1]["type"] == "writes"
        assert edges[1]["target"] == "PUBLIC.TABLE_Y"

    def test_dependency_edge(self):
        rows = [
            (1, "PUBLIC.VIEW_A", "VIEW", "DEPENDENCY", "PUBLIC.TABLE_B",
             "objects/view_a.sql", None, "2025-01-01", "ADMIN"),
        ]
        edges = edges_from_rows(rows)
        assert edges[0]["type"] == "dependency"

    def test_empty_rows(self):
        assert edges_from_rows([]) == []

    def test_edge_type_lowercased(self):
        """OBJECT_LINEAGE stores UPPER; we normalise to lower."""
        rows = [
            (1, "A", "TABLE", "WRITES", "B", "", None, "", ""),
        ]
        edges = edges_from_rows(rows)
        assert edges[0]["type"] == "writes"


# ------------------------------------------------------------------
# generate_html
# ------------------------------------------------------------------

class TestGenerateHtml:
    """Produce a self-contained HTML page from edge data."""

    SAMPLE_EDGES = [
        {"source": "PUBLIC.PROC_A", "target": "PUBLIC.TABLE_X",
         "type": "reads", "object_type": "PROCEDURE"},
        {"source": "PUBLIC.PROC_A", "target": "PUBLIC.TABLE_Y",
         "type": "writes", "object_type": "PROCEDURE"},
        {"source": "PUBLIC.PROC_A", "target": "PUBLIC.VIEW_Z",
         "type": "dependency", "object_type": "PROCEDURE"},
    ]

    def test_returns_valid_html(self):
        html = generate_html(self.SAMPLE_EDGES)
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_title_injected(self):
        html = generate_html(self.SAMPLE_EDGES, title="My Test Graph")
        assert "<title>My Test Graph</title>" in html

    def test_nodes_json_embedded(self):
        html = generate_html(self.SAMPLE_EDGES)
        # Extract the nodes JSON array from the script block
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        assert m, "nodes JSON not found in HTML"
        nodes = json.loads(m.group(1))
        ids = {n["id"] for n in nodes}
        assert "PUBLIC.PROC_A" in ids
        assert "PUBLIC.TABLE_X" in ids
        assert "PUBLIC.TABLE_Y" in ids
        assert "PUBLIC.VIEW_Z" in ids

    def test_links_json_embedded(self):
        html = generate_html(self.SAMPLE_EDGES)
        m = re.search(r"const allLinks = (\[.*?\]);", html, re.DOTALL)
        assert m, "links JSON not found in HTML"
        links = json.loads(m.group(1))
        assert len(links) == 3
        types = {l["type"] for l in links}
        assert types == {"reads", "writes", "dependency"}

    def test_dataflow_dependency_arrow_reversed(self):
        """dependency edge: VIEW depends on TABLE -> arrow TABLE -> VIEW."""
        edges = [
            {"source": "PUBLIC.VIEW_A", "target": "PUBLIC.TABLE_B",
             "type": "dependency", "object_type": "VIEW"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allLinks = (\[.*?\]);", html, re.DOTALL)
        links = json.loads(m.group(1))
        link = links[0]
        assert link["source"] == "PUBLIC.TABLE_B"
        assert link["target"] == "PUBLIC.VIEW_A"

    def test_dataflow_reads_arrow_reversed(self):
        """reads edge: PROC reads TABLE -> arrow TABLE -> PROC."""
        edges = [
            {"source": "PUBLIC.PROC_A", "target": "PUBLIC.TABLE_X",
             "type": "reads", "object_type": "PROCEDURE"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allLinks = (\[.*?\]);", html, re.DOTALL)
        links = json.loads(m.group(1))
        link = links[0]
        assert link["source"] == "PUBLIC.TABLE_X"
        assert link["target"] == "PUBLIC.PROC_A"

    def test_dataflow_writes_arrow_unchanged(self):
        """writes edge: PROC writes TABLE -> arrow PROC -> TABLE (kept)."""
        edges = [
            {"source": "PUBLIC.PROC_A", "target": "PUBLIC.TABLE_Y",
             "type": "writes", "object_type": "PROCEDURE"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allLinks = (\[.*?\]);", html, re.DOTALL)
        links = json.loads(m.group(1))
        link = links[0]
        assert link["source"] == "PUBLIC.PROC_A"
        assert link["target"] == "PUBLIC.TABLE_Y"

    def test_d3_cdn_included(self):
        html = generate_html(self.SAMPLE_EDGES)
        assert "d3js.org" in html or "d3.v7" in html

    def test_node_types_propagated(self):
        html = generate_html(self.SAMPLE_EDGES)
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        proc_nodes = [n for n in nodes if n["id"] == "PUBLIC.PROC_A"]
        assert proc_nodes[0]["type"] == "PROCEDURE"

    def test_external_node_type(self):
        """Nodes that only appear as targets with no node_types get EXTERNAL."""
        edges = [
            {"source": "A", "target": "B", "type": "reads", "object_type": "PROC"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        b_node = [n for n in nodes if n["id"] == "B"][0]
        assert b_node["type"] == "EXTERNAL"

    def test_node_types_override_external(self):
        """node_types dict gives target-only objects their real type."""
        edges = [
            {"source": "PUBLIC.PROC_A", "target": "PUBLIC.TABLE_X",
             "type": "reads", "object_type": "PROCEDURE"},
        ]
        html = generate_html(edges, node_types={
            "PUBLIC.PROC_A": "PROCEDURE",
            "PUBLIC.TABLE_X": "TABLE",
        })
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        tbl = [n for n in nodes if n["id"] == "PUBLIC.TABLE_X"][0]
        assert tbl["type"] == "TABLE"

    def test_target_typed_from_other_edge_source(self):
        """Object that is both a source and a target gets type from its source edge."""
        edges = [
            {"source": "PUBLIC.VIEW_A", "target": "PUBLIC.TABLE_X",
             "type": "dependency", "object_type": "VIEW"},
            {"source": "PUBLIC.TABLE_X", "target": "PUBLIC.SEQ_1",
             "type": "dependency", "object_type": "TABLE"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        tbl = [n for n in nodes if n["id"] == "PUBLIC.TABLE_X"][0]
        assert tbl["type"] == "TABLE"

    def test_node_types_empty_dict(self):
        """Empty node_types dict falls back to edge-based logic."""
        edges = [
            {"source": "A", "target": "B", "type": "reads", "object_type": "PROCEDURE"},
        ]
        html = generate_html(edges, node_types={})
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        a_node = [n for n in nodes if n["id"] == "A"][0]
        b_node = [n for n in nodes if n["id"] == "B"][0]
        assert a_node["type"] == "PROCEDURE"
        assert b_node["type"] == "EXTERNAL"

    def test_empty_edges(self):
        """Empty edges still produce valid HTML with empty arrays."""
        html = generate_html([])
        assert "const allNodes = [];" in html
        assert "const allLinks = [];" in html

    def test_duplicate_nodes_deduped(self):
        """Same object appearing multiple times results in one node."""
        edges = [
            {"source": "A", "target": "B", "type": "reads", "object_type": "TABLE"},
            {"source": "A", "target": "C", "type": "writes", "object_type": "TABLE"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        ids = [n["id"] for n in nodes]
        assert ids.count("A") == 1

    def test_filter_controls_present(self):
        """Pipeline layout has type/edge filter buttons and toolbar."""
        html = generate_html(self.SAMPLE_EDGES)
        assert 'id="type-filters"' in html
        assert 'data-edge="reads"' in html
        assert 'data-edge="writes"' in html
        assert 'data-edge="dependency"' in html
        assert 'data-dir="upstream"' in html
        assert 'data-dir="downstream"' in html
        assert 'id="depth-range"' in html
        # Type colour map present
        assert "TABLE" in html
        assert "PROCEDURE" in html

    def test_search_input_present(self):
        html = generate_html(self.SAMPLE_EDGES)
        assert 'id="search"' in html


# ------------------------------------------------------------------
# Database tree panel
# ------------------------------------------------------------------

class TestDatabaseTreePanel:
    """The HTML should include a tree panel sidebar."""

    EDGES = [
        {"source": "MYDB.PUBLIC.T1", "target": "MYDB.PUBLIC.V1",
         "type": "dependency", "object_type": "TABLE"},
        {"source": "MYDB.RAW.STAGE_X", "target": "MYDB.PUBLIC.T1",
         "type": "reads", "object_type": "STAGE"},
    ]

    def test_tree_panel_present(self):
        html = generate_html(self.EDGES)
        assert 'id="tree-panel"' in html

    def test_tree_toggle_button(self):
        html = generate_html(self.EDGES)
        assert 'id="tree-toggle"' in html

    def test_tree_contains_object_explorer(self):
        html = generate_html(self.EDGES)
        assert "Object Explorer" in html

    def test_tree_panel_css(self):
        html = generate_html(self.EDGES)
        assert "#tree-panel" in html
        assert ".tree-db" in html
        assert ".tree-schema" in html
        assert ".tree-type" in html
        assert ".tree-obj" in html


# ------------------------------------------------------------------
# Focus object (--object flag)
# ------------------------------------------------------------------

class TestFocusObject:
    """generate_html(focus_object=...) pre-selects a node."""

    EDGES = [
        {"source": "DB.PUBLIC.A", "target": "DB.PUBLIC.B",
         "type": "dependency", "object_type": "TABLE"},
    ]

    def test_focus_null_by_default(self):
        html = generate_html(self.EDGES)
        assert "const focusObject = null;" in html

    def test_focus_object_injected(self):
        html = generate_html(self.EDGES, focus_object="DB.PUBLIC.A")
        assert '"DB.PUBLIC.A"' in html
        assert "const focusObject =" in html

    def test_focus_object_uppercased(self):
        html = generate_html(self.EDGES, focus_object="db.public.a")
        assert '"DB.PUBLIC.A"' in html


# ------------------------------------------------------------------
# Depth-based neighbourhood focus
# ------------------------------------------------------------------

class TestDepthFocus:
    """Click-to-focus with depth control."""

    EDGES = [
        {"source": "DB.PUBLIC.A", "target": "DB.PUBLIC.B",
         "type": "dependency", "object_type": "TABLE"},
        {"source": "DB.PUBLIC.B", "target": "DB.PUBLIC.C",
         "type": "dependency", "object_type": "VIEW"},
    ]

    def test_initial_depth_default_is_1(self):
        """Default initial_depth=1 is embedded."""
        html = generate_html(self.EDGES)
        assert "const INITIAL_DEPTH = 1;" in html

    def test_initial_depth_custom_value(self):
        html = generate_html(self.EDGES, initial_depth=3)
        assert "const INITIAL_DEPTH = 3;" in html

    def test_initial_depth_minimum_clamped_to_1(self):
        html = generate_html(self.EDGES, initial_depth=0)
        assert "const INITIAL_DEPTH = 1;" in html

    def test_max_depth_constant_in_js(self):
        html = generate_html(self.EDGES)
        assert "const MAX_DEPTH = 20;" in html

    def test_focus_with_depth(self):
        """Focus object + custom depth are both injected."""
        html = generate_html(self.EDGES, focus_object="DB.PUBLIC.A",
                             initial_depth=2)
        assert '"DB.PUBLIC.A"' in html
        assert "const INITIAL_DEPTH = 2;" in html

    def test_bidirectional_bfs_code_present(self):
        """The JS includes bidirectional BFS for direction=all."""
        html = generate_html(self.EDGES)
        assert 'direction === "upstream" || direction === "all"' in html
        assert 'direction === "downstream" || direction === "all"' in html

    def test_depth_resets_on_click(self):
        """JS click handler sets maxDepth = INITIAL_DEPTH."""
        html = generate_html(self.EDGES)
        assert "maxDepth = INITIAL_DEPTH;" in html

    def test_depth_resets_on_deselect(self):
        """JS deselect sets maxDepth = MAX_DEPTH."""
        html = generate_html(self.EDGES)
        assert "maxDepth = MAX_DEPTH;" in html


# ------------------------------------------------------------------
# Card sizing
# ------------------------------------------------------------------

class TestCardSizing:
    """Cards should be wide enough to display long object names."""

    EDGES = [
        {"source": "DB.PUBLIC.A", "target": "DB.PUBLIC.B",
         "type": "dependency", "object_type": "TABLE"},
    ]

    def test_card_width_is_280(self):
        html = generate_html(self.EDGES)
        assert "CARD_W = 280" in html

    def test_name_truncation_at_38_chars(self):
        html = generate_html(self.EDGES)
        assert 'd.id.length > 38' in html
        assert 'd.id.slice(-37)' in html


# ------------------------------------------------------------------
# Node columns
# ------------------------------------------------------------------

class TestNodeColumns:
    """Column metadata is injected and displayed in the detail panel."""

    EDGES = [
        {"source": "PUBLIC.TABLE_A", "target": "PUBLIC.VIEW_B",
         "type": "dependency", "object_type": "TABLE"},
    ]

    def test_node_columns_injected_as_js_variable(self):
        cols = {"PUBLIC.TABLE_A": [{"name": "COL_1", "type": "NUMBER"}, {"name": "COL_2", "type": "VARCHAR(255)"}, {"name": "COL_3", "type": "DATE"}]}
        html = generate_html(self.EDGES, node_columns=cols)
        assert "const nodeColumns =" in html
        m = re.search(r"const nodeColumns = ({.*?});", html, re.DOTALL)
        assert m
        parsed = json.loads(m.group(1))
        assert parsed["PUBLIC.TABLE_A"][0]["name"] == "COL_1"
        assert parsed["PUBLIC.TABLE_A"][0]["type"] == "NUMBER"

    def test_node_columns_default_empty(self):
        html = generate_html(self.EDGES)
        assert "const nodeColumns = {};" in html

    def test_columns_section_in_detail_panel(self):
        html = generate_html(self.EDGES,
                             node_columns={"PUBLIC.TABLE_A": [{"name": "ID", "type": "NUMBER"}, {"name": "NAME", "type": "VARCHAR"}]})
        assert 'id="det-cols-section"' in html
        assert 'id="det-cols"' in html

    def test_columns_section_hidden_by_default(self):
        html = generate_html(self.EDGES)
        assert 'id="det-cols-section" style="display:none"' in html

    def test_open_detail_populates_columns(self):
        """The openDetail JS function renders column name and type."""
        cols = {"PUBLIC.TABLE_A": [{"name": "A", "type": "INT"}, {"name": "B", "type": "TEXT"}]}
        html = generate_html(self.EDGES, node_columns=cols)
        assert "nodeColumns[d.id]" in html
        assert "c.name" in html
        assert "c.type" in html
        assert "col-name" in html
        assert "col-type" in html

    def test_columns_css_monospace(self):
        html = generate_html(self.EDGES,
                             node_columns={"PUBLIC.TABLE_A": [{"name": "X", "type": "INT"}]})
        assert ".det-cols-list" in html
        assert "monospace" in html
        assert "col-name" in html
        assert "col-type" in html
