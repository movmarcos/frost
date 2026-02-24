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
        """Nodes that only appear as targets get type EXTERNAL."""
        edges = [
            {"source": "A", "target": "B", "type": "reads", "object_type": "PROC"},
        ]
        html = generate_html(edges)
        m = re.search(r"const allNodes = (\[.*?\]);", html, re.DOTALL)
        nodes = json.loads(m.group(1))
        b_node = [n for n in nodes if n["id"] == "B"][0]
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
