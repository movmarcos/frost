"""Interactive HTML lineage visualiser.

Generates a standalone HTML page with a pipeline-style DAG layout
showing object dependencies and data-flow lineage.  Nodes are arranged
in columns (left → right) based on topological depth, with cards
showing type icons and names.  The page supports:

* **Direction toggle** -- Upstream / Downstream from a selected node
* **Type filters** -- show/hide TABLE, VIEW, PROCEDURE, FUNCTION, etc.
* **Edge-type filters** -- show/hide Dependency, Reads, Writes
* **Depth control** -- limit how many levels to display
* **Search** -- filter objects by name
* **Zoom & pan** -- scroll and drag to navigate large graphs

Two data sources are supported:

* **Remote**: rows queried from ``FROST.OBJECT_LINEAGE`` in Snowflake.
* **Local**: edges from ``DependencyGraph.get_all_edges()`` (no
  Snowflake connection required).
"""

import json
import logging
import webbrowser
from pathlib import Path
from typing import Dict, List, Optional, Sequence

log = logging.getLogger("frost")

# ------------------------------------------------------------------
# Public helpers
# ------------------------------------------------------------------


def edges_from_rows(rows: Sequence[tuple]) -> List[dict]:
    """Convert raw ``OBJECT_LINEAGE`` rows to the internal edge format.

    Expected column order (from ``ensure_lineage_table``):
        id, object_fqn, object_type, edge_type, related_fqn,
        file_path, description, recorded_at, recorded_by
    """
    edges: List[dict] = []
    for row in rows:
        edges.append({
            "source": row[1],       # object_fqn
            "object_type": row[2],  # object_type
            "type": row[3].lower(), # edge_type -> dependency|reads|writes
            "target": row[4],       # related_fqn
        })
    return edges


def generate_html(
    edges: List[dict],
    title: str = "frost · Lineage",
) -> str:
    """Return a self-contained HTML page with an interactive graph."""

    # Collect unique nodes with their types
    node_map: Dict[str, str] = {}  # fqn -> object_type
    for e in edges:
        src = e["source"]
        tgt = e["target"]
        if src not in node_map:
            node_map[src] = e.get("object_type", "UNKNOWN")
        if tgt not in node_map:
            # Target might not carry its own type -- mark as referenced
            node_map[tgt] = node_map.get(tgt, "EXTERNAL")

    nodes = [{"id": fqn, "type": otype} for fqn, otype in node_map.items()]
    links = [
        {"source": e["source"], "target": e["target"], "type": e["type"].lower()}
        for e in edges
    ]

    nodes_json = json.dumps(nodes)
    links_json = json.dumps(links)

    return _HTML_TEMPLATE.replace("__NODES__", nodes_json).replace(
        "__LINKS__", links_json
    ).replace("__TITLE__", title)


def write_and_open(html: str, output: str) -> Path:
    """Write *html* to *output* and open in default browser."""
    path = Path(output).resolve()
    path.write_text(html, encoding="utf-8")
    log.info("Lineage visual written to %s", path)
    webbrowser.open(path.as_uri())
    return path


# ------------------------------------------------------------------
# HTML template -- pipeline-style DAG layout
# ------------------------------------------------------------------

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>__TITLE__</title>
<style>
:root{
  --bg:#0f172a;--surface:#1e293b;--border:#334155;--border2:#475569;
  --text:#e2e8f0;--muted:#94a3b8;--dim:#64748b;
  --ice:#38bdf8;--purple:#a78bfa;--green:#34d399;--orange:#fb923c;
  --red:#f87171;--yellow:#facc15;--teal:#2dd4bf;--pink:#f472b6;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
     background:var(--bg);color:var(--text);overflow:hidden;height:100vh}

/* ── Top bar ─────────────────────────────────────── */
#topbar{
  position:fixed;top:0;left:0;right:0;height:52px;background:var(--surface);
  display:flex;align-items:center;padding:0 20px;z-index:100;
  border-bottom:1px solid var(--border);gap:16px;
}
#topbar h1{font-size:17px;font-weight:700;color:var(--ice);white-space:nowrap}
#topbar h1 span{color:var(--muted);font-weight:400;font-size:13px;margin-left:6px}
.stats{font-size:12px;color:var(--dim);white-space:nowrap;margin-left:auto}

/* ── Toolbar (direction, filters, depth, search) ── */
#toolbar{
  position:fixed;top:52px;left:0;right:0;height:44px;background:var(--surface);
  display:flex;align-items:center;padding:0 20px;z-index:99;
  border-bottom:1px solid var(--border);gap:14px;font-size:13px;
}
.tb-group{display:flex;align-items:center;gap:6px}
.tb-label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.5px;
           margin-right:2px;white-space:nowrap}
.tb-btn{
  background:var(--bg);border:1px solid var(--border);border-radius:4px;
  color:var(--muted);padding:3px 10px;cursor:pointer;font-size:12px;
  transition:all .15s;white-space:nowrap;
}
.tb-btn:hover{border-color:var(--border2);color:var(--text)}
.tb-btn.active{background:var(--ice);color:var(--bg);border-color:var(--ice);font-weight:600}
.tb-sep{width:1px;height:24px;background:var(--border);flex-shrink:0}

#search{
  background:var(--bg);border:1px solid var(--border);border-radius:4px;
  color:var(--text);padding:4px 10px;font-size:12px;width:180px;outline:none;
}
#search:focus{border-color:var(--ice)}
#search::placeholder{color:var(--dim)}

#depth-val{display:inline-block;min-width:20px;text-align:center;color:var(--ice);
           font-weight:600;font-size:13px}
#depth-range{width:80px;accent-color:var(--ice)}

/* ── Canvas ──────────────────────────────────────── */
#canvas-wrap{
  position:fixed;top:96px;left:0;right:0;bottom:0;overflow:hidden;
}
svg{width:100%;height:100%}

/* ── Node cards ──────────────────────────────────── */
.node-card{cursor:pointer}
.node-card rect{rx:6;ry:6;transition:opacity .2s}
.node-card .card-icon{font-size:14px}
.node-card .card-type{font-size:9px;text-transform:uppercase;letter-spacing:.4px}
.node-card .card-name{font-size:12px;font-weight:600}
.node-card .card-badge{font-size:9px;font-weight:600}

/* ── Tooltip ─────────────────────────────────────── */
#tooltip{
  position:fixed;pointer-events:none;background:var(--surface);
  border:1px solid var(--border2);border-radius:8px;
  padding:12px 16px;font-size:12px;display:none;z-index:200;
  max-width:380px;line-height:1.6;box-shadow:0 8px 24px rgba(0,0,0,.4);
}
#tooltip .tt-head{font-size:14px;font-weight:700;color:var(--ice);margin-bottom:2px}
#tooltip .tt-type{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.3px}
#tooltip .tt-section{margin-top:6px;color:var(--muted);font-size:11px}
#tooltip .tt-list{color:var(--text);font-size:12px;margin:2px 0 0 4px}

/* ── Selected-object panel ───────────────────────── */
#detail{
  position:fixed;top:96px;right:0;width:0;bottom:0;background:var(--surface);
  border-left:1px solid var(--border);z-index:50;overflow-y:auto;
  transition:width .25s;padding:0;font-size:13px;
}
#detail.open{width:320px;padding:20px}
#detail h2{font-size:15px;color:var(--ice);margin-bottom:4px}
#detail .det-type{color:var(--muted);font-size:11px;text-transform:uppercase;margin-bottom:14px}
#detail .det-section{font-size:11px;color:var(--dim);text-transform:uppercase;
                     letter-spacing:.5px;margin:12px 0 4px}
#detail ul{list-style:none;padding:0}
#detail li{padding:3px 0;color:var(--text);font-size:12px;border-bottom:1px solid var(--border)}
#detail li:last-child{border-bottom:none}
#detail .det-close{position:absolute;top:14px;right:14px;background:none;border:none;
                   color:var(--muted);cursor:pointer;font-size:18px}
</style>
</head>
<body>

<!-- ── Top bar ────────────────────────────────────── -->
<div id="topbar">
  <h1>frost<span>lineage</span></h1>
  <div class="stats" id="stats"></div>
</div>

<!-- ── Toolbar ────────────────────────────────────── -->
<div id="toolbar">
  <div class="tb-group">
    <span class="tb-label">Direction</span>
    <button class="tb-btn active" data-dir="all">All</button>
    <button class="tb-btn" data-dir="upstream">Upstream</button>
    <button class="tb-btn" data-dir="downstream">Downstream</button>
  </div>
  <div class="tb-sep"></div>
  <div class="tb-group" id="type-filters"></div>
  <div class="tb-sep"></div>
  <div class="tb-group">
    <span class="tb-label">Edges</span>
    <button class="tb-btn active" data-edge="dependency">Dep</button>
    <button class="tb-btn active" data-edge="reads">Reads</button>
    <button class="tb-btn active" data-edge="writes">Writes</button>
  </div>
  <div class="tb-sep"></div>
  <div class="tb-group">
    <span class="tb-label">Depth</span>
    <input type="range" id="depth-range" min="1" max="20" value="20"/>
    <span id="depth-val">∞</span>
  </div>
  <div class="tb-sep"></div>
  <input id="search" type="text" placeholder="Search objects…" autocomplete="off"/>
</div>

<!-- ── Canvas ─────────────────────────────────────── -->
<div id="canvas-wrap">
  <svg id="canvas"></svg>
</div>

<!-- ── Detail panel ───────────────────────────────── -->
<div id="detail">
  <button class="det-close" id="det-close">&times;</button>
  <h2 id="det-name"></h2>
  <div class="det-type" id="det-type"></div>
  <div class="det-section">Reads from</div>
  <ul id="det-reads"></ul>
  <div class="det-section">Writes to</div>
  <ul id="det-writes"></ul>
  <div class="det-section">Depends on</div>
  <ul id="det-deps"></ul>
  <div class="det-section">Used by</div>
  <ul id="det-usedby"></ul>
</div>

<!-- ── Tooltip ────────────────────────────────────── -->
<div id="tooltip"></div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
"use strict";

// ── Data (injected by frost) ─────────────────────
const allNodes = __NODES__;
const allLinks = __LINKS__;

// ── Colour / icon maps ──────────────────────────
const typeStyle = {
  TABLE:       {color:"#38bdf8", icon:"▦"},
  VIEW:        {color:"#34d399", icon:"◈"},
  PROCEDURE:   {color:"#a78bfa", icon:"⚙"},
  FUNCTION:    {color:"#fb923c", icon:"ƒ"},
  TASK:        {color:"#facc15", icon:"⏱"},
  STREAM:      {color:"#2dd4bf", icon:"⇶"},
  STAGE:       {color:"#818cf8", icon:"◎"},
  SEQUENCE:    {color:"#f472b6", icon:"#"},
  FILE_FORMAT: {color:"#c084fc", icon:"❖"},
  PIPE:        {color:"#22d3ee", icon:"⇒"},
  EXTERNAL:    {color:"#64748b", icon:"☁"},
  UNKNOWN:     {color:"#64748b", icon:"?"},
};
const defaultStyle = {color:"#94a3b8", icon:"•"};
const ts = t => typeStyle[(t||"").toUpperCase()] || defaultStyle;

const edgeColor = {dependency:"#475569", reads:"#38bdf8", writes:"#f87171"};

// ── Card dimensions ─────────────────────────────
const CARD_W = 200, CARD_H = 56, COL_GAP = 100, ROW_GAP = 20;

// ── State ───────────────────────────────────────
let selectedNode = null;
let direction = "all";    // all | upstream | downstream
let activeEdgeTypes = new Set(["dependency","reads","writes"]);
let activeNodeTypes = new Set();
let maxDepth = 20;

// ── Build type filter buttons ───────────────────
const allTypes = [...new Set(allNodes.map(n => (n.type||"UNKNOWN").toUpperCase()))].sort();
allTypes.forEach(t => activeNodeTypes.add(t));
const tfDiv = document.getElementById("type-filters");
tfDiv.innerHTML = '<span class="tb-label">Types</span>';
allTypes.forEach(t => {
  const b = document.createElement("button");
  b.className = "tb-btn active";
  b.dataset.ntype = t;
  b.textContent = t.charAt(0) + t.slice(1).toLowerCase();
  b.style.borderLeft = `3px solid ${ts(t).color}`;
  tfDiv.appendChild(b);
});

// ── Stats ───────────────────────────────────────
const nReads = allLinks.filter(l => l.type === "reads").length;
const nWrites = allLinks.filter(l => l.type === "writes").length;
document.getElementById("stats").textContent =
  `${allNodes.length} objects · ${allLinks.length} edges (${nReads} reads, ${nWrites} writes)`;

// ── SVG setup ───────────────────────────────────
const svg = d3.select("#canvas");
const gRoot = svg.append("g");
const zoom = d3.zoom().scaleExtent([0.08, 4]).on("zoom", e => gRoot.attr("transform", e.transform));
svg.call(zoom);

// Defs: arrow markers + card shadow
const defs = svg.append("defs");
["dependency","reads","writes"].forEach(t => {
  defs.append("marker").attr("id",`arr-${t}`).attr("viewBox","0 -5 10 10")
    .attr("refX",10).attr("refY",0).attr("markerWidth",7).attr("markerHeight",7)
    .attr("orient","auto").append("path").attr("d","M0,-4L10,0L0,4").attr("fill",edgeColor[t]);
});
// Card shadow
defs.append("filter").attr("id","shadow").attr("x","-10%").attr("y","-10%")
    .attr("width","130%").attr("height","140%")
  .append("feDropShadow").attr("dx",0).attr("dy",2).attr("stdDeviation",4)
    .attr("flood-color","rgba(0,0,0,.35)");

// ── Layout engine (topological layering) ────────
function computeLayout(nodes, links) {
  // Build adjacency: source -> [targets]
  const adj = new Map();
  const radj = new Map();
  const nodeById = new Map();
  nodes.forEach(n => { nodeById.set(n.id, n); adj.set(n.id, []); radj.set(n.id, []); });
  const linkNodeIds = new Set(nodes.map(n => n.id));
  links.forEach(l => {
    if (linkNodeIds.has(l.source) && linkNodeIds.has(l.target)) {
      adj.get(l.source).push(l.target);
      radj.get(l.target).push(l.source);
    }
  });

  // Assign layers via longest-path from roots
  const layer = new Map();
  const visited = new Set();
  function dfs(id, depth) {
    if (visited.has(id) && (layer.get(id) || 0) >= depth) return;
    visited.add(id);
    layer.set(id, Math.max(layer.get(id) || 0, depth));
    adj.get(id)?.forEach(t => dfs(t, depth + 1));
  }
  // Roots: nodes with no incoming edges
  nodes.forEach(n => { if ((radj.get(n.id) || []).length === 0) dfs(n.id, 0); });
  // Handle any not visited (cycles or isolated)
  nodes.forEach(n => { if (!visited.has(n.id)) { layer.set(n.id, 0); } });

  // Group by layer
  const layers = new Map();
  nodes.forEach(n => {
    const l = layer.get(n.id) || 0;
    if (!layers.has(l)) layers.set(l, []);
    layers.get(l).push(n);
  });

  // Assign x,y coordinates
  const sortedLayers = [...layers.keys()].sort((a,b) => a - b);
  sortedLayers.forEach((layerIdx, col) => {
    const group = layers.get(layerIdx);
    // Sort alphabetically within column for stability
    group.sort((a,b) => a.id.localeCompare(b.id));
    group.forEach((n, row) => {
      n.x = col * (CARD_W + COL_GAP) + 40;
      n.y = row * (CARD_H + ROW_GAP) + 40;
    });
  });
}

// ── Filter + render ─────────────────────────────
function getFilteredData() {
  let nodes = allNodes.filter(n => activeNodeTypes.has((n.type||"UNKNOWN").toUpperCase()));
  let links = allLinks.filter(l => activeEdgeTypes.has(l.type));

  const nodeIds = new Set(nodes.map(n => n.id));
  links = links.filter(l => nodeIds.has(l.source) && nodeIds.has(l.target));

  // Direction + depth from selected node
  if (selectedNode && direction !== "all") {
    const reachable = new Set();
    reachable.add(selectedNode);
    let frontier = [selectedNode];
    for (let d = 0; d < maxDepth && frontier.length; d++) {
      const next = [];
      frontier.forEach(id => {
        links.forEach(l => {
          if (direction === "upstream") {
            // upstream: who feeds into this node (follow target -> source)
            if (l.target === id && !reachable.has(l.source)) { reachable.add(l.source); next.push(l.source); }
          } else {
            // downstream: who does this node feed (follow source -> target)
            if (l.source === id && !reachable.has(l.target)) { reachable.add(l.target); next.push(l.target); }
          }
        });
      });
      frontier = next;
    }
    nodes = nodes.filter(n => reachable.has(n.id));
    const rSet = reachable;
    links = links.filter(l => rSet.has(l.source) && rSet.has(l.target));
  }

  // Search filter
  const q = document.getElementById("search").value.trim().toUpperCase();
  if (q) {
    const matching = new Set();
    nodes.forEach(n => { if (n.id.toUpperCase().includes(q)) matching.add(n.id); });
    // Include direct neighbours of matching nodes
    links.forEach(l => {
      if (matching.has(l.source)) matching.add(l.target);
      if (matching.has(l.target)) matching.add(l.source);
    });
    nodes = nodes.filter(n => matching.has(n.id));
    links = links.filter(l => matching.has(l.source) && matching.has(l.target));
  }

  return { nodes: nodes.map(n => ({...n})), links: links.map(l => ({...l})) };
}

function render() {
  gRoot.selectAll("*").remove();
  const {nodes, links} = getFilteredData();
  if (!nodes.length) return;

  computeLayout(nodes, links);

  const nodeById = new Map();
  nodes.forEach(n => nodeById.set(n.id, n));

  // ── Edges (curved paths) ──────────────────────
  const linkG = gRoot.append("g");
  linkG.selectAll("path").data(links).enter().append("path")
    .attr("fill","none")
    .attr("stroke", d => edgeColor[d.type] || "#475569")
    .attr("stroke-width", d => d.type === "dependency" ? 1.2 : 2)
    .attr("stroke-opacity", d => d.type === "dependency" ? 0.35 : 0.6)
    .attr("marker-end", d => `url(#arr-${d.type})`)
    .attr("d", d => {
      const s = nodeById.get(d.source);
      const t = nodeById.get(d.target);
      if (!s || !t) return "";
      const sx = s.x + CARD_W, sy = s.y + CARD_H / 2;
      const tx = t.x, ty = t.y + CARD_H / 2;
      const mx = (sx + tx) / 2;
      return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
    });

  // Edge type labels on hover paths
  linkG.selectAll("text").data(links).enter().append("text")
    .attr("fill", d => edgeColor[d.type]).attr("font-size","9px").attr("opacity",0)
    .attr("text-anchor","middle")
    .attr("x", d => {
      const s = nodeById.get(d.source), t = nodeById.get(d.target);
      return s && t ? (s.x + CARD_W + t.x) / 2 : 0;
    })
    .attr("y", d => {
      const s = nodeById.get(d.source), t = nodeById.get(d.target);
      return s && t ? (s.y + t.y + CARD_H) / 2 - 4 : 0;
    })
    .text(d => d.type.toUpperCase());

  // ── Node cards ────────────────────────────────
  const nodeG = gRoot.append("g");
  const card = nodeG.selectAll("g.node-card").data(nodes).enter()
    .append("g").attr("class","node-card")
    .attr("transform", d => `translate(${d.x},${d.y})`);

  // Background rect
  card.append("rect")
    .attr("width", CARD_W).attr("height", CARD_H)
    .attr("fill", d => {
      const c = ts(d.type).color;
      // Dark tinted card
      return d.id === selectedNode ? c + "33" : "#1e293b";
    })
    .attr("stroke", d => d.id === selectedNode ? ts(d.type).color : "#334155")
    .attr("stroke-width", d => d.id === selectedNode ? 2 : 1)
    .attr("filter","url(#shadow)");

  // Left colour bar
  card.append("rect")
    .attr("width", 4).attr("height", CARD_H)
    .attr("rx", 2)
    .attr("fill", d => ts(d.type).color);

  // Type icon
  card.append("text").attr("class","card-icon")
    .attr("x", 16).attr("y", 22)
    .attr("fill", d => ts(d.type).color)
    .text(d => ts(d.type).icon);

  // Type label
  card.append("text").attr("class","card-type")
    .attr("x", 34).attr("y", 20)
    .attr("fill", "#94a3b8")
    .text(d => (d.type || "UNKNOWN").toUpperCase());

  // Edge summary badges
  card.each(function(d) {
    const g = d3.select(this);
    const rCnt = links.filter(l => l.source === d.id && l.type === "reads").length;
    const wCnt = links.filter(l => l.source === d.id && l.type === "writes").length;
    let bx = CARD_W - 10;
    if (wCnt) {
      g.append("rect").attr("x", bx - 20).attr("y", 8).attr("width", 20).attr("height", 14)
        .attr("rx", 3).attr("fill", "#f8717122");
      g.append("text").attr("class","card-badge").attr("x", bx - 10).attr("y", 18)
        .attr("fill","#f87171").attr("text-anchor","middle").text(`W${wCnt}`);
      bx -= 26;
    }
    if (rCnt) {
      g.append("rect").attr("x", bx - 20).attr("y", 8).attr("width", 20).attr("height", 14)
        .attr("rx", 3).attr("fill", "#38bdf822");
      g.append("text").attr("class","card-badge").attr("x", bx - 10).attr("y", 18)
        .attr("fill","#38bdf8").attr("text-anchor","middle").text(`R${rCnt}`);
    }
  });

  // Object name (truncated)
  card.append("text").attr("class","card-name")
    .attr("x", 16).attr("y", 44)
    .attr("fill", "#e2e8f0")
    .text(d => {
      const name = d.id.length > 28 ? "…" + d.id.slice(-27) : d.id;
      return name;
    });

  // ── Interactions ──────────────────────────────
  const tooltip = document.getElementById("tooltip");

  card.on("mouseenter", (evt, d) => {
    const reads  = links.filter(l => l.source === d.id && l.type === "reads");
    const writes = links.filter(l => l.source === d.id && l.type === "writes");
    const deps   = links.filter(l => l.source === d.id && l.type === "dependency");
    const usedBy = links.filter(l => l.target === d.id);

    let h = `<div class="tt-head">${d.id}</div><div class="tt-type">${d.type}</div>`;
    if (reads.length)  h += `<div class="tt-section">Reads from</div><div class="tt-list">${reads.map(l=>l.target).join("<br>")}</div>`;
    if (writes.length) h += `<div class="tt-section">Writes to</div><div class="tt-list">${writes.map(l=>l.target).join("<br>")}</div>`;
    if (deps.length)   h += `<div class="tt-section">Depends on</div><div class="tt-list">${deps.map(l=>l.target).join("<br>")}</div>`;
    if (usedBy.length) h += `<div class="tt-section">Used by</div><div class="tt-list">${usedBy.map(l=>l.source).join("<br>")}</div>`;
    tooltip.innerHTML = h;
    tooltip.style.display = "block";

    // Highlight connected
    const connected = new Set([d.id]);
    links.forEach(l => {
      if (l.source === d.id) connected.add(l.target);
      if (l.target === d.id) connected.add(l.source);
    });
    card.select("rect").attr("opacity", n => connected.has(n.id) ? 1 : 0.2);
    card.selectAll("text").attr("opacity", n => connected.has(n.id) ? 1 : 0.2);
    linkG.selectAll("path").attr("stroke-opacity", l => (l.source === d.id || l.target === d.id) ? 0.9 : 0.06);
    linkG.selectAll("text").attr("opacity", l => (l.source === d.id || l.target === d.id) ? 1 : 0);
  })
  .on("mouseleave", () => {
    tooltip.style.display = "none";
    card.select("rect").attr("opacity", 1);
    card.selectAll("text").attr("opacity", 1);
    linkG.selectAll("path").attr("stroke-opacity", d => d.type === "dependency" ? 0.35 : 0.6);
    linkG.selectAll("text").attr("opacity", 0);
  })
  .on("mousemove", evt => {
    tooltip.style.left = Math.min(evt.pageX + 14, window.innerWidth - 400) + "px";
    tooltip.style.top = (evt.pageY - 14) + "px";
  })
  .on("click", (evt, d) => {
    selectedNode = selectedNode === d.id ? null : d.id;
    openDetail(d);
    render();
  });

  // Auto-fit
  if (nodes.length) {
    const pad = 60;
    const xExt = d3.extent(nodes, n => n.x);
    const yExt = d3.extent(nodes, n => n.y);
    const bw = xExt[1] - xExt[0] + CARD_W + pad * 2;
    const bh = yExt[1] - yExt[0] + CARD_H + pad * 2;
    const cw = window.innerWidth - (document.getElementById("detail").classList.contains("open") ? 320 : 0);
    const ch = window.innerHeight - 96;
    const scale = Math.min(cw / bw, ch / bh, 1.2);
    const tx = (cw - bw * scale) / 2 - xExt[0] * scale + pad * scale;
    const ty = (ch - bh * scale) / 2 - yExt[0] * scale + pad * scale;
    svg.transition().duration(400).call(
      zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale)
    );
  }
}

// ── Detail panel ────────────────────────────────
function openDetail(d) {
  const panel = document.getElementById("detail");
  if (!selectedNode) { panel.classList.remove("open"); return; }
  panel.classList.add("open");
  document.getElementById("det-name").textContent = d.id;
  document.getElementById("det-type").textContent = d.type;

  const fill = (elId, items) => {
    const ul = document.getElementById(elId);
    ul.innerHTML = items.length ? items.map(i => `<li>${i}</li>`).join("") : "<li style='color:var(--dim)'>—</li>";
  };
  fill("det-reads", allLinks.filter(l => l.source === d.id && l.type === "reads").map(l => l.target));
  fill("det-writes", allLinks.filter(l => l.source === d.id && l.type === "writes").map(l => l.target));
  fill("det-deps", allLinks.filter(l => l.source === d.id && l.type === "dependency").map(l => l.target));
  fill("det-usedby", allLinks.filter(l => l.target === d.id).map(l => l.source));
}

document.getElementById("det-close").addEventListener("click", () => {
  selectedNode = null;
  document.getElementById("detail").classList.remove("open");
  render();
});

// ── Toolbar event handlers ──────────────────────

// Direction buttons
document.querySelectorAll("[data-dir]").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll("[data-dir]").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    direction = btn.dataset.dir;
    render();
  });
});

// Node-type filter buttons
document.getElementById("type-filters").addEventListener("click", e => {
  const btn = e.target.closest("[data-ntype]");
  if (!btn) return;
  const t = btn.dataset.ntype;
  if (activeNodeTypes.has(t)) { activeNodeTypes.delete(t); btn.classList.remove("active"); }
  else { activeNodeTypes.add(t); btn.classList.add("active"); }
  render();
});

// Edge-type filter buttons
document.querySelectorAll("[data-edge]").forEach(btn => {
  btn.addEventListener("click", () => {
    const t = btn.dataset.edge;
    if (activeEdgeTypes.has(t)) { activeEdgeTypes.delete(t); btn.classList.remove("active"); }
    else { activeEdgeTypes.add(t); btn.classList.add("active"); }
    render();
  });
});

// Depth slider
const depthRange = document.getElementById("depth-range");
const depthVal = document.getElementById("depth-val");
depthRange.addEventListener("input", () => {
  maxDepth = parseInt(depthRange.value);
  depthVal.textContent = maxDepth >= 20 ? "∞" : maxDepth;
  render();
});

// Search
document.getElementById("search").addEventListener("input", () => render());

// ── Initial render ──────────────────────────────
render();
</script>
</body>
</html>
"""
