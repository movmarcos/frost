"""Interactive HTML lineage visualiser.

Generates a standalone HTML page with a D3.js force-directed graph
showing object dependencies and data-flow lineage.  The page can be
opened offline -- all JavaScript is loaded from a CDN and the data is
inlined as JSON.

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
# HTML template (D3 v7 force-directed graph)
# ------------------------------------------------------------------

_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>__TITLE__</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
     background:#0f172a;color:#e2e8f0;overflow:hidden}
#header{position:fixed;top:0;left:0;right:0;height:48px;background:#1e293b;
        display:flex;align-items:center;padding:0 20px;z-index:10;
        border-bottom:1px solid #334155;gap:16px}
#header h1{font-size:18px;font-weight:700;color:#38bdf8}
#header h1 span{color:#94a3b8;font-weight:400;font-size:14px;margin-left:6px}
#header .stats{font-size:13px;color:#94a3b8}
svg{position:fixed;top:48px;left:0;right:0;bottom:0;width:100%;height:calc(100vh - 48px)}
#legend{position:fixed;bottom:20px;left:20px;background:#1e293b;border:1px solid #334155;
        border-radius:8px;padding:14px 18px;z-index:10;font-size:12px}
#legend h3{font-size:13px;color:#94a3b8;margin-bottom:8px}
.leg-row{display:flex;align-items:center;gap:8px;margin:4px 0}
.leg-dot{width:12px;height:12px;border-radius:50%;flex-shrink:0}
.leg-line{width:24px;height:3px;border-radius:2px;flex-shrink:0}

#tooltip{position:fixed;pointer-events:none;background:#1e293b;border:1px solid #475569;
         border-radius:6px;padding:10px 14px;font-size:13px;display:none;z-index:20;
         max-width:350px;line-height:1.5}
#tooltip strong{color:#38bdf8}
#tooltip .tt-type{color:#94a3b8;font-size:11px}

/* search */
#search{background:#0f172a;border:1px solid #475569;border-radius:4px;color:#e2e8f0;
        padding:4px 10px;font-size:13px;width:200px;outline:none}
#search:focus{border-color:#38bdf8}
#search::placeholder{color:#64748b}

/* edge labels */
.edge-label{font-size:10px;fill:#94a3b8;pointer-events:none}
</style>
</head>
<body>

<div id="header">
  <h1>frost<span>lineage</span></h1>
  <input id="search" type="text" placeholder="Search objects…" autocomplete="off"/>
  <div class="stats" id="stats"></div>
</div>

<svg id="canvas"></svg>

<div id="legend">
  <h3>Node Types</h3>
  <div class="leg-row"><div class="leg-dot" style="background:#38bdf8"></div> TABLE</div>
  <div class="leg-row"><div class="leg-dot" style="background:#a78bfa"></div> PROCEDURE</div>
  <div class="leg-row"><div class="leg-dot" style="background:#34d399"></div> VIEW</div>
  <div class="leg-row"><div class="leg-dot" style="background:#fb923c"></div> FUNCTION</div>
  <div class="leg-row"><div class="leg-dot" style="background:#94a3b8"></div> OTHER / EXTERNAL</div>
  <h3 style="margin-top:12px">Edge Types</h3>
  <div class="leg-row"><div class="leg-line" style="background:#475569"></div> Dependency</div>
  <div class="leg-row"><div class="leg-line" style="background:#38bdf8"></div> Reads</div>
  <div class="leg-row"><div class="leg-line" style="background:#f87171"></div> Writes</div>
</div>

<div id="tooltip"></div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
// --- data (injected by frost) ------------------------------------
const nodes = __NODES__;
const links = __LINKS__;

// --- colour maps -------------------------------------------------
const nodeColor = {
  TABLE:     "#38bdf8",
  VIEW:      "#34d399",
  PROCEDURE: "#a78bfa",
  FUNCTION:  "#fb923c",
  TASK:      "#facc15",
  STREAM:    "#2dd4bf",
  STAGE:     "#818cf8",
  SEQUENCE:  "#f472b6",
  FILE_FORMAT:"#c084fc",
  PIPE:      "#22d3ee",
};
const defaultNodeColor = "#94a3b8";

const edgeColor = {
  dependency: "#475569",
  reads:      "#38bdf8",
  writes:     "#f87171",
};

const nodeRadius = d => {
  const t = (d.type || "").toUpperCase();
  if (t === "PROCEDURE" || t === "FUNCTION") return 10;
  if (t === "VIEW") return 9;
  return 8;
};

// --- stats -------------------------------------------------------
const nObj = nodes.length;
const nEdge = links.length;
const nReads = links.filter(l => l.type === "reads").length;
const nWrites = links.filter(l => l.type === "writes").length;
document.getElementById("stats").textContent =
  `${nObj} objects · ${nEdge} edges (${nReads} reads, ${nWrites} writes)`;

// --- SVG setup ---------------------------------------------------
const svg = d3.select("#canvas");
const width  = window.innerWidth;
const height = window.innerHeight - 48;

const g = svg.append("g");

// Zoom
svg.call(
  d3.zoom()
    .scaleExtent([0.1, 6])
    .on("zoom", e => g.attr("transform", e.transform))
);

// Arrow markers
const markerTypes = ["dependency","reads","writes"];
svg.append("defs").selectAll("marker")
  .data(markerTypes).enter().append("marker")
  .attr("id", d => `arrow-${d}`)
  .attr("viewBox","0 -5 10 10").attr("refX",22).attr("refY",0)
  .attr("markerWidth",6).attr("markerHeight",6)
  .attr("orient","auto")
  .append("path")
  .attr("d","M0,-5L10,0L0,5")
  .attr("fill", d => edgeColor[d]);

// --- simulation --------------------------------------------------
const simulation = d3.forceSimulation(nodes)
  .force("link", d3.forceLink(links).id(d => d.id).distance(140))
  .force("charge", d3.forceManyBody().strength(-350))
  .force("center", d3.forceCenter(width / 2, height / 2))
  .force("collision", d3.forceCollide().radius(24));

// --- draw edges --------------------------------------------------
const link = g.append("g").selectAll("line")
  .data(links).enter().append("line")
  .attr("stroke", d => edgeColor[d.type] || "#475569")
  .attr("stroke-width", d => d.type === "dependency" ? 1.2 : 2)
  .attr("stroke-opacity", d => d.type === "dependency" ? 0.4 : 0.7)
  .attr("marker-end", d => `url(#arrow-${d.type})`);

// --- draw nodes --------------------------------------------------
const node = g.append("g").selectAll("g")
  .data(nodes).enter().append("g")
  .call(d3.drag()
    .on("start", dragStart)
    .on("drag", dragging)
    .on("end", dragEnd));

node.append("circle")
  .attr("r", nodeRadius)
  .attr("fill", d => nodeColor[(d.type||"").toUpperCase()] || defaultNodeColor)
  .attr("stroke", "#0f172a")
  .attr("stroke-width", 1.5);

node.append("text")
  .text(d => d.id.split(".").pop())
  .attr("dx", 14).attr("dy", 4)
  .attr("fill", "#cbd5e1")
  .attr("font-size", "12px");

// --- tooltip -----------------------------------------------------
const tooltip = document.getElementById("tooltip");

node.on("mouseenter", (evt, d) => {
  // count edges
  const reads  = links.filter(l => (l.source.id||l.source)===d.id && l.type==="reads");
  const writes = links.filter(l => (l.source.id||l.source)===d.id && l.type==="writes");
  const deps   = links.filter(l => (l.source.id||l.source)===d.id && l.type==="dependency");
  const usedBy = links.filter(l => (l.target.id||l.target)===d.id && l.type==="dependency");

  let html = `<strong>${d.id}</strong><br><span class="tt-type">${d.type}</span><br>`;
  if (reads.length)  html += `Reads: ${reads.map(l=>l.target.id||l.target).join(", ")}<br>`;
  if (writes.length) html += `Writes: ${writes.map(l=>l.target.id||l.target).join(", ")}<br>`;
  if (deps.length)   html += `Depends on: ${deps.map(l=>l.target.id||l.target).join(", ")}<br>`;
  if (usedBy.length) html += `Used by: ${usedBy.map(l=>l.source.id||l.source).join(", ")}`;

  tooltip.innerHTML = html;
  tooltip.style.display = "block";
  tooltip.style.left = (evt.pageX + 14) + "px";
  tooltip.style.top  = (evt.pageY - 14) + "px";

  // Highlight connected
  const connected = new Set();
  connected.add(d.id);
  links.forEach(l => {
    const s = l.source.id||l.source, t = l.target.id||l.target;
    if (s === d.id) connected.add(t);
    if (t === d.id) connected.add(s);
  });
  node.select("circle").attr("opacity", n => connected.has(n.id) ? 1 : 0.15);
  node.select("text").attr("opacity", n => connected.has(n.id) ? 1 : 0.1);
  link.attr("stroke-opacity", l => {
    const s = l.source.id||l.source, t = l.target.id||l.target;
    return (s===d.id||t===d.id) ? 0.9 : 0.05;
  });
}).on("mouseleave", () => {
  tooltip.style.display = "none";
  node.select("circle").attr("opacity", 1);
  node.select("text").attr("opacity", 1);
  link.attr("stroke-opacity", d => d.type === "dependency" ? 0.4 : 0.7);
}).on("mousemove", evt => {
  tooltip.style.left = (evt.pageX + 14) + "px";
  tooltip.style.top  = (evt.pageY - 14) + "px";
});

// --- search ------------------------------------------------------
const searchInput = document.getElementById("search");
searchInput.addEventListener("input", () => {
  const q = searchInput.value.trim().toUpperCase();
  if (!q) {
    node.select("circle").attr("opacity", 1);
    node.select("text").attr("opacity", 1);
    link.attr("stroke-opacity", d => d.type === "dependency" ? 0.4 : 0.7);
    return;
  }
  node.select("circle").attr("opacity", d => d.id.toUpperCase().includes(q) ? 1 : 0.12);
  node.select("text").attr("opacity", d => d.id.toUpperCase().includes(q) ? 1 : 0.08);
  link.attr("stroke-opacity", 0.05);
});

// --- tick --------------------------------------------------------
simulation.on("tick", () => {
  link
    .attr("x1", d => d.source.x).attr("y1", d => d.source.y)
    .attr("x2", d => d.target.x).attr("y2", d => d.target.y);
  node.attr("transform", d => `translate(${d.x},${d.y})`);
});

// --- drag --------------------------------------------------------
function dragStart(evt, d) {
  if (!evt.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x; d.fy = d.y;
}
function dragging(evt, d) { d.fx = evt.x; d.fy = evt.y; }
function dragEnd(evt, d) {
  if (!evt.active) simulation.alphaTarget(0);
  d.fx = null; d.fy = null;
}
</script>
</body>
</html>
"""
