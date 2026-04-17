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
    if (msg.type === "pickObject") {
      searchInput.value = msg.fqn;
      pickObject(msg.fqn);
      return;
    }
  });

  // Tell the extension we're ready for the initial object list.
  vscode.postMessage({ type: "ready" });
})();
