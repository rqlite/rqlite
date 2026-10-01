/* Cluster topology: all observations come from the node serving this console. */
(function () {
    "use strict";

    function topology(status, data) {
        var store = status.store || {};
        var reportedLeader = store.leader || {};
        var nodes = data.nodes.slice().sort(function (a, b) {
            return String(a.id).localeCompare(String(b.id), undefined, { numeric: true });
        }).map(function (node) {
            return Object.assign({}, node, { id: String(node.id) });
        });
        var leader = nodes.find(function (node) { return node.leader; });
        if (!leader) {
            leader = nodes.find(function (node) {
                return node.id === String(reportedLeader.node_id) && node.addr === reportedLeader.addr;
            });
        }
        nodes.forEach(function (node) {
            node.isLeader = node === leader;
            node.role = node.isLeader ? (node.leader ? "Leader" : "Reported leader") :
                (node.voter ? "Follower" : "Read-only");
            // Without a known leader, we cannot infer a voting node's Raft state.
            if (!leader && node.voter) node.role = "Voter";
        });
        return { nodes: nodes, leader: leader, localID: String(store.node_id || "") };
    }

    function layout(model) {
        var peers = model.nodes.filter(function (node) { return !node.isLeader; });
        var positions = new Map();
        var height = peers.length === 0 && model.leader ? 250 : peers.length === 2 ? 470 : 610;
        if (model.leader) positions.set(model.leader.id, { x: 450, y: peers.length === 0 ? 125 : peers.length === 2 ? 140 : 305 });
        if (peers.length <= 6 && model.leader) {
            peers.forEach(function (node, i) {
                var angle = peers.length === 2 ? (i ? 1 : 5) * Math.PI / 6 :
                    -Math.PI / 2 + i * 2 * Math.PI / Math.max(peers.length, 1);
                positions.set(node.id, { x: 450 + 305 * Math.cos(angle), y: (peers.length === 2 ? 230 : 305) + 210 * Math.sin(angle) });
            });
        } else {
            // Two stable columns leave a clear central lane for larger clusters.
            height = Math.max(400, Math.ceil(peers.length / 2) * 180 + 60);
            if (model.leader) positions.set(model.leader.id, { x: 450, y: height / 2 });
            peers.forEach(function (node, i) {
                positions.set(node.id, { x: i % 2 ? 755 : 145, y: 110 + Math.floor(i / 2) * 180 });
            });
        }
        return { positions: positions, width: 900, height: height };
    }

    if (typeof module !== "undefined" && module.exports) {
        module.exports = { topology: topology, layout: layout };
        return;
    }

    var section = document.getElementById("cluster");
    var map = document.getElementById("cluster-map");
    var summary = document.getElementById("cluster-summary");
    var details = document.getElementById("cluster-details");
    var refresh = document.getElementById("cluster-refresh");
    var auto = document.getElementById("cluster-auto-refresh");
    var message = document.getElementById("cluster-message");
    var updated = document.getElementById("cluster-updated");
    var model = null;
    var stale = false;
    var selected = null;
    var timer = null;
    var busy = false;
    var cards = new Map();
    var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.classList.add("cluster-links");
    svg.setAttribute("aria-hidden", "true");
    svg.setAttribute("preserveAspectRatio", "none");
    map.appendChild(svg);

    function escape(value) {
        return String(value == null ? "" : value).replace(/[&<>"']/g, function (c) {
            return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
        });
    }

    function health(node) {
        return stale ? "unknown" : node.reachable === true ? "reachable" :
            node.reachable === false ? "unreachable" : "unknown";
    }

    function healthLabel(node) {
        return stale ? "Stale observation" : { reachable: "Reachable", unreachable: "Unreachable", unknown: "Unknown" }[health(node)];
    }

    function detailHTML(node) {
        var rows = [
            ["Role", node.role], ["Reachability", healthLabel(node)],
            ["Raft address", node.addr || "Unavailable"],
            ["API address", node.api_addr || "Unavailable"],
            ["Version", node.version || "Unavailable"],
            ["Probe time", node.time_s || "Unavailable"]
        ];
        if (node.error) rows.push(["Probe error", node.error]);
        return '<strong>Node ' + escape(node.id) + '</strong><dl>' + rows.map(function (row) {
            return '<div><dt>' + row[0] + '</dt><dd>' + escape(row[1]) + '</dd></div>';
        }).join("") + '</dl>';
    }

    function renderSelection() {
        var node = model.nodes.find(function (n) { return n.id === selected; });
        cards.forEach(function (card, id) {
            card.button.setAttribute("aria-pressed", String(id === selected));
        });
        if (!node) {
            selected = null;
            details.innerHTML = '<p class="cluster-muted">Hover over or focus a node for details. Click to keep it selected.</p>';
            return;
        }
        details.innerHTML = detailHTML(node);
        if (node.api_addr) {
            try {
                var url = new URL(node.api_addr);
                if (url.protocol === "http:" || url.protocol === "https:") {
                    var link = document.createElement("a");
                    link.href = node.api_addr.replace(/\/$/, "") + "/console/";
                    link.textContent = "Open node console ↗";
                    link.target = "_blank";
                    link.rel = "noopener";
                    details.appendChild(link);
                }
            } catch (_) { /* Only valid HTTP API addresses become links. */ }
        }
    }

    function render() {
        var geometry = layout(model);
        map.style.height = geometry.height + "px";
        svg.setAttribute("viewBox", "0 0 " + geometry.width + " " + geometry.height);
        svg.replaceChildren();
        var voters = model.nodes.filter(function (n) { return n.voter; }).length;
        var reachable = model.nodes.filter(function (n) { return n.reachable === true; }).length;
        summary.innerHTML = [
            [model.nodes.length, "Nodes"], [voters, "Voters"],
            [model.nodes.length - voters, "Read-only"],
            [stale ? "—" : reachable + " / " + model.nodes.length, "Reachable"]
        ].map(function (item) {
            return '<div><strong>' + item[0] + '</strong><span>' + item[1] + '</span></div>';
        }).join("");
        document.getElementById("cluster-perspective").textContent =
            "Viewed from node " + (model.localID || "unknown") + " · " + window.location.host;

        cards.forEach(function (card, id) {
            if (!geometry.positions.has(id)) { card.wrapper.remove(); cards.delete(id); }
        });
        model.nodes.forEach(function (node, index) {
            var position = geometry.positions.get(node.id);
            if (model.leader && !node.isLeader) {
                var origin = geometry.positions.get(model.leader.id);
                var line = document.createElementNS(svg.namespaceURI, "line");
                line.setAttribute("x1", origin.x);
                line.setAttribute("y1", origin.y);
                line.setAttribute("x2", position.x);
                line.setAttribute("y2", position.y);
                line.setAttribute("class", "cluster-edge is-" + health(node) + (node.voter ? "" : " is-replica"));
                svg.appendChild(line);
            }
            var card = cards.get(node.id);
            if (!card) {
                var wrapper = document.createElement("div");
                var button = document.createElement("button");
                var tooltip = document.createElement("div");
                wrapper.className = "cluster-node";
                button.type = "button";
                button.className = "cluster-node-button";
                tooltip.className = "cluster-tooltip";
                tooltip.setAttribute("role", "tooltip");
                button.addEventListener("click", function () {
                    selected = selected === node.id ? null : node.id;
                    renderSelection();
                });
                wrapper.addEventListener("keydown", function (event) {
                    if (event.key === "Escape") {
                        wrapper.classList.add("tooltip-dismissed");
                        selected = null;
                        renderSelection();
                    }
                });
                wrapper.addEventListener("mouseenter", function () { wrapper.classList.remove("tooltip-dismissed"); });
                button.addEventListener("focus", function () { wrapper.classList.remove("tooltip-dismissed"); });
                wrapper.append(button, tooltip);
                map.appendChild(wrapper);
                card = { wrapper: wrapper, button: button, tooltip: tooltip };
                cards.set(node.id, card);
            }
            card.wrapper.style.left = (100 * position.x / geometry.width) + "%";
            card.wrapper.style.top = position.y + "px";
            card.wrapper.classList.toggle("tooltip-below", position.y < 220);
            card.wrapper.classList.toggle("tooltip-align-right", position.x > geometry.width / 2);
            card.button.className = "cluster-node-button is-" + health(node) + (node.isLeader ? " is-leader" : "") + (node.voter ? "" : " is-replica");
            card.button.innerHTML = '<span class="cluster-node-heading"><span class="cluster-node-icon" aria-hidden="true">' +
                (node.isLeader ? "★" : node.voter ? "●" : "◇") + '</span><span><strong>Node ' + escape(node.id) +
                '</strong><span class="cluster-node-role">' + escape(node.role) + '</span></span>' +
                (node.id === model.localID ? '<span class="cluster-this-node">This node</span>' : '') + '</span>' +
                '<span class="cluster-address"><b>Raft</b> ' + escape(node.addr || "Unavailable") + '</span>' +
                '<span class="cluster-address"><b>API</b> ' + escape(node.api_addr || "Unavailable") + '</span>' +
                '<span class="cluster-node-health"><i class="cluster-dot is-' + health(node) + '"></i>' + healthLabel(node) + '</span>';
            card.tooltip.id = "cluster-tooltip-" + index;
            card.button.setAttribute("aria-describedby", card.tooltip.id);
            card.tooltip.innerHTML = detailHTML(node);
        });
        renderSelection();
    }

    function active() {
        return section.classList.contains("active") && !document.hidden;
    }

    function schedule() {
        clearTimeout(timer);
        if (active() && auto.checked && !busy) timer = setTimeout(load, 5000);
    }

    function read(path, signal) {
        return fetch(path, { signal: signal }).then(function (response) {
            if (!response.ok) throw new Error("HTTP " + response.status + " from " + path.split("?")[0]);
            return response.json();
        });
    }

    function load() {
        if (busy || !active()) return;
        busy = true;
        clearTimeout(timer);
        refresh.disabled = true;
        refresh.textContent = "Refreshing…";
        var controller = new AbortController();
        var timeout = setTimeout(function () { controller.abort(); }, 7000);
        Promise.all([
            read("/status", controller.signal),
            read("/nodes?nonvoters&ver=2&timeout=2s", controller.signal)
        ]).then(function (responses) {
            if (!responses[0] || !responses[0].store || !responses[1] || !Array.isArray(responses[1].nodes) ||
                responses[1].nodes.some(function (n) { return !n || n.id == null; })) {
                throw new Error("Invalid cluster response");
            }
            model = topology(responses[0], responses[1]);
            stale = false;
            render();
            message.textContent = !model.nodes.length ? "No cluster members reported." :
                !model.leader ? "No leader reported. Showing membership without replication connections." :
                !model.leader.leader ? "The reported leader could not be reached. Leadership may be changing." : "";
            message.hidden = !message.textContent;
            updated.textContent = "Updated " + new Date().toLocaleTimeString();
        }).catch(function (error) {
            stale = true;
            if (model) render();
            message.textContent = "Unable to refresh cluster: " + (error.name === "AbortError" ? "request timed out" : error.message) +
                (model ? ". Showing the last observation; reachability is now unknown." : ". Try Refresh to reconnect.");
            message.hidden = false;
        }).finally(function () {
            clearTimeout(timeout);
            busy = false;
            refresh.disabled = false;
            refresh.textContent = "Refresh";
            schedule();
        });
    }

    refresh.addEventListener("click", load);
    auto.addEventListener("change", function () { if (auto.checked) load(); else clearTimeout(timer); });
    function visibilityChanged() {
        if (active()) load();
        else clearTimeout(timer);
    }
    new MutationObserver(visibilityChanged).observe(section, { attributes: true, attributeFilter: ["class"] });
    document.addEventListener("visibilitychange", visibilityChanged);
    if (window.location.hash === "#cluster") document.querySelector('[data-tab="cluster"]').click();
})();
