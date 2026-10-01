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
                (node.voter ? "Follower" : "Read Replica");
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

    function preferences(saved, hash) {
        var result = { autoRefresh: true, showReadReplicas: true };
        try {
            var parsed = JSON.parse(saved);
            Object.keys(result).forEach(function (key) {
                if (parsed && typeof parsed[key] === "boolean") result[key] = parsed[key];
            });
        } catch (_) { /* Ignore unavailable or invalid saved preferences. */ }
        var tab = hash.split("?")[0];
        if (tab === "#topology" || tab === "#cluster") {
            var params = new URLSearchParams(hash.split("?")[1] || "");
            [["auto-refresh", "autoRefresh"], ["read-replicas", "showReadReplicas"]].forEach(function (pair) {
                var value = params.get(pair[0]);
                if (value === "0" || value === "1") result[pair[1]] = value === "1";
            });
        }
        return result;
    }

    function preferenceHash(settings) {
        return "topology?auto-refresh=" + (settings.autoRefresh ? "1" : "0") +
            "&read-replicas=" + (settings.showReadReplicas ? "1" : "0");
    }

    function consoleURL(address, settings) {
        try {
            var url = new URL(address);
            if (url.protocol !== "http:" && url.protocol !== "https:") return "";
            url.pathname = url.pathname.replace(/\/$/, "") + "/console/";
            url.search = "";
            url.hash = preferenceHash(settings);
            return url.href;
        } catch (_) {
            return "";
        }
    }

    function probeLabel(model, peer, stale) {
        if (stale || !model.leader || peer.isLeader) return "";
        // /nodes probes originate at the viewing node, not necessarily the leader.
        var target = model.localID === model.leader.id ? peer :
            model.localID === peer.id ? model.leader : null;
        if (!target || target.reachable !== true || typeof target.time !== "number" ||
            !Number.isFinite(target.time) || target.time < 0) return "";
        var ms = target.time * 1000;
        var duration = ms > 0 && ms < 0.1 ? "<0.1 ms" :
            ms < 1000 ? ms.toFixed(1).replace(/\.0$/, "") + " ms" :
                target.time.toFixed(1).replace(/\.0$/, "") + " s";
        return "Probe: " + duration;
    }

    if (typeof module !== "undefined" && module.exports) {
        module.exports = { topology: topology, layout: layout, preferences: preferences, consoleURL: consoleURL, probeLabel: probeLabel };
        return;
    }

    var section = document.getElementById("topology");
    var map = document.getElementById("cluster-map");
    var summary = document.getElementById("cluster-summary");
    var refresh = document.getElementById("cluster-refresh");
    var auto = document.getElementById("cluster-auto-refresh");
    var showReadReplicas = document.getElementById("cluster-show-read-replicas");
    var preferencesKey = "rqlite_topology_preferences";
    var savedPreferences = null;
    try { savedPreferences = localStorage.getItem(preferencesKey); } catch (_) { /* Storage may be disabled. */ }
    var initialPreferences = preferences(savedPreferences, window.location.hash);
    auto.checked = initialPreferences.autoRefresh;
    showReadReplicas.checked = initialPreferences.showReadReplicas;
    // Origins cannot share localStorage, so node links carry these preferences
    // in the fragment. Save incoming preferences on the destination node too.
    savePreferences();
    var message = document.getElementById("cluster-message");
    var updated = document.getElementById("cluster-updated");
    var model = null;
    var stale = false;
    var timer = null;
    var busy = false;
    var cards = new Map();
    var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.classList.add("cluster-links");
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", "Replication connections");
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

    function currentPreferences() {
        return { autoRefresh: auto.checked, showReadReplicas: showReadReplicas.checked };
    }

    function savePreferences() {
        try { localStorage.setItem(preferencesKey, JSON.stringify(currentPreferences())); } catch (_) { /* Storage may be disabled. */ }
    }

    function preferencesChanged() {
        savePreferences();
        // Keep the current fragment up to date so a reload never reapplies
        // older preferences imported when navigating from another node.
        window.history.replaceState(null, "", "#" + preferenceHash(currentPreferences()));
        if (model) render();
    }

    function render() {
        var visibleNodes = model.nodes.filter(function (node) { return showReadReplicas.checked || node.voter; });
        var visibleLeader = visibleNodes.find(function (node) { return node.isLeader; });
        var geometry = layout(Object.assign({}, model, { nodes: visibleNodes, leader: visibleLeader }));
        map.style.height = geometry.height + "px";
        svg.setAttribute("viewBox", "0 0 " + geometry.width + " " + geometry.height);
        svg.replaceChildren();
        var probes = [];
        var voters = model.nodes.filter(function (n) { return n.voter; }).length;
        var reachable = model.nodes.filter(function (n) { return n.reachable === true; }).length;
        summary.innerHTML = [
            [model.nodes.length, "Nodes"], [voters, "Voters"],
            [model.nodes.length - voters, "Read Replicas"],
            [stale ? "—" : reachable + " / " + model.nodes.length, "Reachable"]
        ].map(function (item) {
            return '<div><strong>' + item[0] + '</strong><span>' + item[1] + '</span></div>';
        }).join("");
        document.getElementById("cluster-perspective").textContent =
            "Viewed from node " + (model.localID || "unknown") + " · " + window.location.host;

        cards.forEach(function (card, id) {
            if (!geometry.positions.has(id)) { card.wrapper.remove(); cards.delete(id); }
        });
        visibleNodes.forEach(function (node) {
            var position = geometry.positions.get(node.id);
            if (visibleLeader && !node.isLeader) {
                var origin = geometry.positions.get(visibleLeader.id);
                var line = document.createElementNS(svg.namespaceURI, "line");
                line.setAttribute("x1", origin.x);
                line.setAttribute("y1", origin.y);
                line.setAttribute("x2", position.x);
                line.setAttribute("y2", position.y);
                line.setAttribute("class", "cluster-edge" + (node.voter ? "" : " is-replica"));
                svg.appendChild(line);
                var probe = probeLabel(model, node, stale);
                if (probe) probes.push({ text: probe, origin: origin, position: position, peerID: node.id });
            }
            var card = cards.get(node.id);
            if (!card) {
                var wrapper = document.createElement("div");
                var link = document.createElement("a");
                wrapper.className = "cluster-node";
                wrapper.appendChild(link);
                map.appendChild(wrapper);
                card = { wrapper: wrapper, link: link };
                cards.set(node.id, card);
            }
            card.wrapper.style.left = (100 * position.x / geometry.width) + "%";
            card.wrapper.style.top = position.y + "px";
            card.link.title = "Raft: " + (node.addr || "Unavailable") + "\nAPI: " + (node.api_addr || "Unavailable");
            var destination = consoleURL(node.api_addr, currentPreferences());
            if (destination) {
                card.link.href = destination;
                card.link.removeAttribute("aria-disabled");
            } else {
                card.link.removeAttribute("href");
                card.link.setAttribute("aria-disabled", "true");
            }
            card.link.className = "cluster-node-link is-" + health(node) + (node.isLeader ? " is-leader" : "") +
                (node.id === model.localID ? " is-current" : "") + (node.voter ? "" : " is-replica");
            card.link.innerHTML = '<span class="cluster-node-heading"><span class="cluster-node-icon" aria-hidden="true">' +
                (node.isLeader ? "★" : node.voter ? "●" : "◇") + '</span><span><strong>Node ' + escape(node.id) +
                '</strong><span class="cluster-node-role">' + escape(node.role) + '</span></span>' +
                (node.id === model.localID ? '<span class="cluster-this-node">This node</span>' : '') + '</span>' +
                '<span class="cluster-address"><b>Raft</b> ' + escape(node.addr || "Unavailable") + '</span>' +
                '<span class="cluster-address"><b>API</b> ' + escape(node.api_addr || "Unavailable") + '</span>' +
                '<span class="cluster-node-health"><i class="cluster-dot is-' + health(node) + '"></i>' + healthLabel(node) + '</span>';
        });
        renderProbes(probes, geometry);
    }

    function renderProbes(probes, geometry) {
        var scaleX = geometry.width / map.clientWidth;
        var obstacles = [];
        cards.forEach(function (card, id) {
            var p = geometry.positions.get(id);
            obstacles.push({ x: p.x, y: p.y, w: card.wrapper.offsetWidth * scaleX + 12, h: card.wrapper.offsetHeight + 12 });
        });
        probes.forEach(function (probe) {
            var label = document.createElementNS(svg.namespaceURI, "text");
            label.setAttribute("class", "cluster-probe-label");
            label.textContent = probe.text;
            svg.appendChild(label);
            var bounds = label.getBBox();
            var dx = probe.position.x - probe.origin.x;
            var dy = probe.position.y - probe.origin.y;
            var distance = Math.hypot(dx, dy);
            var candidates = [];
            [0.5, 0.65, 0.35].forEach(function (fraction) {
                [0, -24, 24, -48, 48, -72, 72, -96, 96].forEach(function (offset) {
                    candidates.push({ x: probe.origin.x + dx * fraction - dy / distance * offset,
                        y: probe.origin.y + dy * fraction + dx / distance * offset,
                        w: bounds.width + 10, h: bounds.height + 8 });
                });
            });
            // Keep labels clear of cards and one another, including diagonal links.
            var placed = candidates.find(function (p) {
                return p.x > p.w / 2 && p.x < geometry.width - p.w / 2 &&
                    p.y > p.h / 2 && p.y < geometry.height - p.h / 2 &&
                    obstacles.every(function (other) {
                        return Math.abs(p.x - other.x) >= (p.w + other.w) / 2 ||
                            Math.abs(p.y - other.y) >= (p.h + other.h) / 2;
                    });
            }) || candidates[0];
            label.setAttribute("x", placed.x);
            label.setAttribute("y", placed.y);
            obstacles.push(placed);
        });
        svg.setAttribute("aria-label", "Replication connections" + probes.map(function (probe) {
            var targetID = model.localID === model.leader.id ? probe.peerID : model.leader.id;
            return "; from node " + model.localID + " to node " + targetID + ", " + probe.text;
        }).join(""));
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
    showReadReplicas.addEventListener("change", preferencesChanged);
    auto.addEventListener("change", function () {
        preferencesChanged();
        if (auto.checked) load(); else clearTimeout(timer);
    });
    function visibilityChanged() {
        if (active()) load();
        else clearTimeout(timer);
    }
    new MutationObserver(visibilityChanged).observe(section, { attributes: true, attributeFilter: ["class"] });
    document.addEventListener("visibilitychange", visibilityChanged);
    var initialTab = window.location.hash.split("?")[0];
    if (initialTab === "#topology" || initialTab === "#cluster") {
        document.querySelector('[data-tab="topology"]').click();
    }
})();
