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
        var localID = store.node_id == null ? "" : String(store.node_id);
        var local = nodes.find(function (node) { return node.id === localID; });
        if (local && local.voter && store.raft && store.raft.state) local.role = store.raft.state;
        return { nodes: nodes, leader: leader, localID: localID };
    }

    function layout(model) {
        var peers = model.nodes.filter(function (node) { return !node.isLeader; });
        var positions = new Map();
        var height = 400;
        if (model.leader && peers.length <= 2) {
            positions.set(model.leader.id, { x: peers.length === 1 ? 150 : 300, y: peers.length === 2 ? 95 : 200 });
            peers.forEach(function (node, i) {
                positions.set(node.id, { x: peers.length === 1 ? 450 : i ? 460 : 140, y: peers.length === 1 ? 200 : 295 });
            });
        } else if (model.leader && peers.length <= 6) {
            height = 480;
            positions.set(model.leader.id, { x: 300, y: 240 });
            peers.forEach(function (node, i) {
                var angle = -Math.PI / 2 + i * 2 * Math.PI / peers.length;
                positions.set(node.id, { x: 300 + 210 * Math.cos(angle), y: 240 + 165 * Math.sin(angle) });
            });
        } else {
            height = Math.max(400, Math.ceil(peers.length / 2) * 130 + 50);
            if (model.leader) positions.set(model.leader.id, { x: 300, y: height / 2 });
            peers.forEach(function (node, i) {
                positions.set(node.id, { x: peers.length === 1 ? 300 : i % 2 ? 480 : 120, y: 90 + Math.floor(i / 2) * 130 });
            });
        }
        return { positions: positions, width: 600, height: height };
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
        if (tab === "#home" || tab === "#status" || tab === "#topology" || tab === "#cluster" || tab === "#backup") {
            var params = new URLSearchParams(hash.split("?")[1] || "");
            [["auto-refresh", "autoRefresh"], ["read-replicas", "showReadReplicas"]].forEach(function (pair) {
                var value = params.get(pair[0]);
                if (value === "0" || value === "1") result[pair[1]] = value === "1";
            });
        }
        return result;
    }

    function preferenceHash(settings) {
        return "home?auto-refresh=" + (settings.autoRefresh ? "1" : "0") +
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

    var section = document.getElementById("status");
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
    var localStatus = null;
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

    var nodeMenu = document.getElementById("node-menu");
    var menuItems = Array.from(nodeMenu.querySelectorAll('[role="menuitem"]'));
    var menuNode = null;
    var menuTrigger = null;

    function closeMenu(restoreFocus) {
        nodeMenu.hidden = true;
        if (menuTrigger) {
            menuTrigger.setAttribute("aria-expanded", "false");
            if (restoreFocus && menuTrigger.isConnected) menuTrigger.focus();
        }
        menuNode = null;
    }

    function openMenu(node, trigger, x, y) {
        closeMenu(false);
        menuNode = node;
        menuTrigger = trigger;
        var destination = consoleURL(node.api_addr, currentPreferences());
        document.getElementById("node-menu-title").textContent = "Node " + node.id;
        menuItems.forEach(function (item) {
            item.setAttribute("aria-disabled", destination ? "false" : "true");
        });
        var backup = document.getElementById("node-menu-backup");
        var open = document.getElementById("node-menu-open");
        if (destination) {
            backup.href = destination.replace("#home?", "#backup?") + "&noleader=1";
            open.href = destination;
        } else {
            backup.removeAttribute("href");
            open.removeAttribute("href");
        }
        nodeMenu.hidden = false;
        trigger.setAttribute("aria-expanded", "true");
        nodeMenu.style.left = Math.max(8, Math.min(x, window.innerWidth - nodeMenu.offsetWidth - 8)) + "px";
        nodeMenu.style.top = Math.max(8, Math.min(y, window.innerHeight - nodeMenu.offsetHeight - 8)) + "px";
        menuItems[0].focus();
    }

    nodeMenu.addEventListener("click", function (event) {
        var item = event.target.closest('[role="menuitem"]');
        if (!item) return;
        if (item.getAttribute("aria-disabled") === "true") { event.preventDefault(); return; }
        var node = menuNode;
        closeMenu(true);
        var localBackup = item.id === "node-menu-backup" && node.id === model.localID &&
            !event.ctrlKey && !event.metaKey && !event.shiftKey && !event.altKey;
        if (localBackup) event.preventDefault();
        if (localBackup || item.id === "node-menu-copy") {
            document.dispatchEvent(new CustomEvent("rqlite:node-action", { detail: {
                action: localBackup ? "backup" : "copy",
                url: consoleURL(node.api_addr, currentPreferences())
            } }));
        }
    });
    nodeMenu.addEventListener("keydown", function (event) {
        var index = menuItems.indexOf(document.activeElement);
        if (event.key === "Escape" || event.key === "Tab") {
            closeMenu(true);
            if (event.key === "Escape") event.preventDefault();
        } else if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
            event.preventDefault();
            var next = event.key === "Home" ? 0 : event.key === "End" ? menuItems.length - 1 :
                (index + (event.key === "ArrowDown" ? 1 : -1) + menuItems.length) % menuItems.length;
            menuItems[next].focus();
        } else if (event.key === " " && document.activeElement.tagName === "A") {
            event.preventDefault();
            document.activeElement.click();
        }
    });
    document.addEventListener("pointerdown", function (event) {
        if (!nodeMenu.hidden && !nodeMenu.contains(event.target)) closeMenu(false);
    });
    document.addEventListener("focusin", function (event) {
        if (!nodeMenu.hidden && !nodeMenu.contains(event.target)) closeMenu(false);
    });
    document.addEventListener("scroll", function (event) {
        if (!nodeMenu.hidden && !nodeMenu.contains(event.target)) closeMenu(false);
    }, true);
    document.addEventListener("rqlite:tab-change", function () { closeMenu(false); });
    window.addEventListener("resize", function () { closeMenu(false); });

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

    function renderInspector() {
        var node = model.nodes.find(function (member) { return member.id === model.localID; });
        var store = localStatus.store || {};
        var raft = store.raft || {};
        var note = document.getElementById("inspector-note");
        document.getElementById("inspector-title").textContent = model.localID ? "Node " + model.localID : "This node";
        document.getElementById("inspector-state").innerHTML =
            '<span>' + escape(stale ? "Role unknown" : node ? node.role : raft.state || "Role unknown") + '</span>' +
            '<span><i class="cluster-dot is-' + (node ? health(node) : "unknown") + '"></i>' +
            (node ? healthLabel(node) : "Reachability unknown") + '</span>';
        var rows = [
            ["API address", (node && node.api_addr) || (localStatus.cluster || {}).api_addr || "Unavailable"],
            ["Raft address", (node && node.addr) || store.addr || "Unavailable"]
        ];
        document.getElementById("inspector-meta").innerHTML = rows.map(function (row) {
            return '<div><dt>' + escape(row[0]) + '</dt><dd>' + escape(row[1]) + '</dd></div>';
        }).join("");
        document.getElementById("local-node-details").hidden = false;
        note.textContent = stale ? "Refresh failed. These details are from the last observation." :
            !node ? "This node is not in the reported cluster membership." :
            node.error ? "Probe failed: " + node.error : "";
        note.hidden = !note.textContent;
        note.classList.toggle("is-stale", stale);
    }

    function render() {
        var visibleNodes = model.nodes.filter(function (node) { return showReadReplicas.checked || node.voter; });
        var visibleLeader = visibleNodes.find(function (node) { return node.isLeader; });
        var geometry = layout(Object.assign({}, model, { nodes: visibleNodes, leader: visibleLeader }));
        // Use a short, scrollable list at narrow widths; never overlap node controls.
        var compact = map.clientWidth < 460;
        if (compact) {
            geometry.height = Math.max(180, Math.ceil(visibleNodes.length / 2) * 120 + 30);
            visibleNodes.forEach(function (node, i) {
                geometry.positions.set(node.id, { x: visibleNodes.length === 1 ? 300 : i % 2 ? 450 : 150, y: 70 + Math.floor(i / 2) * 120 });
            });
        }
        map.style.height = geometry.height + "px";
        map.hidden = !visibleNodes.length;
        var empty = document.getElementById("cluster-empty");
        empty.hidden = !!visibleNodes.length;
        empty.textContent = model.nodes.length ? "No voting nodes. Enable read replicas to see cluster members." : "No cluster members reported.";
        svg.setAttribute("viewBox", "0 0 " + geometry.width + " " + geometry.height);
        svg.replaceChildren();
        var probes = [];
        var voters = model.nodes.filter(function (n) { return n.voter; }).length;
        var reachable = model.nodes.filter(function (n) { return n.reachable === true; }).length;
        var reachability = stale || !model.nodes.length ? "unknown" : reachable === model.nodes.length ? "reachable" : "unreachable";
        summary.innerHTML = '<span class="cluster-health is-' + reachability + '"><i class="cluster-dot is-' + reachability + '"></i>' +
            (stale ? "Status unknown" : reachable + '/' + model.nodes.length + ' reachable') + '</span>' +
            '<span' + (!model.leader || stale ? ' class="needs-attention"' : '') + '>Leader <strong>' +
            escape(stale || !model.leader ? "unknown" : model.leader.id) + '</strong></span>' +
            '<span>' + voters + (voters === 1 ? ' voter' : ' voters') + '</span>' +
            (model.nodes.length > voters ? '<span>' + (model.nodes.length - voters) + ' read replica' + (model.nodes.length - voters === 1 ? '' : 's') + '</span>' : '');
        document.getElementById("cluster-replicas-option").hidden = model.nodes.length === voters;
        document.getElementById("cluster-perspective").textContent =
            "Observed from node " + (model.localID || "unknown") + " · " + window.location.host +
            ". Reachability does not measure replication health. " +
            (compact ? "Leader marked ★; read replicas have dashed borders." : "Lines show replication relationships; dashed lines indicate read replicas.");

        cards.forEach(function (card, id) {
            if (!geometry.positions.has(id)) {
                if (menuNode && menuNode.id === id) closeMenu(false);
                card.wrapper.remove(); cards.delete(id);
            }
        });
        visibleNodes.forEach(function (node) {
            var position = geometry.positions.get(node.id);
            if (visibleLeader && !node.isLeader && !compact) {
                var origin = geometry.positions.get(visibleLeader.id);
                var line = document.createElementNS(svg.namespaceURI, "line");
                line.setAttribute("x1", origin.x);
                line.setAttribute("y1", origin.y);
                line.setAttribute("x2", position.x);
                line.setAttribute("y2", position.y);
                line.setAttribute("class", "cluster-edge" + (node.voter ? "" : " is-replica") +
                    (node.id === model.localID || visibleLeader.id === model.localID ? " is-current" : ""));
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
                var actions = document.createElement("button");
                actions.type = "button";
                actions.className = "cluster-node-actions";
                actions.textContent = "⋯";
                actions.setAttribute("aria-label", "Actions for node " + node.id);
                actions.setAttribute("aria-haspopup", "menu");
                actions.setAttribute("aria-controls", "node-menu");
                actions.setAttribute("aria-expanded", "false");
                actions.title = "Node actions";
                wrapper.appendChild(actions);
                map.appendChild(wrapper);
                card = { wrapper: wrapper, link: link, actions: actions, node: node };
                actions.addEventListener("click", function () {
                    var bounds = actions.getBoundingClientRect();
                    openMenu(card.node, actions, bounds.left, bounds.bottom + 4);
                });
                wrapper.addEventListener("contextmenu", function (event) {
                    event.preventDefault();
                    var bounds = actions.getBoundingClientRect();
                    openMenu(card.node, actions, event.clientX || bounds.left, event.clientY || bounds.bottom);
                });
                wrapper.addEventListener("keydown", function (event) {
                    if (event.key === "ContextMenu" || (event.shiftKey && event.key === "F10")) {
                        event.preventDefault();
                        var bounds = actions.getBoundingClientRect();
                        openMenu(card.node, actions, bounds.left, bounds.bottom + 4);
                    }
                });
                cards.set(node.id, card);
            }
            card.node = node;
            card.wrapper.style.left = (100 * position.x / geometry.width) + "%";
            card.wrapper.style.top = position.y + "px";
            var destination = consoleURL(node.api_addr, currentPreferences());
            if (destination) {
                card.link.href = destination;
                card.link.removeAttribute("aria-disabled");
                card.link.title = "Open node " + node.id + " console";
            } else {
                card.link.removeAttribute("href");
                card.link.setAttribute("aria-disabled", "true");
                card.link.title = "Node " + node.id + ": API address unavailable";
            }
            if (node.id === model.localID) card.link.setAttribute("aria-current", "page");
            else card.link.removeAttribute("aria-current");
            card.link.className = "cluster-node-link is-" + health(node) + (node.isLeader ? " is-leader" : "") +
                (node.id === model.localID ? " is-current" : "") + (node.voter ? "" : " is-replica");
            card.link.innerHTML = '<span class="cluster-node-heading"><span class="cluster-node-icon" aria-hidden="true">' +
                (node.isLeader ? "★" : node.voter ? "●" : "◇") + '</span><strong>Node ' + escape(node.id) + '</strong></span>' +
                '<span class="cluster-node-role">' + escape(stale ? "Role unknown" : node.role) + '</span>' +
                '<span class="cluster-node-health"><i class="cluster-dot is-' + health(node) + '"></i>' +
                (stale ? "Stale" : healthLabel(node)) +
                (node.id === model.localID ? '<span class="cluster-this-node">This node</span>' : '') + '</span>';
        });
        renderProbes(probes, geometry);
        renderInspector();
    }

    function renderProbes(probes, geometry) {
        // Restore can request fresh membership while Home is hidden.
        if (!map.clientWidth) return;
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

    function load(force) {
        if (busy || (!force && !active())) return;
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
            localStatus = responses[0];
            model = topology(localStatus, responses[1]);
            stale = false;
            render();
            document.dispatchEvent(new CustomEvent("rqlite:cluster-updated", {
                detail: { status: responses[0], nodes: responses[1].nodes }
            }));
            message.textContent = !model.nodes.length ? "No cluster members reported." :
                !model.leader ? "No leader reported. Showing membership without replication connections." :
                !model.leader.leader ? "The reported leader could not be reached. Leadership may be changing." :
                responses[0].store.ready === false ? "This node is not ready to serve requests." : "";
            message.hidden = !message.textContent;
            updated.textContent = "Updated " + new Date().toLocaleTimeString();
        }).catch(function (error) {
            stale = true;
            if (model) render();
            document.dispatchEvent(new CustomEvent("rqlite:cluster-error"));
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
        if (active()) {
            if (model) render();
            load();
        }
        else clearTimeout(timer);
    }
    window.addEventListener("resize", function () {
        if (active() && model) render();
    });
    document.addEventListener("rqlite:tab-change", visibilityChanged);
    document.addEventListener("rqlite:cluster-refresh", function () { load(true); });
    document.addEventListener("visibilitychange", visibilityChanged);
    // Home is the default view. Legacy #topology and #cluster links still land
    // here, and their refresh and replica preferences are retained above.
    load(true);
})();
