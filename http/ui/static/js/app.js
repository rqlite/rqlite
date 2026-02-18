(function () {
    "use strict";

    // --- Utility ---

    function apiRequest(method, path, body) {
        var opts = {
            method: method,
            headers: {}
        };
        if (body !== undefined) {
            opts.headers["Content-Type"] = "application/json";
            opts.body = JSON.stringify(body);
        }
        return fetch(path, opts).then(function (resp) {
            return resp.json().then(function (data) {
                return { status: resp.status, data: data };
            });
        });
    }

    function escapeHTML(str) {
        if (str === null || str === undefined) return "";
        var div = document.createElement("div");
        div.appendChild(document.createTextNode(String(str)));
        return div.innerHTML.replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    // --- Node Info (header) ---

    var nodeInfoDiv = document.getElementById("node-info");

    var nodeIdText = document.getElementById("node-id-text");
    var raftStateBadge = document.getElementById("raft-state-badge");

    function updateNodeInfo(data) {
        var store = data.store || {};
        var raft = store.raft || {};
        var state = (raft.state || "").toLowerCase();

        nodeIdText.textContent = store.node_id || "";

        raftStateBadge.textContent = raft.state || "";
        raftStateBadge.className = "raft-state-badge";
        if (state === "leader" || state === "follower" || state === "candidate") {
            raftStateBadge.classList.add(state);
        }
    }

    // --- Tabs ---

    var tabs = document.querySelectorAll(".tab");
    var tabContents = document.querySelectorAll(".tab-content");
    var homeLink = document.getElementById("home-link");

    function showStatus() {
        tabs.forEach(function (t) { t.classList.remove("active"); });
        tabContents.forEach(function (tc) { tc.classList.remove("active"); });
        document.getElementById("status").classList.add("active");
        loadStatus();
    }

    homeLink.addEventListener("click", showStatus);

    tabs.forEach(function (tab) {
        tab.addEventListener("click", function () {
            var target = tab.getAttribute("data-tab");
            tabs.forEach(function (t) { t.classList.remove("active"); });
            tabContents.forEach(function (tc) { tc.classList.remove("active"); });
            tab.classList.add("active");
            document.getElementById(target).classList.add("active");
        });
    });

    // --- Query Tab ---

    var sqlInput = document.getElementById("sql-input");
    var executeBtn = document.getElementById("execute-btn");
    var resultsDiv = document.getElementById("query-results");

    executeBtn.addEventListener("click", executeQuery);

    sqlInput.addEventListener("keydown", function (e) {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            executeQuery();
        }
    });

    // --- Query History ---

    var historyDiv = document.getElementById("query-history");
    var HISTORY_KEY = "rqlite_query_history";
    var MAX_HISTORY = 20;

    function getHistory() {
        try {
            return JSON.parse(localStorage.getItem(HISTORY_KEY)) || [];
        } catch (e) {
            return [];
        }
    }

    function saveToHistory(sql) {
        var history = getHistory();
        // Remove duplicate if exists
        var idx = history.indexOf(sql);
        if (idx !== -1) history.splice(idx, 1);
        // Add to front
        history.unshift(sql);
        // Limit size
        if (history.length > MAX_HISTORY) history = history.slice(0, MAX_HISTORY);
        localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
        renderHistory();
    }

    function renderHistory() {
        var history = getHistory();
        if (history.length === 0) {
            historyDiv.innerHTML = "";
            return;
        }
        var html = '<div class="query-history-header">';
        html += '<span class="query-history-label">Recent queries</span>';
        html += '<button class="query-history-clear" id="clear-history-btn">Clear</button>';
        html += '</div>';
        html += '<ul class="query-history-list">';
        history.forEach(function (q, i) {
            html += '<li data-index="' + i + '" title="' + escapeHTML(q) + '">' + escapeHTML(q) + '</li>';
        });
        html += '</ul>';
        historyDiv.innerHTML = html;

        // Click to recall
        historyDiv.querySelectorAll(".query-history-list li").forEach(function (li) {
            li.addEventListener("click", function () {
                var i = parseInt(li.getAttribute("data-index"), 10);
                sqlInput.value = history[i];
                sqlInput.focus();
            });
        });

        // Clear button
        var clearBtn = document.getElementById("clear-history-btn");
        if (clearBtn) {
            clearBtn.addEventListener("click", function () {
                localStorage.removeItem(HISTORY_KEY);
                renderHistory();
            });
        }
    }

    renderHistory();

    function executeQuery() {
        var sql = sqlInput.value.trim();
        if (!sql) return;

        executeBtn.disabled = true;
        resultsDiv.innerHTML = "";

        saveToHistory(sql);

        apiRequest("POST", "/db/request?timings", [sql])
            .then(function (resp) {
                renderResults(resp.data);
            })
            .catch(function (err) {
                resultsDiv.innerHTML = '<div class="result-error">' + escapeHTML(err.message) + '</div>';
            })
            .finally(function () {
                executeBtn.disabled = false;
            });
    }

    function renderResults(data) {
        if (!data || !data.results) {
            resultsDiv.innerHTML = '<div class="result-error">No results returned</div>';
            return;
        }

        var html = "";
        data.results.forEach(function (result) {
            if (result.error) {
                html += '<div class="result-error">' + escapeHTML(result.error) + '</div>';
            } else if (isQueryResult(result)) {
                html += renderTable(result);
            } else {
                html += renderWriteResult(result);
            }
            if (result.time) {
                html += '<div class="result-timing">Query time: ' + result.time.toFixed(6) + 's</div>';
            }
        });
        resultsDiv.innerHTML = html;
    }

    // Match the rqlite CLI logic: if columns, types, or values is present, it's a query result.
    function isQueryResult(result) {
        return result.columns != null || result.types != null || result.values != null;
    }

    function renderTable(result) {
        var columns = result.columns || [];
        var values = result.values || [];

        var html = '<table class="result-table"><thead><tr>';
        columns.forEach(function (col) {
            html += "<th>" + escapeHTML(col) + "</th>";
        });
        html += "</tr></thead><tbody>";

        if (values.length === 0) {
            html += '<tr><td colspan="' + columns.length + '" style="text-align:center;color:#999;">No rows returned</td></tr>';
        } else {
            values.forEach(function (row) {
                html += "<tr>";
                row.forEach(function (val) {
                    if (val === null || val === undefined) {
                        html += '<td class="null-value">NULL</td>';
                    } else {
                        html += "<td>" + escapeHTML(val) + "</td>";
                    }
                });
                html += "</tr>";
            });
        }
        html += "</tbody></table>";
        return html;
    }

    function renderWriteResult(result) {
        var parts = [];
        if (result.rows_affected !== undefined) {
            parts.push("Rows affected: " + result.rows_affected);
        }
        if (result.last_insert_id !== undefined) {
            parts.push("Last insert ID: " + result.last_insert_id);
        }
        if (parts.length === 0) {
            parts.push("OK");
        }
        return '<div class="result-message success">' + escapeHTML(parts.join(" | ")) + '</div>';
    }

    // --- Status Tab ---

    var statusCards = document.getElementById("status-cards");
    var nodesTable = document.getElementById("nodes-table");
    var detailSections = document.getElementById("detail-sections");
    var rawJsonPre = document.getElementById("raw-json");
    var rawJsonBtn = document.getElementById("raw-json-btn");
    var autoRefreshCheckbox = document.getElementById("auto-refresh");
    var showNonVoters = document.getElementById("show-nonvoters");
    var lastUpdatedSpan = document.getElementById("last-updated");
    var refreshTimer = null;
    var lastStatusData = null;

    autoRefreshCheckbox.addEventListener("change", function () {
        if (autoRefreshCheckbox.checked) {
            loadStatus();
            refreshTimer = setInterval(loadStatus, 5000);
        } else {
            if (refreshTimer) {
                clearInterval(refreshTimer);
                refreshTimer = null;
            }
        }
    });

    // Status is the default tab, so load immediately.
    loadStatus();

    rawJsonBtn.addEventListener("click", function () {
        if (rawJsonPre.classList.contains("hidden")) {
            rawJsonPre.classList.remove("hidden");
            rawJsonBtn.textContent = "Hide Raw JSON";
            if (lastStatusData) {
                rawJsonPre.textContent = JSON.stringify(lastStatusData, null, 2);
            }
        } else {
            rawJsonPre.classList.add("hidden");
            rawJsonBtn.textContent = "Show Raw JSON";
        }
    });

    showNonVoters.addEventListener("change", function () {
        loadStatus();
    });

    function nodesURL() {
        return showNonVoters.checked ? "/nodes?nonvoters" : "/nodes";
    }

    function loadStatus() {
        Promise.all([
            apiRequest("GET", "/status"),
            apiRequest("GET", nodesURL())
        ]).then(function (responses) {
            lastStatusData = responses[0].data;
            renderStatus(lastStatusData);
            updateNodeInfo(lastStatusData);
            renderNodes(responses[1].data);
            if (!rawJsonPre.classList.contains("hidden")) {
                rawJsonPre.textContent = JSON.stringify(lastStatusData, null, 2);
            }
            // Update last-updated timestamp
            var now = new Date();
            var hh = String(now.getHours()).padStart(2, "0");
            var mm = String(now.getMinutes()).padStart(2, "0");
            var ss = String(now.getSeconds()).padStart(2, "0");
            lastUpdatedSpan.textContent = "Updated " + hh + ":" + mm + ":" + ss;
        }).catch(function (err) {
            statusCards.innerHTML = '<div class="status-error">' + escapeHTML(err.message) + '</div>';
        });
    }

    function renderStatus(data) {
        var store = data.store || {};
        var raft = store.raft || {};
        var node = data.node || {};
        var http = data.http || {};
        var build = data.build || {};
        var sqlite = store.sqlite3 || {};

        // --- Overview cards ---
        var cards = [
            { label: "Node ID", value: store.node_id || "N/A" },
            { label: "Raft State", value: raft.state || "N/A" },
            { label: "Leader", value: store.leader ? store.leader.addr || "N/A" : "N/A" },
            { label: "Version", value: build.version || "N/A" },
            { label: "Uptime", value: node.uptime ? node.uptime.replace(/(\d+\.\d)\d+s$/, "$1s") : "N/A" },
            { label: "DB Size", value: store.dir_size_friendly || "N/A" },
            { label: "API Address", value: http.bind_addr || "N/A" },
            { label: "Raft Address", value: store.addr || "N/A" }
        ];

        var raftStateClass = "";
        var raftStateLower = (raft.state || "").toLowerCase();
        if (raftStateLower === "leader") raftStateClass = " raft-leader";
        else if (raftStateLower === "follower") raftStateClass = " raft-follower";
        else if (raftStateLower === "candidate") raftStateClass = " raft-candidate";

        var html = "";
        cards.forEach(function (c) {
            var extraClass = (c.label === "Raft State") ? raftStateClass : "";
            html += '<div class="card' + extraClass + '"><div class="label">' + escapeHTML(c.label) +
                '</div><div class="value">' + escapeHTML(c.value) + '</div></div>';
        });
        statusCards.innerHTML = html;

        // --- Detail sections ---
        var queue = (http.queue && http.queue._default) || {};
        var connRO = (sqlite.conn_pool_stats && sqlite.conn_pool_stats.ro) || {};
        var connRW = (sqlite.conn_pool_stats && sqlite.conn_pool_stats.rw) || {};
        var oss = data.os || {};
        var rt = data.runtime || {};
        var cluster = data.cluster || {};

        var sections = [
            {
                title: "Store",
                rows: [
                    ["FSM Index", store.fsm_index],
                    ["FSM Term", store.fsm_term],
                    ["DB Applied Index", store.db_applied_index],
                    ["Ready", store.ready],
                    ["Apply Timeout", store.apply_timeout],
                    ["Election Timeout", store.election_timeout],
                    ["Heartbeat Timeout", store.heartbeat_timeout],
                    ["Snapshot Threshold", store.snapshot_threshold],
                    ["Snapshot Interval", store.snapshot_interval],
                    ["Trailing Logs", store.trailing_logs],
                    ["Data Directory", store.dir],
                    ["Directory Size", store.dir_size_friendly]
                ]
            },
            {
                title: "Raft",
                rows: [
                    ["State", raft.state],
                    ["Term", raft.term],
                    ["Commit Index", raft.commit_index],
                    ["Applied Index", raft.applied_index],
                    ["Last Log Index", raft.last_log_index],
                    ["Last Log Term", raft.last_log_term],
                    ["Last Snapshot Index", raft.last_snapshot_index],
                    ["Last Snapshot Term", raft.last_snapshot_term],
                    ["Last Contact", raft.last_contact],
                    ["Num Peers", raft.num_peers],
                    ["Protocol Version", raft.protocol_version],
                    ["Voter", raft.voter],
                    ["Log Size", raft.log_size]
                ]
            },
            {
                title: "SQLite",
                rows: [
                    ["Version", sqlite.version],
                    ["DB Size", sqlite.db_size_friendly],
                    ["WAL Size", sqlite.wal_size],
                    ["Path", sqlite.path],
                    ["Last Modified", sqlite.last_modified],
                    ["Page Size", sqlite.mem_stats ? sqlite.mem_stats.page_size : undefined],
                    ["Page Count", sqlite.mem_stats ? sqlite.mem_stats.page_count : undefined],
                    ["RW Connections (in-use / idle)", connRW.in_use !== undefined ? connRW.in_use + " / " + connRW.idle : undefined],
                    ["RO Connections (in-use / idle)", connRO.in_use !== undefined ? connRO.in_use + " / " + connRO.idle : undefined]
                ]
            },
            {
                title: "HTTP",
                rows: [
                    ["Bind Address", http.bind_addr],
                    ["Auth", http.auth],
                    ["TLS", http.tls ? http.tls.enabled : undefined],
                    ["Queue Capacity", queue.max_size],
                    ["Queue Batch Size", queue.batch_size],
                    ["Queue Timeout", queue.timeout]
                ]
            },
            {
                title: "Cluster",
                rows: [
                    ["Address", cluster.addr],
                    ["API Address", cluster.api_addr],
                    ["HTTPS", cluster.https]
                ]
            },
            {
                title: "OS / Runtime",
                rows: [
                    ["Hostname", oss.hostname],
                    ["PID", oss.pid],
                    ["Executable", oss.executable],
                    ["Go Version", rt.version],
                    ["GOOS / GOARCH", rt.GOOS !== undefined ? rt.GOOS + " / " + rt.GOARCH : undefined],
                    ["GOMAXPROCS", rt.GOMAXPROCS],
                    ["Num CPU", rt.num_cpu],
                    ["Goroutines", rt.num_goroutine]
                ]
            }
        ];

        html = "";
        sections.forEach(function (section) {
            html += '<div class="detail-section">';
            html += '<div class="detail-section-header" onclick="this.parentElement.classList.toggle(\'open\')">';
            html += '<span>' + escapeHTML(section.title) + '</span>';
            html += '<span class="arrow">&#9654;</span>';
            html += '</div>';
            html += '<div class="detail-section-body"><table class="detail-table">';
            section.rows.forEach(function (row) {
                var val = row[1];
                var display;
                if (val === undefined || val === null) {
                    display = '<span style="color:#999">N/A</span>';
                } else if (typeof val === "boolean") {
                    display = escapeHTML(val ? "Yes" : "No");
                } else {
                    display = escapeHTML(String(val));
                }
                html += '<tr><td>' + escapeHTML(row[0]) + '</td><td>' + display + '</td></tr>';
            });
            html += '</table></div></div>';
        });
        detailSections.innerHTML = html;
    }

    var nodesColumns = [
        { key: "id", label: "ID" },
        { key: "api_addr", label: "API Address" },
        { key: "addr", label: "Raft Address" },
        { key: "voter", label: "Voter" },
        { key: "reachable", label: "Reachable" },
        { key: "leader", label: "Leader" }
    ];
    var lastNodesData = [];
    var nodesSortKey = "id";
    var nodesSortAsc = true;

    function renderNodes(data) {
        if (!data || typeof data !== "object") {
            lastNodesData = [];
            nodesTable.innerHTML = '<div class="result-message">No node information available</div>';
            return;
        }

        // /nodes returns an object keyed by node ID, not an array.
        lastNodesData = Object.values(data);
        if (lastNodesData.length === 0) {
            nodesTable.innerHTML = '<div class="result-message">No node information available</div>';
            return;
        }

        renderNodesTable();
    }

    function renderNodesTable() {
        var nodes = lastNodesData.slice();

        if (nodesSortKey) {
            nodes.sort(function (a, b) {
                var va = a[nodesSortKey];
                var vb = b[nodesSortKey];
                if (typeof va === "boolean") { va = va ? 1 : 0; vb = vb ? 1 : 0; }
                if (va == null) va = "";
                if (vb == null) vb = "";
                var cmp = String(va).localeCompare(String(vb), undefined, { numeric: true });
                return nodesSortAsc ? cmp : -cmp;
            });
        }

        var html = '<table><thead><tr>';
        nodesColumns.forEach(function (col) {
            var arrow = "";
            if (nodesSortKey === col.key) {
                arrow = nodesSortAsc ? " \u25B2" : " \u25BC";
            }
            html += '<th class="sortable" data-sort-key="' + col.key + '">' + escapeHTML(col.label) + arrow + '</th>';
        });
        html += '</tr></thead><tbody>';

        nodes.forEach(function (node) {
            html += "<tr>";
            html += "<td>" + escapeHTML(node.id) + "</td>";
            var apiURL = node.api_addr || "";
            if (apiURL) {
                var uiURL = apiURL.replace(/\/$/, "") + "/ui/";
                html += '<td><a href="' + escapeHTML(uiURL) + '">' + escapeHTML(apiURL) + '</a></td>';
            } else {
                html += "<td>N/A</td>";
            }
            html += "<td>" + escapeHTML(node.addr) + "</td>";
            html += "<td>" + (node.voter ? "Yes" : "No") + "</td>";
            html += '<td><span class="status-dot ' + (node.reachable ? 'reachable' : 'unreachable') + '" title="' + (node.reachable ? 'Reachable' : 'Unreachable') + '"></span></td>';
            html += "<td>" + (node.leader ? "Yes" : "No") + "</td>";
            html += "</tr>";
        });
        html += "</tbody></table>";
        nodesTable.innerHTML = html;

        // Attach click handlers to sortable headers.
        nodesTable.querySelectorAll("th.sortable").forEach(function (th) {
            th.addEventListener("click", function () {
                var key = th.getAttribute("data-sort-key");
                if (nodesSortKey === key) {
                    nodesSortAsc = !nodesSortAsc;
                } else {
                    nodesSortKey = key;
                    nodesSortAsc = true;
                }
                renderNodesTable();
            });
        });
    }

    // --- Backup Tab ---

    var backupFormat = document.getElementById("backup-format");
    var backupTables = document.getElementById("backup-tables");
    var backupVacuum = document.getElementById("backup-vacuum");
    var backupCompress = document.getElementById("backup-compress");
    var backupNoLeader = document.getElementById("backup-noleader");
    var backupBtn = document.getElementById("backup-btn");
    var backupStatusDiv = document.getElementById("backup-status");

    // Show/hide tables field based on format selection.
    var tablesField = document.getElementById("tables-field");
    backupFormat.addEventListener("change", function () {
        var isSql = backupFormat.value === "sql";
        tablesField.style.display = isSql ? "" : "none";
        if (!isSql) backupTables.value = "";
    });

    backupBtn.addEventListener("click", function () {
        var params = [];
        var fmt = backupFormat.value;
        var filename = "backup";

        if (fmt) {
            params.push("fmt=" + fmt);
        }
        if (fmt === "sql") {
            filename += ".sql";
            var tables = backupTables.value.trim();
            if (tables) {
                params.push("tables=" + encodeURIComponent(tables));
            }
        } else {
            filename += ".sqlite3";
        }
        if (backupVacuum.checked) {
            params.push("vacuum");
        }
        if (backupCompress.checked) {
            params.push("compress");
            filename += ".gz";
        }
        if (backupNoLeader.checked) {
            params.push("noleader");
        }

        var url = "/db/backup";
        if (params.length > 0) {
            url += "?" + params.join("&");
        }

        backupBtn.disabled = true;
        backupStatusDiv.innerHTML = "";

        fetch(url)
            .then(function (resp) {
                if (!resp.ok) {
                    return resp.text().then(function (text) {
                        throw new Error(text || ("HTTP " + resp.status));
                    });
                }
                return resp.blob();
            })
            .then(function (blob) {
                var a = document.createElement("a");
                a.href = URL.createObjectURL(blob);
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(a.href);
                backupStatusDiv.innerHTML = '<span class="success">Backup downloaded successfully.</span>';
            })
            .catch(function (err) {
                backupStatusDiv.innerHTML = '<span class="error">Backup failed: ' + escapeHTML(err.message) + '</span>';
            })
            .finally(function () {
                backupBtn.disabled = false;
            });
    });
})();
