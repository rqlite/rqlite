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

    function formatTimeOfDay(date) {
        var d = date || new Date();
        var hh = String(d.getHours()).padStart(2, "0");
        var mm = String(d.getMinutes()).padStart(2, "0");
        var ss = String(d.getSeconds()).padStart(2, "0");
        return hh + ":" + mm + ":" + ss;
    }

    // copyToClipboard copies text to the clipboard, returning a Promise that
    // resolves on success and rejects on failure. Uses the async Clipboard
    // API when available (secure contexts only), otherwise falls back to a
    // hidden textarea + document.execCommand("copy") for plain-HTTP serves.
    function copyToClipboard(text) {
        if (navigator.clipboard && window.isSecureContext) {
            return navigator.clipboard.writeText(text);
        }
        return new Promise(function (resolve, reject) {
            var ta = document.createElement("textarea");
            ta.value = text;
            ta.setAttribute("readonly", "");
            ta.style.position = "fixed";
            ta.style.top = "0";
            ta.style.left = "0";
            ta.style.opacity = "0";
            document.body.appendChild(ta);
            ta.select();
            var ok = false;
            try {
                ok = document.execCommand("copy");
            } catch (e) {
                ok = false;
            }
            document.body.removeChild(ta);
            if (ok) {
                resolve();
            } else {
                reject(new Error("Copy not supported in this context"));
            }
        });
    }

    // --- Dark Mode ---

    var THEME_KEY = "rqlite_theme";
    var themeToggle = document.getElementById("theme-toggle");

    function applyTheme(theme) {
        document.documentElement.setAttribute("data-theme", theme);
        themeToggle.textContent = theme === "dark" ? "\u2600" : "\u263E";
        themeToggle.title = theme === "dark" ? "Switch to light mode" : "Switch to dark mode";
    }

    var savedTheme = localStorage.getItem(THEME_KEY) || "light";
    applyTheme(savedTheme);

    themeToggle.addEventListener("click", function () {
        var current = document.documentElement.getAttribute("data-theme") || "light";
        var next = current === "dark" ? "light" : "dark";
        localStorage.setItem(THEME_KEY, next);
        applyTheme(next);
    });

    // --- Node Info (header) ---

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
            if (target === "schema") {
                loadSchema();
            }
        });
    });

    // --- Query Tab ---

    var sqlInput = document.getElementById("sql-input");
    var executeBtn = document.getElementById("execute-btn");
    var resultsDiv = document.getElementById("query-results");
    var queryLevel = document.getElementById("query-level");
    var queryFreshness = document.getElementById("query-freshness");
    var queryFreshnessStrict = document.getElementById("query-freshness-strict");
    var queryActions = document.querySelector("#query .query-actions");

    var QUERY_LEVEL_KEY = "rqlite_query_level";
    var savedLevel = localStorage.getItem(QUERY_LEVEL_KEY);
    if (savedLevel) {
        queryLevel.value = savedLevel;
    }
    updateFreshnessVisibility();

    queryLevel.addEventListener("change", function () {
        localStorage.setItem(QUERY_LEVEL_KEY, queryLevel.value);
        updateFreshnessVisibility();
    });

    function updateFreshnessVisibility() {
        if (queryLevel.value === "none") {
            queryActions.classList.add("show-freshness");
        } else {
            queryActions.classList.remove("show-freshness");
        }
    }

    executeBtn.addEventListener("click", executeQuery);

    sqlInput.addEventListener("keydown", function (e) {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            e.preventDefault();
            executeQuery();
        }
    });

    // --- Auto-grow textarea ---

    function autoGrow() {
        sqlInput.style.height = "auto";
        sqlInput.style.height = sqlInput.scrollHeight + "px";
    }

    sqlInput.addEventListener("input", autoGrow);

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
                autoGrow();
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

    function buildRequestURL() {
        var params = ["timings"];
        var level = queryLevel.value;
        if (level && level !== "weak") {
            params.push("level=" + encodeURIComponent(level));
        }
        if (level === "none") {
            var f = queryFreshness.value.trim();
            if (f) {
                params.push("freshness=" + encodeURIComponent(f));
            }
            if (queryFreshnessStrict.checked) {
                params.push("freshness_strict");
            }
        }
        return "/db/request?" + params.join("&");
    }

    // --- Last query results (for export) ---
    var lastQueryResults = null;

    function executeQuery() {
        var sql = sqlInput.value.trim();
        if (!sql) return;

        executeBtn.disabled = true;
        resultsDiv.innerHTML = "";

        saveToHistory(sql);

        apiRequest("POST", buildRequestURL(), [sql])
            .then(function (resp) {
                lastQueryResults = resp.data;
                renderResults(resp.data);
            })
            .catch(function (err) {
                lastQueryResults = null;
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
        data.results.forEach(function (result, idx) {
            if (result.error) {
                html += '<div class="result-error">' + escapeHTML(result.error) + '</div>';
            } else if (isQueryResult(result)) {
                html += renderTable(result);
                html += renderExportButtons(idx);
            } else {
                html += renderWriteResult(result);
            }
            if (result.time) {
                html += '<div class="result-timing">Query time: ' + result.time.toFixed(6) + 's</div>';
            }
        });
        resultsDiv.innerHTML = html;
    }

    // Event delegation for export buttons
    resultsDiv.addEventListener("click", function (e) {
        var btn = e.target;
        if (btn.classList.contains("export-csv") || btn.classList.contains("export-json")) {
            var format = btn.classList.contains("export-csv") ? "csv" : "json";
            var resultIndex = parseInt(btn.getAttribute("data-result-index"), 10);
            copyResultAs(format, resultIndex, btn);
        }
    });

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
            html += '<tr><td colspan="' + columns.length + '" style="text-align:center;color:var(--text-muted);">No rows returned</td></tr>';
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

    function renderExportButtons(resultIndex) {
        return '<div class="result-export">' +
            '<button class="export-csv" data-result-index="' + resultIndex + '">Copy CSV</button>' +
            '<button class="export-json" data-result-index="' + resultIndex + '">Copy JSON</button>' +
            '</div>';
    }

    function copyResultAs(format, resultIndex, btn) {
        if (!lastQueryResults || !lastQueryResults.results || !lastQueryResults.results[resultIndex]) return;
        var result = lastQueryResults.results[resultIndex];
        var columns = result.columns || [];
        var values = result.values || [];
        var text;

        if (format === "csv") {
            var lines = [];
            lines.push(columns.map(csvEscape).join(","));
            values.forEach(function (row) {
                var cells = [];
                for (var j = 0; j < columns.length; j++) {
                    var val = j < row.length ? row[j] : null;
                    cells.push(val === null || val === undefined ? "NULL" : csvEscape(String(val)));
                }
                lines.push(cells.join(","));
            });
            text = lines.join("\n");
        } else {
            var rows = [];
            values.forEach(function (row) {
                var obj = {};
                columns.forEach(function (col, j) {
                    obj[col] = j < row.length ? row[j] : null;
                });
                rows.push(obj);
            });
            text = JSON.stringify(rows, null, 2);
        }

        var orig = btn.textContent;
        copyToClipboard(text).then(function () {
            btn.textContent = "Copied!";
            setTimeout(function () { btn.textContent = orig; }, 1500);
        }, function () {
            btn.textContent = "Copy failed";
            setTimeout(function () { btn.textContent = orig; }, 1500);
        });
    }

    function csvEscape(val) {
        var s = String(val);
        if (s.indexOf(",") !== -1 || s.indexOf('"') !== -1 || s.indexOf("\n") !== -1) {
            return '"' + s.replace(/"/g, '""') + '"';
        }
        return s;
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

    var rawJsonWrapper = document.getElementById("raw-json-wrapper");
    var copyJsonBtn = document.getElementById("copy-json-btn");

    rawJsonBtn.addEventListener("click", function () {
        if (rawJsonWrapper.classList.contains("hidden")) {
            rawJsonWrapper.classList.remove("hidden");
            rawJsonBtn.textContent = "Hide Raw JSON";
            if (lastStatusData) {
                rawJsonPre.textContent = JSON.stringify(lastStatusData, null, 2);
            }
        } else {
            rawJsonWrapper.classList.add("hidden");
            rawJsonBtn.textContent = "Show Raw JSON";
        }
    });

    copyJsonBtn.addEventListener("click", function () {
        copyToClipboard(rawJsonPre.textContent).then(function () {
            copyJsonBtn.textContent = "\u2714";
            setTimeout(function () { copyJsonBtn.textContent = "\u2398"; }, 1500);
        }, function () {
            copyJsonBtn.title = "Copy not supported in this context";
            copyJsonBtn.textContent = "\u2718";
            setTimeout(function () { copyJsonBtn.textContent = "\u2398"; }, 1500);
        });
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
            if (!rawJsonWrapper.classList.contains("hidden")) {
                rawJsonPre.textContent = JSON.stringify(lastStatusData, null, 2);
            }
            lastUpdatedSpan.textContent = "Updated " + formatTimeOfDay();
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
        var mux = data.mux || {};
        var snapStore = store.snapshot_store || {};

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
                    ["RO Connections (in-use / idle)", connRO.in_use !== undefined ? connRO.in_use + " / " + connRO.idle : undefined],
                    ["Extensions", Array.isArray(sqlite.extensions) ? (sqlite.extensions.length > 0 ? sqlite.extensions.join(", ") : "None") : undefined]
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
                    ["TLS", mux.tls]
                ]
            },
            {
                title: "Snapshots",
                rows: [
                    ["Interval", store.snapshot_interval],
                    ["Threshold", store.snapshot_threshold],
                    ["Count", Array.isArray(snapStore.snapshots) ? snapStore.snapshots.length : undefined],
                    ["Snapshots", Array.isArray(snapStore.snapshots) ? (snapStore.snapshots.length > 0 ? snapStore.snapshots.join(", ") : "None") : undefined]
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
                    display = '<span style="color:var(--text-muted)">N/A</span>';
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
        { key: "leader", label: "Leader" },
        { key: "reachable", label: "Reachable" }
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
                var uiURL = apiURL.replace(/\/$/, "") + "/console/";
                html += '<td><a href="' + escapeHTML(uiURL) + '">' + escapeHTML(apiURL) + '</a></td>';
            } else {
                html += "<td>N/A</td>";
            }
            html += "<td>" + escapeHTML(node.addr) + "</td>";
            html += "<td>" + (node.voter ? "Yes" : "No") + "</td>";
            html += "<td>" + (node.leader ? "Yes" : "No") + "</td>";
            html += '<td><span class="status-dot ' + (node.reachable ? 'reachable' : 'unreachable') + '" title="' + (node.reachable ? 'Reachable' : 'Unreachable') + '"></span></td>';
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

    // --- Schema Tab ---

    var schemaContent = document.getElementById("schema-content");
    var schemaRefreshBtn = document.getElementById("schema-refresh-btn");
    var schemaLastUpdated = document.getElementById("schema-last-updated");

    var SCHEMA_TABLES_QUERY = "SELECT m.name AS table_name, m.sql AS table_sql, p.name AS column_name, p.type, p.\"notnull\" AS not_null, p.dflt_value, p.pk FROM sqlite_master m JOIN pragma_table_info(m.name) p WHERE m.type = 'table' ORDER BY m.name, p.cid";
    var SCHEMA_OBJECTS_QUERY = "SELECT name, type, tbl_name, sql FROM sqlite_master WHERE type IN ('index', 'trigger') AND sql IS NOT NULL ORDER BY type, tbl_name, name";
    var SCHEMA_COUNT_ROWS_KEY = "rqlite_schema_count_rows";

    function shouldCountRows() {
        return localStorage.getItem(SCHEMA_COUNT_ROWS_KEY) === "true";
    }

    schemaRefreshBtn.addEventListener("click", loadSchema);

    function slugify(s) {
        return String(s).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
    }

    function loadSchema() {
        schemaRefreshBtn.disabled = true;
        schemaContent.innerHTML = '<div class="schema-loading">Loading schema...</div>';

        apiRequest("POST", "/db/query?associative", [SCHEMA_TABLES_QUERY, SCHEMA_OBJECTS_QUERY])
            .then(function (resp) {
                var tableNames = extractTableNames(resp.data);
                if (tableNames.length === 0 || !shouldCountRows()) {
                    renderSchema(resp.data, {});
                    schemaLastUpdated.textContent = "Last updated: " + formatTimeOfDay();
                    return;
                }
                var countQueries = tableNames.map(function (n) {
                    return "SELECT COUNT(*) FROM " + sqlQuoteIdent(n);
                });
                return apiRequest("POST", "/db/query?level=none", countQueries)
                    .then(function (cresp) {
                        var counts = {};
                        var results = (cresp.data && cresp.data.results) || [];
                        results.forEach(function (r, i) {
                            if (r && !r.error && r.values && r.values[0]) {
                                counts[tableNames[i]] = r.values[0][0];
                            }
                        });
                        renderSchema(resp.data, counts);
                        schemaLastUpdated.textContent = "Last updated: " + formatTimeOfDay();
                    }, function () {
                        renderSchema(resp.data, {});
                        schemaLastUpdated.textContent = "Last updated: " + formatTimeOfDay();
                    });
            })
            .catch(function (err) {
                schemaContent.innerHTML = '<div class="result-error">' + escapeHTML(err.message) + '</div>';
            })
            .finally(function () {
                schemaRefreshBtn.disabled = false;
            });
    }

    function extractTableNames(data) {
        var names = [];
        var seen = {};
        var rows = (data && data.results && data.results[0] && data.results[0].rows) || [];
        rows.forEach(function (row) {
            var n = row.table_name;
            if (n && !seen[n]) {
                seen[n] = true;
                names.push(n);
            }
        });
        return names;
    }

    function renderSchema(data, rowCounts) {
        rowCounts = rowCounts || {};
        if (!data || !data.results || data.results.length === 0) {
            schemaContent.innerHTML = '<div class="result-error">No results returned</div>';
            return;
        }
        var tableResult = data.results[0] || {};
        var objectResult = data.results[1] || {};
        if (tableResult.error) {
            schemaContent.innerHTML = '<div class="result-error">' + escapeHTML(tableResult.error) + '</div>';
            return;
        }

        // Group table columns by table name, preserving order.
        var tables = {};
        var tableOrder = [];
        (tableResult.rows || []).forEach(function (row) {
            var name = row.table_name;
            if (!tables[name]) {
                tables[name] = { sql: row.table_sql, columns: [] };
                tableOrder.push(name);
            }
            tables[name].columns.push(row);
        });

        // Group indexes and triggers by type.
        var indexes = [];
        var triggers = [];
        (objectResult.rows || []).forEach(function (row) {
            if (row.type === "index") indexes.push(row);
            else if (row.type === "trigger") triggers.push(row);
        });

        if (tableOrder.length === 0 && indexes.length === 0 && triggers.length === 0) {
            schemaContent.innerHTML = '<div class="schema-empty">No tables, indexes, or triggers found.</div>';
            return;
        }

        var html = "";

        html += '<div class="schema-actions">';
        html += '<button type="button" class="schema-expand-all">Expand all</button>';
        html += '<button type="button" class="schema-collapse-all">Collapse all</button>';
        html += '<label class="schema-count-toggle" title="Run COUNT(*) on every table when loading the schema">';
        html += '<input type="checkbox" class="schema-count-rows"' + (shouldCountRows() ? ' checked' : '') + '> Count rows';
        html += '</label>';
        html += '</div>';

        // Tables.
        tableOrder.forEach(function (tableName) {
            var t = tables[tableName];
            var anchor = "schema-table-" + slugify(tableName);
            html += '<div class="detail-section schema-section open" id="' + escapeHTML(anchor) + '">';
            html += '<div class="detail-section-header">';
            html += '<span><span class="schema-kind">table</span> ' + escapeHTML(tableName);
            var columnsStr = t.columns.length + ' column' + (t.columns.length === 1 ? '' : 's');
            html += ' <span class="schema-count">(' + escapeHTML(columnsStr);
            if (Object.prototype.hasOwnProperty.call(rowCounts, tableName)) {
                var rc = Number(rowCounts[tableName]);
                var rowsStr = rc.toLocaleString() + ' row' + (rc === 1 ? '' : 's');
                html += ', <span class="schema-rowcount-text" title="Row count read with &#39;none&#39; consistency &mdash; may be slightly stale">' + escapeHTML(rowsStr) + '</span>';
            }
            html += ')</span></span>';
            html += '<span class="arrow">&#9654;</span>';
            html += '</div>';
            html += '<div class="detail-section-body">';
            html += '<table class="result-table schema-columns"><thead><tr>';
            html += '<th>Column</th>';
            html += '<th>Type</th>';
            html += '<th class="schema-center">Not Null</th>';
            html += '<th>Default</th>';
            html += '</tr></thead><tbody>';
            t.columns.forEach(function (col) {
                html += '<tr' + (col.pk ? ' class="schema-pk-row"' : '') + '>';
                html += '<td>' + escapeHTML(col.column_name);
                if (col.pk) {
                    html += ' <span class="schema-pk-badge" title="Primary key">&#128273;</span>';
                }
                html += '</td>';
                html += '<td>' + escapeHTML(col.type) + '</td>';
                html += '<td class="schema-center">' + (col.not_null
                    ? '<span class="schema-check" title="Value may not be NULL">&#10003;</span>'
                    : '<span class="schema-dash" title="Value may be NULL">&mdash;</span>') + '</td>';
                if (col.dflt_value === null || col.dflt_value === undefined) {
                    html += '<td class="null-value">NULL</td>';
                } else {
                    html += '<td>' + escapeHTML(col.dflt_value) + '</td>';
                }
                html += '</tr>';
            });
            html += '</tbody></table>';
            html += '<div class="schema-sql-block">';
            html += '<div class="schema-sql-actions">';
            if (t.sql) {
                html += '<button class="schema-sql-toggle" type="button">Show CREATE TABLE</button>';
            } else {
                html += '<span></span>';
            }
            html += '<button class="schema-drop-table" type="button" data-table-name="' + escapeHTML(tableName) + '">Drop table</button>';
            html += '</div>';
            if (t.sql) {
                html += '<div class="schema-sql-wrapper hidden">';
                html += '<button type="button" class="schema-sql-copy" title="Copy to clipboard">&#x2398;</button>';
                html += '<pre class="schema-sql">' + escapeHTML(t.sql) + '</pre>';
                html += '</div>';
            }
            html += '</div>';
            html += '</div></div>';
        });

        // Indexes.
        indexes.forEach(function (idx) {
            var anchor = "schema-index-" + slugify(idx.name);
            html += '<div class="detail-section schema-section" id="' + escapeHTML(anchor) + '">';
            html += '<div class="detail-section-header">';
            html += '<span><span class="schema-kind">index</span> ' + escapeHTML(idx.name);
            html += ' <span class="schema-count">on ' + escapeHTML(idx.tbl_name) + '</span></span>';
            html += '<span class="arrow">&#9654;</span>';
            html += '</div>';
            html += '<div class="detail-section-body">';
            html += '<pre class="schema-sql">' + escapeHTML(idx.sql) + '</pre>';
            html += '</div></div>';
        });

        // Triggers.
        triggers.forEach(function (trg) {
            var anchor = "schema-trigger-" + slugify(trg.name);
            html += '<div class="detail-section schema-section" id="' + escapeHTML(anchor) + '">';
            html += '<div class="detail-section-header">';
            html += '<span><span class="schema-kind">trigger</span> ' + escapeHTML(trg.name);
            html += ' <span class="schema-count">on ' + escapeHTML(trg.tbl_name) + '</span></span>';
            html += '<span class="arrow">&#9654;</span>';
            html += '</div>';
            html += '<div class="detail-section-body">';
            html += '<pre class="schema-sql">' + escapeHTML(trg.sql) + '</pre>';
            html += '</div></div>';
        });

        schemaContent.innerHTML = html;
    }

    function sqlQuoteIdent(name) {
        return '"' + String(name).replace(/"/g, '""') + '"';
    }

    function dropTable(tableName, btn) {
        btn.disabled = true;
        var sql = "DROP TABLE " + sqlQuoteIdent(tableName);
        apiRequest("POST", "/db/execute", [sql])
            .then(function (resp) {
                var results = (resp.data && resp.data.results) || [];
                var err = results[0] && results[0].error;
                if (err) {
                    window.alert("Failed to drop table \"" + tableName + "\": " + err);
                    btn.disabled = false;
                    return;
                }
                loadSchema();
            })
            .catch(function (err) {
                window.alert("Failed to drop table \"" + tableName + "\": " + err.message);
                btn.disabled = false;
            });
    }

    schemaContent.addEventListener("change", function (e) {
        var t = e.target;
        if (t && t.classList && t.classList.contains("schema-count-rows")) {
            localStorage.setItem(SCHEMA_COUNT_ROWS_KEY, t.checked ? "true" : "false");
            loadSchema();
        }
    });

    schemaContent.addEventListener("click", function (e) {
        var header = e.target.closest && e.target.closest(".detail-section-header");
        if (header && schemaContent.contains(header)) {
            header.parentElement.classList.toggle("open");
            return;
        }

        var btn = e.target;
        if (!btn.classList) return;

        if (btn.classList.contains("schema-sql-toggle")) {
            var block = btn.closest(".schema-sql-block");
            var wrapper = block ? block.querySelector(".schema-sql-wrapper") : null;
            if (!wrapper) return;
            if (wrapper.classList.contains("hidden")) {
                wrapper.classList.remove("hidden");
                btn.textContent = "Hide CREATE TABLE";
            } else {
                wrapper.classList.add("hidden");
                btn.textContent = "Show CREATE TABLE";
            }
            return;
        }

        if (btn.classList.contains("schema-sql-copy")) {
            var wrap = btn.closest(".schema-sql-wrapper");
            var preEl = wrap ? wrap.querySelector(".schema-sql") : null;
            if (!preEl) return;
            copyToClipboard(preEl.textContent).then(function () {
                btn.textContent = "✔";
                setTimeout(function () { btn.innerHTML = "⎘"; }, 1500);
            }, function () {
                btn.title = "Copy not supported in this context";
                btn.textContent = "✘";
                setTimeout(function () { btn.innerHTML = "⎘"; }, 1500);
            });
            return;
        }

        if (btn.classList.contains("schema-drop-table")) {
            var tableName = btn.getAttribute("data-table-name");
            if (!tableName) return;
            var ok = window.confirm("Drop table \"" + tableName + "\"?\n\nThis permanently deletes the table and all its data. This action cannot be undone.");
            if (!ok) return;
            dropTable(tableName, btn);
            return;
        }


        if (btn.classList.contains("schema-expand-all")) {
            schemaContent.querySelectorAll(".schema-section").forEach(function (s) {
                s.classList.add("open");
            });
            return;
        }

        if (btn.classList.contains("schema-collapse-all")) {
            schemaContent.querySelectorAll(".schema-section").forEach(function (s) {
                s.classList.remove("open");
            });
            return;
        }
    });
})();
