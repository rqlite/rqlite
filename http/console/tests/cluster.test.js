// Run with: node http/console/tests/cluster.test.js
"use strict";
const assert = require("assert");
const cluster = require("../static/js/cluster.js");
const defaults = { autoRefresh: true, showReadReplicas: true };
assert.deepStrictEqual(cluster.preferences(null, "#topology"), defaults);
assert.deepStrictEqual(cluster.preferences("invalid", "#topology"), defaults);
assert.deepStrictEqual(cluster.preferences('{"autoRefresh":"false","showReadReplicas":null}', "#topology"), defaults);
assert.deepStrictEqual(cluster.preferences('{"autoRefresh":false,"showReadReplicas":false}', "#topology"), { autoRefresh: false, showReadReplicas: false });
for (const autoRefresh of [false, true]) {
    for (const showReadReplicas of [false, true]) {
        const settings = { autoRefresh, showReadReplicas };
        const destination = new URL(cluster.consoleURL("https://replica.example:4003/", settings));
        assert.strictEqual(destination.pathname, "/console/");
        assert.strictEqual(destination.search, "", "preferences belong in the fragment, not the server request");
        assert.deepStrictEqual(cluster.preferences(JSON.stringify({ autoRefresh: !autoRefresh, showReadReplicas: !showReadReplicas }), destination.hash), settings, "node-link preferences override the destination's saved settings");
    }
}
assert.deepStrictEqual(cluster.preferences('{"autoRefresh":false}', "#topology?auto-refresh=invalid&read-replicas=0"), { autoRefresh: false, showReadReplicas: false });
assert.deepStrictEqual(cluster.preferences(null, "#query?auto-refresh=0"), defaults);
assert.deepStrictEqual(cluster.preferences(null, "#cluster?auto-refresh=0"), { autoRefresh: false, showReadReplicas: true });
assert.strictEqual(cluster.consoleURL("javascript:alert(1)", defaults), "");
assert.strictEqual(cluster.consoleURL(undefined, defaults), "");
const status = { store: { node_id: "1", leader: { node_id: "1", addr: "localhost:4002" } } };
const nodes = [
    { id: "2", addr: "localhost:4004", voter: true, reachable: true },
    { id: "1", addr: "localhost:4002", voter: true, reachable: true, leader: true },
    { id: "3", addr: "localhost:4006", voter: false, reachable: false, error: "timeout" }
];
const original = JSON.stringify(nodes);
let model = cluster.topology(status, { nodes: nodes });
assert.strictEqual(model.leader.id, "1");
assert.strictEqual(model.localID, "1");
assert.deepStrictEqual(model.nodes.map(n => n.role), ["Leader", "Follower", "Read Replica"]);
assert.strictEqual(model.nodes[2].error, "timeout");
assert.strictEqual(JSON.stringify(nodes), original, "normalization must not mutate observations");
assert.deepStrictEqual(cluster.layout(model), cluster.layout(cluster.topology(status, { nodes: nodes.slice().reverse() })), "response ordering must not move nodes");

model = cluster.topology(status, { nodes: nodes.map(n => Object.assign({}, n, { leader: false, reachable: false })) });
assert.strictEqual(model.leader.role, "Reported leader");
assert.strictEqual(model.leader.reachable, false);
model = cluster.topology(status, { nodes: nodes.map(n => Object.assign({}, n, { leader: n.id === "2" })) });
assert.strictEqual(model.leader.id, "2", "a probed leader takes precedence over an older status observation");
model = cluster.topology({ store: {} }, { nodes: nodes.map(n => Object.assign({}, n, { leader: false })) });
assert.strictEqual(model.leader, undefined);
assert.strictEqual(model.nodes[0].role, "Voter");
assert.strictEqual(cluster.layout(model).positions.size, 3);
assert.strictEqual(cluster.layout(cluster.topology(status, { nodes: [] })).positions.size, 0);

for (let count = 1; count <= 25; count++) {
    model = cluster.topology(status, { nodes: Array.from({ length: count }, (_, i) => ({ id: String(i + 1), voter: true, leader: i === 0 })) });
    const geometry = cluster.layout(model);
    const positions = Array.from(geometry.positions.values());
    assert.strictEqual(positions.length, count);
    positions.forEach((p, i) => {
        assert(p.x >= 117.5 && p.x <= geometry.width - 117.5, "card must fit horizontally");
        assert(p.y >= 75 && p.y <= geometry.height - 75, "card must fit vertically");
        positions.slice(i + 1).forEach(q => assert(Math.abs(p.x - q.x) >= 235 || Math.abs(p.y - q.y) >= 150, "cards must not overlap for " + count + " nodes"));
    });
}
console.log("Cluster topology and layout checks passed (1–25 nodes).");

const probeNodes = [
    { id: "1", leader: true, voter: true, reachable: true, time: 0.0008 },
    { id: "2", voter: true, reachable: true, time: 0.0012 },
    { id: "3", voter: false, reachable: true, time: 0.00002 }
];
function probeModel(localID) { return cluster.topology({ store: { node_id: localID } }, { nodes: probeNodes }); }
model = probeModel("1");
assert.strictEqual(cluster.probeLabel(model, model.nodes[1], false), "Probe: 1.2 ms");
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "Probe: <0.1 ms");
model = probeModel("2");
assert.strictEqual(cluster.probeLabel(model, model.nodes[1], false), "Probe: 0.8 ms", "follower view must use the leader's probe time");
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "", "do not label a connection between other nodes");
model = probeModel("3");
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "Probe: 0.8 ms", "replica view probes the leader too");
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], true), "", "omit stale times");
model.leader.reachable = false;
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "", "omit failed probes");
model.leader.reachable = true;
[undefined, null, -1, NaN, Infinity, "0.1"].forEach(time => {
    model.leader.time = time;
    assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "");
});
model.leader.time = 1.25;
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "Probe: 1.3 s");
model.leader = null;
assert.strictEqual(cluster.probeLabel(model, model.nodes[2], false), "");
console.log("Probe timing perspective and unavailable-data checks passed.");
