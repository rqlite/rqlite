// Run with: node http/console/tests/core.test.js
"use strict";
const assert = require("assert");
const U = require("../static/js/core.js");
let passed = 0;
function test(name, fn) {
    return Promise.resolve().then(fn).then(() => { passed++; console.log("PASS " + name); });
}
function rejects(promise, pattern) {
    return promise.then(() => { throw new Error("Expected rejection"); }, err => { assert(pattern.test(err.message), err.message); });
}
function response(status, text) {
    return { ok: status >= 200 && status < 300, status, text: () => Promise.resolve(text) };
}
(async function () {
    await test("preserves SQLite int64 boundaries, arrays, strings, and exponent literals", () => {
        const json = '{"columns":["id","id"],"values":[[9223372036854775807,-9223372036854775808],[9007199254740993,1e400],[1.5,"9223372036854775807"]]}';
        const result = U.parseJSON(json);
        assert.strictEqual(String(result.values[0][0]), "9223372036854775807");
        assert.strictEqual(U.stringifyJSON(result), json);
        assert.strictEqual(result.values[2][1], "9223372036854775807");
        assert.strictEqual(U.parseJSON(U.stringifyJSON(result, true)).columns.length, 2);
    });
    await test("lossless number markers cannot collide with strings or escaped keys", () => {
        const json = '{"v":"\\u005f_rqlite_number__0","__rqlite_number___0":"plain","n":9007199254740993}';
        const result = U.parseJSON(json);
        assert.strictEqual(result.v, "__rqlite_number__0");
        assert.strictEqual(result.__rqlite_number___0, "plain");
        assert.strictEqual(String(result.n), "9007199254740993");
        assert.strictEqual(U.parseJSON('{"__proto__":{"polluted":true}}').__proto__.polluted, true);
        assert.strictEqual({}.polluted, undefined);
    });
    await test("rejects malformed JSON before replacing numeric tokens", () => {
        ['[01]', '[+2]', '[NaN]', '{"a":1,}', '[1e]', '["unterminated]', '[Infinity]'].forEach(text => assert.throws(() => U.parseJSON(text)));
    });
    await test("CSV retains exact integers, duplicate columns, CR, quotes, null and empty values", () => {
        const result = U.parseJSON('{"columns":["id","id","text"],"values":[[9007199254740993,null,"a\\rb"],[1,"NULL",""],[2,"a,b","a\\\"b"]]}');
        assert.strictEqual(U.resultCSV(result), 'id,id,text\r\n9007199254740993,NULL,"a\rb"\r\n1,"NULL",""\r\n2,"a,b","a""b"');
        assert.strictEqual(U.stringifyJSON(result).includes('"columns":["id","id","text"]'), true);
    });
    await test("splits scripts without splitting strings, identifiers or comments", () => {
        const statements = U.splitSQL('-- comment;\nSELECT \'a;it\'\'s\', "x;y", [z;w], `a;b`; /* ; */ SELECT 2; -- trailing');
        assert.strictEqual(statements.length, 2);
        assert.strictEqual(U.splitSQL(";; -- only a comment\n/*empty*/").length, 0);
        assert.strictEqual(U.splitSQL("SELECT 1; SELECT 2").length, 2);
        assert.throws(() => U.splitSQL("SELECT 'unterminated"), /Unclosed/);
        assert.throws(() => U.splitSQL("SELECT 1; /* unfinished"), /Unclosed/);
    });
    await test("preserves full trigger bodies, CASE, end identifiers and EXPLAIN", () => {
        ['CREATE TRIGGER', 'CREATE TEMP TRIGGER', 'EXPLAIN CREATE TRIGGER'].forEach(prefix => {
            const trigger = prefix + " tr AFTER INSERT ON t BEGIN UPDATE t SET end=CASE WHEN NEW.x=1 THEN 2 ELSE 3 END; SELECT 'END;'; END;";
            assert.deepStrictEqual(U.splitSQL(trigger + " SELECT 9;"), [trigger, "SELECT 9;"]);
        });
        assert.throws(() => U.splitSQL("CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1;"), /Incomplete trigger/);
    });
    await test("forms single and batched parameterized requests without losing integer parameters", () => {
        assert.deepStrictEqual(U.requestBody("SELECT 1; SELECT 2", ""), ["SELECT 1;", "SELECT 2"]);
        const body = U.requestBody("SELECT ?", "[9007199254740993]");
        assert.strictEqual(U.stringifyJSON(body), '[["SELECT ?",9007199254740993]]');
        assert.strictEqual(U.requestBody("SELECT :n; SELECT :n", '{"n":2}').length, 2);
        assert.throws(() => U.requestBody("SELECT ?; SELECT ?", "[1]"), /Positional/);
        assert.throws(() => U.requestBody("SELECT 1", "null"), /Parameters/);
        assert.throws(() => U.requestBody("-- empty", ""), /Enter a SQL/);
        assert.throws(() => U.requestBody("/*comment*/ BEGIN; INSERT INTO t VALUES(1); COMMIT;", ""), /Execute atomically/);
    });
    await test("handles plaintext, empty HTTP errors and top-level JSON errors", async () => {
        await rejects(U.readResponse(response(503, "leader not found")), /HTTP 503: leader not found/);
        await rejects(U.readResponse(response(401, "")), /HTTP 401/);
        await rejects(U.readResponse(response(200, '{"error":"query timeout"}')), /query timeout/);
        await rejects(U.readResponse(response(400, '{"error":"invalid freshness"}')), /HTTP 400: invalid freshness/);
        await rejects(U.readResponse(response(200, "<html>proxy error</html>")), /invalid JSON/);
        const r = await U.readResponse(response(200, '{"results":[{"error":"syntax error"}]}'));
        assert.strictEqual(r.data.results[0].error, "syntax error");
    });
    console.log(passed + " tests passed");
})().catch(err => { console.error(err); process.exitCode = 1; });
