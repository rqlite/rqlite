// Run with: node http/console/tests/schema.test.js
"use strict";
const assert = require("assert");
const schema = require("../static/js/schema.js");

// Names that collide with Object.prototype properties are valid SQLite
// identifiers and must behave like any other table name.
const reserved = ["constructor", "__proto__", "toString", "hasOwnProperty", "valueOf"];
const ordinary = ["users", "Orders", "a b", "ünïcode", "123"];

function column(table, name, sql) {
    return { table_name: table, table_sql: sql || "CREATE TABLE " + table, column_name: name, type: "TEXT" };
}

// tables: grouping, order, and reserved names.
for (const name of reserved.concat(ordinary)) {
    const rows = [column(name, "id", "CREATE TABLE x"), column(name, "val")];
    const tables = schema.tables(rows);
    assert.strictEqual(tables.length, 1, name + " must group into one table");
    assert.strictEqual(tables[0].name, name);
    assert.strictEqual(tables[0].sql, "CREATE TABLE x", "sql comes from the first row");
    assert.deepStrictEqual(tables[0].columns.map(c => c.column_name), ["id", "val"]);
}

{
    const rows = [
        column("zeta", "a"), column("constructor", "a"), column("alpha", "a"),
        column("zeta", "b"), column("__proto__", "a"), column("constructor", "b"), column("__proto__", "b")
    ];
    const tables = schema.tables(rows);
    assert.deepStrictEqual(tables.map(t => t.name), ["zeta", "constructor", "alpha", "__proto__"], "first-seen order is preserved");
    assert.deepStrictEqual(tables.map(t => t.columns.length), [2, 2, 1, 2]);
    assert.strictEqual(Object.getPrototypeOf(tables[3]), Object.prototype, "__proto__ entry is an ordinary table object");
}
assert.deepStrictEqual(schema.tables([]), []);
assert.deepStrictEqual(schema.tables(undefined), []);

// tableNames: distinct names in first-seen order from the query response.
function response(rows) { return { results: [{ rows: rows }] }; }
for (const name of reserved.concat(ordinary)) {
    assert.deepStrictEqual(schema.tableNames(response([column(name, "a"), column(name, "b")])), [name]);
}
assert.deepStrictEqual(
    schema.tableNames(response([column("t", "a"), column("__proto__", "a"), column("t", "b"), column("toString", "a"), column("__proto__", "b")])),
    ["t", "__proto__", "toString"]);
assert.deepStrictEqual(schema.tableNames(response([{ table_name: "" }, { table_name: null }, {}])), [], "blank names are skipped");
assert.deepStrictEqual(schema.tableNames(null), []);
assert.deepStrictEqual(schema.tableNames({}), []);
assert.deepStrictEqual(schema.tableNames({ results: [] }), []);
assert.deepStrictEqual(schema.tableNames({ results: [{ error: "boom" }] }), []);

// rowCounts: Map keyed by name, aligned with the count query results.
{
    const names = ["users", "__proto__", "constructor", "broken", "toString"];
    const results = [
        { values: [[3]] },
        { values: [[7]] },
        { values: [[0]] },
        { error: "no such table" },
        { values: [[12]] }
    ];
    const counts = schema.rowCounts(names, results);
    assert(counts instanceof Map);
    assert.strictEqual(counts.size, 4);
    assert.strictEqual(counts.get("users"), 3);
    assert.strictEqual(counts.get("__proto__"), 7);
    assert.strictEqual(counts.get("constructor"), 0, "a zero count is still recorded");
    assert.strictEqual(counts.has("broken"), false, "failed counts are left out");
    assert.strictEqual(counts.get("toString"), 12, "an earlier failure does not shift later counts");
}
assert.strictEqual(schema.rowCounts(["t"], [{ values: [] }]).has("t"), false);
assert.strictEqual(schema.rowCounts(["t"], [null]).has("t"), false);
assert.strictEqual(schema.rowCounts(["t"], undefined).size, 0);
assert.strictEqual(schema.rowCounts(["t"], [{ values: [[1]] }, { values: [[2]] }]).size, 1, "extra results beyond the names are ignored");

console.log("schema tests passed");
