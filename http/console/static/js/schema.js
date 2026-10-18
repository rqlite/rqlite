/* Schema model: shapes the console's schema query results for rendering. */
(function (root) {
    "use strict";

    // SQL identifiers are arbitrary strings. Names such as "constructor" or
    // "__proto__" are valid table names, so anything keyed by an identifier
    // must be a Map, never a plain object.

    // tableNames returns the distinct table names from the schema query, in
    // the order they first appear.
    function tableNames(data) {
        var names = [];
        var seen = new Set();
        var rows = (data && data.results && data.results[0] && data.results[0].rows) || [];
        rows.forEach(function (row) {
            var n = row.table_name;
            if (n && !seen.has(n)) {
                seen.add(n);
                names.push(n);
            }
        });
        return names;
    }

    // tables groups the schema query's column rows by table, preserving the
    // order in which tables first appear. Each entry carries the table's
    // name, its CREATE statement, and its column rows.
    function tables(rows) {
        var byName = new Map();
        var ordered = [];
        (rows || []).forEach(function (row) {
            var name = row.table_name;
            var t = byName.get(name);
            if (!t) {
                t = { name: name, sql: row.table_sql, columns: [] };
                byName.set(name, t);
                ordered.push(t);
            }
            t.columns.push(row);
        });
        return ordered;
    }

    // rowCounts pairs each table name with the value of its COUNT(*) query,
    // which were issued in the same order as names. Tables whose count
    // failed are left out.
    function rowCounts(names, results) {
        var counts = new Map();
        (results || []).forEach(function (r, i) {
            if (i < names.length && r && !r.error && r.values && r.values[0]) {
                counts.set(names[i], r.values[0][0]);
            }
        });
        return counts;
    }

    var api = { tableNames: tableNames, tables: tables, rowCounts: rowCounts };
    if (typeof module !== "undefined" && module.exports) module.exports = api;
    else root.ConsoleSchema = api;
})(typeof window !== "undefined" ? window : this);
