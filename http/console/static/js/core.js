/* Pure helpers shared by the console and its dependency-free tests. */
(function (root) {
    "use strict";

    // Keep unsafe JSON numbers as their original literals, not rounded Numbers.
    function ExactNumber(literal) { this.literal = literal; }
    ExactNumber.prototype.toString = function () { return this.literal; };
    ExactNumber.prototype.valueOf = function () { return Number(this.literal); };

    function parseJSON(text) {
        var numbers = [];
        var marked = text.replace(/"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g, function (token) {
            if (token[0] === '"') return token;
            numbers.push(token);
            return token;
        });
        // Validate before replacing numeric tokens, so malformed JSON cannot
        // become valid as a side effect of the lossless conversion.
        var validated = JSON.stringify(JSON.parse(marked));
        var index = 0;
        var prefix = "__rqlite_number__";
        while (validated.indexOf(prefix) !== -1) prefix += "_";
        marked = text.replace(/"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g, function (token) {
            if (token[0] === '"') return token;
            var n = Number(token);
            var id = index++;
            if (!Number.isFinite(n) || (Number.isInteger(n) && !Number.isSafeInteger(n))) {
                return JSON.stringify(prefix + id);
            }
            return token;
        });
        return JSON.parse(marked, function (_, value) {
            if (typeof value === "string" && value.indexOf(prefix) === 0) {
                return new ExactNumber(numbers[Number(value.slice(prefix.length))]);
            }
            return value;
        });
    }

    function stringifyJSON(value, pretty) {
        function encode(v, depth) {
            if (v instanceof ExactNumber) return v.literal;
            if (v === null || typeof v !== "object") return JSON.stringify(v);
            var array = Array.isArray(v);
            var keys = array ? v.map(function (_, i) { return i; }) : Object.keys(v);
            var parts = keys.map(function (key) {
                return (array ? "" : JSON.stringify(key) + (pretty ? ": " : ":")) + encode(v[key], depth + 1);
            });
            var open = array ? "[" : "{";
            var close = array ? "]" : "}";
            if (!parts.length) return open + close;
            if (!pretty) return open + parts.join(",") + close;
            var indent = "  ".repeat(depth + 1);
            return open + "\n" + indent + parts.join(",\n" + indent) + "\n" + "  ".repeat(depth) + close;
        }
        return encode(value, 0);
    }

    function cellText(value) {
        if (value === null || value === undefined) return "NULL";
        if (value instanceof ExactNumber) return value.literal;
        return typeof value === "object" ? stringifyJSON(value) : String(value);
    }

    function csvCell(value) {
        if (value === null || value === undefined) return "NULL";
        var s = cellText(value);
        // Quote literal NULL and empty strings to distinguish them from nulls.
        return /[",\r\n]/.test(s) || s === "NULL" || s === "" ? '"' + s.replace(/"/g, '""') + '"' : s;
    }

    function resultCSV(result) {
        return [result.columns.map(csvCell).join(",")].concat((result.values || []).map(function (row) {
            return result.columns.map(function (_, i) { return csvCell(row[i]); }).join(",");
        })).join("\r\n");
    }

    // SQLite strings, quoted identifiers, and comments must stay intact when
    // splitting scripts. Retain token offsets for editor highlighting, too.
    function sqlTokens(sql, tolerant) {
        var tokens = [];
        var i = 0;
        while (i < sql.length) {
            var start = i;
            var c = sql[i];
            var kind = "punctuation";
            if (/\s/.test(c)) {
                kind = "space";
                while (i < sql.length && /\s/.test(sql[i])) i++;
            } else if (sql.slice(i, i + 2) === "--") {
                kind = "comment";
                while (i < sql.length && sql[i] !== "\n") i++;
            } else if (sql.slice(i, i + 2) === "/*") {
                kind = "comment";
                var end = sql.indexOf("*/", i + 2);
                i = end < 0 ? sql.length : end + 2;
                if (end < 0 && !tolerant) throw new Error("Unclosed SQL comment.");
            } else if (c === "'" || c === '"' || c === "`" || c === "[") {
                kind = c === "'" ? "string" : "identifier";
                var close = c === "[" ? "]" : c;
                var closed = false;
                i++;
                while (i < sql.length) {
                    if (sql[i++] === close) {
                        if (close !== "]" && sql[i] === close) { i++; continue; }
                        closed = true;
                        break;
                    }
                }
                if (!closed && !tolerant) throw new Error("Unclosed SQL string or identifier.");
            } else if (/[A-Za-z_]/.test(c)) {
                kind = "word";
                while (i < sql.length && /[A-Za-z_0-9$]/.test(sql[i])) i++;
            } else if (/[0-9]/.test(c)) {
                kind = "number";
                while (i < sql.length && /[0-9.eE]/.test(sql[i])) i++;
            } else {
                i++;
            }
            tokens.push({ text: sql.slice(start, i), kind: kind, start: start, end: i });
        }
        return tokens;
    }

    function splitSQL(sql) {
        // Same completion states as SQLite's sqlite3_complete(): trigger bodies
        // end at ; END ;, not at an internal semicolon or a CASE expression.
        // Columns named "end" and EXPLAIN CREATE TRIGGER work here as well.
        var transitions = [
            [1, 0, 2, 3, 4, 2, 2, 2], // empty
            [1, 1, 2, 3, 4, 2, 2, 2], // statement boundary
            [1, 2, 2, 2, 2, 2, 2, 2], // normal SQL
            [1, 3, 3, 2, 4, 2, 2, 2], // EXPLAIN
            [1, 4, 2, 2, 2, 4, 5, 2], // CREATE [TEMP]
            [6, 5, 5, 5, 5, 5, 5, 5], // trigger body
            [6, 6, 5, 5, 5, 5, 5, 7], // trigger semicolon
            [1, 7, 5, 5, 5, 5, 5, 5]  // trigger END
        ];
        var tokenTypes = { EXPLAIN: 3, CREATE: 4, TEMP: 5, TEMPORARY: 5, TRIGGER: 6, END: 7 };
        var statements = [];
        var state = 0;
        var start = 0;
        var hasSQL = false;
        sqlTokens(sql).forEach(function (token) {
            if (token.kind === "space" || token.kind === "comment") return;
            var type = token.text === ";" ? 0 : (token.kind === "word" ? tokenTypes[token.text.toUpperCase()] || 2 : 2);
            state = transitions[state][type];
            if (state === 1) {
                if (hasSQL) statements.push(sql.slice(start, token.end).trim());
                start = token.end;
                hasSQL = false;
            } else hasSQL = true;
        });
        if (state === 5 || state === 6) throw new Error("Incomplete trigger: expected END;.");
        if (hasSQL) statements.push(sql.slice(start).trim());
        return statements;
    }

    function requestBody(sql, parameters) {
        var statements = splitSQL(sql);
        if (!statements.length) throw new Error("Enter a SQL statement to run.");
        statements.forEach(function (statement) {
            var first = sqlTokens(statement).filter(function (t) { return t.kind !== "space" && t.kind !== "comment"; })[0];
            if (/^(BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)$/i.test(first.text)) {
                throw new Error("Use Execute atomically instead of SQL transaction-control statements.");
            }
        });
        if (!parameters.trim()) return statements;
        var params = parseJSON(parameters);
        if (!params || typeof params !== "object" || params instanceof ExactNumber) {
            throw new Error("Parameters must be a JSON array or object.");
        }
        if (Array.isArray(params) && statements.length !== 1) {
            throw new Error("Positional parameters require one statement. Use named parameters for a batch.");
        }
        return statements.map(function (statement) {
            return Array.isArray(params) ? [statement].concat(params) : [statement, params];
        });
    }

    // HTTP errors may be plain text, even when Content-Type says JSON.
    function readResponse(resp) {
        return resp.text().then(function (text) {
            var data;
            try { data = parseJSON(text); } catch (_) {
                if (resp.ok) throw new Error("Server returned an invalid JSON response.");
            }
            if (!resp.ok) {
                throw new Error("HTTP " + resp.status + ": " + ((data && data.error) || text || resp.statusText || "Request failed"));
            }
            if (data && data.error) throw new Error(String(data.error));
            if (!data || typeof data !== "object") throw new Error("Server returned an empty or invalid response.");
            return { status: resp.status, data: data };
        });
    }

    var api = { ExactNumber: ExactNumber, parseJSON: parseJSON, stringifyJSON: stringifyJSON,
        cellText: cellText, resultCSV: resultCSV, sqlTokens: sqlTokens, splitSQL: splitSQL,
        requestBody: requestBody, readResponse: readResponse };
    if (typeof module !== "undefined" && module.exports) module.exports = api;
    else root.ConsoleUtils = api;
})(typeof window !== "undefined" ? window : this);
