// Run with: node http/console/tests/api.test.js
"use strict";
const assert = require("assert");
const api = require("../static/js/api.js");

function response(status, text, statusText) {
    return {
        ok: status >= 200 && status < 300,
        status: status,
        statusText: statusText || "",
        text: () => Promise.resolve(text)
    };
}

async function fails(resp, message) {
    let err = null;
    try {
        await api.readResponse(resp);
    } catch (e) {
        err = e;
    }
    assert(err instanceof Error, "expected a rejection with: " + message);
    assert.strictEqual(err.message, message);
}

(async function () {
    // Successful requests resolve with the status and the decoded body.
    {
        const r = await api.readResponse(response(200, '{"results":[{"last_insert_id":1,"rows_affected":1}]}'));
        assert.strictEqual(r.status, 200);
        assert.deepStrictEqual(r.data, { results: [{ last_insert_id: 1, rows_affected: 1 }] });
    }
    assert.deepStrictEqual((await api.readResponse(response(200, '{"results":[]}'))).data, { results: [] });

    // An error for a single statement is not a failure of the request.
    {
        const r = await api.readResponse(response(200, '{"results":[{"error":"near \\"SELEC\\": syntax error"}]}'));
        assert.strictEqual(r.data.results[0].error, 'near "SELEC": syntax error');
    }

    // HTTP 200 with a top-level error is a failed request, with or without
    // an accompanying results array.
    await fails(response(200, '{"results":[],"error":"database unavailable"}'), "database unavailable");
    await fails(response(200, '{"error":"query timeout"}'), "query timeout");

    // Non-2xx responses fail. Their bodies are usually plain text.
    await fails(response(503, "leader not found\n"), "HTTP 503: leader not found");
    await fails(response(401, "Unauthorized\n"), "HTTP 401: Unauthorized");
    await fails(response(400, '{"error":"invalid freshness"}'), "HTTP 400: invalid freshness");
    await fails(response(500, "", "Internal Server Error"), "HTTP 500: Internal Server Error");
    await fails(response(502, ""), "HTTP 502: request failed");

    // A 2xx response which is not a JSON object is not a usable response.
    await fails(response(200, "<html>proxy error</html>"), "Server returned an invalid response");
    await fails(response(200, ""), "Server returned an invalid response");
    await fails(response(200, "null"), "Server returned an invalid response");
    await fails(response(200, "42"), "Server returned an invalid response");

    console.log("api tests passed");
})().catch(function (err) {
    console.error(err);
    process.exitCode = 1;
});
