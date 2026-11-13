/* API responses: decides whether a request to the rqlite HTTP API failed. */
(function (root) {
    "use strict";

    // readResponse takes a fetch Response. It resolves to { status, data }
    // if the request succeeded, and rejects with an Error if it failed.
    //
    // A request can fail in two ways:
    //   - the HTTP status is not 2xx. The body is then usually plain text,
    //     not JSON.
    //   - the HTTP status is 200, but the body has a top-level "error". The
    //     API does this when the request as a whole could not be executed.
    //
    // An error for a single statement, reported inside "results", is not a
    // failure of the request. Those are left for the caller to display.
    function readResponse(resp) {
        return resp.text().then(function (text) {
            var data = null;
            try { data = JSON.parse(text); } catch (_) { /* not JSON */ }
            var isObject = data !== null && typeof data === "object";
            if (!resp.ok) {
                var detail = (isObject && data.error) || text.trim() || resp.statusText || "request failed";
                throw new Error("HTTP " + resp.status + ": " + detail);
            }
            if (!isObject) throw new Error("Server returned an invalid response");
            if (data.error) throw new Error(String(data.error));
            return { status: resp.status, data: data };
        });
    }

    var api = { readResponse: readResponse };
    if (typeof module !== "undefined" && module.exports) module.exports = api;
    else root.ConsoleAPI = api;
})(typeof window !== "undefined" ? window : this);
