## 2.0 (unreleased)

- `timings` URL param to control presence of timing information in response.
- [PR #74](https://github.com/otoolep/rqlite/pull/74): Use SQLite connection directly. Thanks @zmedico.
- Update operations return last-inserted ID.
- Column-oriented API responses.
- Types in API response body.
- Query times in addition to sum of query times.
- New Raft consensus module, built on Hashsicorp's implementation.
- Hot backup support.
- Selectable read-consistency levels of none, weak, and strong.
- SQLite file size added to HTTP API status endpoint.
- expvar support added to HTTP server.

## 1.0 (June 23rd 2015)

Check out [this tag](https://github.com/otoolep/rqlite/releases/tag/v1.0) for full details.

