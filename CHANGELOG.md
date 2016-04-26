## 2.2.3 (unreleased)
- [PR #104](https://github.com/otoolep/rqlite/pull/104): Handle the `-join` option sensibly when already member of cluster.

## 2.2.2 (April 24th 2016)
- [PR #96](https://github.com/otoolep/rqlite/pull/96): Add build time to status output.
- [PR #101](https://github.com/otoolep/rqlite/pull/101): Fix restore to in-memory databases.

## 2.2.1 (April 19th 2016)
- [PR #95](https://github.com/otoolep/rqlite/pull/95): Correctly set HTTP authentication.

## 2.2.0 (April 18th 2016)
- [PR #84](https://github.com/otoolep/rqlite/pull/84): Encrypted API (HTTPS) now supported.
- [PR #85](https://github.com/otoolep/rqlite/pull/85): BasicAuth support.
- [PR #85](https://github.com/otoolep/rqlite/pull/85): User-level permissions support.
- Print rqlited logo on start-up.
- End-to-end single-node and multi-node unit tests.

## 2.1 (April 9th 2016)
- [PR #76](https://github.com/otoolep/rqlite/pull/76): Obey timing information display at database level.
- [PR #77](https://github.com/otoolep/rqlite/pull/77): Add version information to binary.

## 2.0 (April 5th 2016)
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

