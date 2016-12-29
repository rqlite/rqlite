## 3.9.1 (December 29th 2016)
- [PR #247](https://github.com/rqlite/rqlite/pull/247): Simplify loading of SQLite dump files via single command execution. Fixes [issue #246](https://github.com/rqlite/rqlite/issues/246).
- [PR #247](https://github.com/rqlite/rqlite/pull/247): Correct SQLite dump load authentication check.

## 3.9.0 (December 24th 2016)
- [PR #239](https://github.com/rqlite/rqlite/pull/239): Add an API to the `Store` layer for custom peers storage and logging. Thanks @tych0
- [PR #221](https://github.com/rqlite/rqlite/pull/221): Start full support for various SQLite text types. Fix [issue #240](https://github.com/rqlite/rqlite/issues/240).
- [PR #242](https://github.com/rqlite/rqlite/pull/242): Support direct copying of the database via the Store. Thanks @tych0.
- [PR #243](https://github.com/rqlite/rqlite/pull/243): Use Store logging everywhere in the Store package.

## 3.8.0 (December 15th 2016)
- [PR #230](https://github.com/rqlite/rqlite/pull/230): Move Chinook test data to idiomatic testdata directory.
- [PR #232](https://github.com/rqlite/rqlite/pull/232), [PR #233](https://github.com/rqlite/rqlite/pull/233): rqlite CLI now supports accessing secured rqlited nodes. Thanks @tych0.
- [PR #235](https://github.com/rqlite/rqlite/pull/235): Return correct error, if one occurs, during backup. Thanks @tych0.
- [PR #237](https://github.com/rqlite/rqlite/pull/237), [PR #238](https://github.com/rqlite/rqlite/pull/238): Support observing Raft changes. Thanks @tych0

## 3.7.0 (November 24th 2016)
- With this release rqlite is moving to Go 1.7.
- [PR #206](https://github.com/rqlite/rqlite/pull/206), [#217](https://github.com/rqlite/rqlite/pull/217): Support loading data directly from SQLite dump files.
- [PR #209](https://github.com/rqlite/rqlite/pull/209): Tweak help output.
- [PR #229](https://github.com/rqlite/rqlite/pull/229): Remove explicit control of foreign key constraints.
- [PR #207](https://github.com/rqlite/rqlite/pull/207): Database supports returning foreign key constraint status.
- [PR #211](https://github.com/rqlite/rqlite/pull/211): Diagnostics show actual foreign key constraint state.
- [PR #212](https://github.com/rqlite/rqlite/pull/212): Add database configuration to diagnostics output.
- [PR #224](https://github.com/rqlite/rqlite/pull/224), [PR #225](https://github.com/rqlite/rqlite/pull/225): Add low-level database layer expvar stats.

## 3.6.0 (October 1st 2016)
- [PR #195](https://github.com/rqlite/rqlite/pull/195): Set Content-type "application/json" on all HTTP responses.
- [PR #193](https://github.com/rqlite/rqlite/pull/193): Allow joining a cluster through any node, not just the leader.
- [PR #187](https://github.com/rqlite/rqlite/pull/187): Support memory profiling.
- Go cyclo complexity changes.
- With this release Windows compatibility is checked with every build.

## 3.5.0 (September 5th 2016)
- [PR #185](https://github.com/rqlite/rqlite/pull/185): Enable foreign key constraints by default.

## 3.4.1 (September 1st 2016)
- [PR #175](https://github.com/rqlite/rqlite/pull/175): Simplify error handling of Update Peers API.
- [PR #170](https://github.com/rqlite/rqlite/pull/170): Log any failure to call `Serve()` on HTTP service.
- Go lint fixes.
- Go cyclo complexity changes.

## 3.4.0 (July 7th 2016)
- [PR #159](https://github.com/rqlite/rqlite/pull/159): All HTTP responses set X-RQLITE-VERSION.

## 3.3.0 (June 1st 2016)
- [PR #151](https://github.com/rqlite/rqlite/pull/151): Support configurable Raft heartbeat timeout.
- [PR #149](https://github.com/rqlite/rqlite/pull/149): Support configurable Raft snapshot thresholds.
- [PR #148](https://github.com/rqlite/rqlite/pull/148): Support pprof information over HTTP.
- [PR #154](https://github.com/rqlite/rqlite/pull/154): CLI now redirects to leader if necessary.
- [PR #155](https://github.com/rqlite/rqlite/pull/155): CLI now handles "no rows" correctly.

## 3.2.1 (May 22nd 2016)
- [PR #143](https://github.com/rqlite/rqlite/pull/143): Use DELETE as HTTP method to remove nodes.

## 3.2.0 (May 21st 2016)
- [PR #142](https://github.com/rqlite/rqlite/pull/142): Use correct HTTP methods on all endpoints.
- [PR #137](https://github.com/rqlite/rqlite/pull/137): Use resolved version of joining node's address.
- [PR #136](https://github.com/rqlite/rqlite/pull/136): Better errors on join failures.
- [PR #133](https://github.com/rqlite/rqlite/pull/133): Add Peers to status output.
- [PR #132](https://github.com/rqlite/rqlite/pull/132): Support removing a node from a cluster.
- [PR #131](https://github.com/rqlite/rqlite/pull/131): Only convert []byte from database to string if "text". Thanks @bkeroackdsc
- [PR #129](https://github.com/rqlite/rqlite/pull/129): Verify all statements sent to query endpoint begin with "SELECT".
- [PR #141](https://github.com/rqlite/rqlite/pull/141): Store methods to expose node Raft state. Thanks @bkeroack

## 3.1.0 (May 4th 2016)
- [PR #118](https://github.com/rqlite/rqlite/pull/118): New rqlite CLI. Thanks @mkideal
- [PR #125](https://github.com/rqlite/rqlite/pull/125): Add Go runtime details to status endpoint.

## 3.0.1 (May 1st 2016)
- [PR #117](https://github.com/rqlite/rqlite/pull/117): Use Raft advertise address, if exists, during join.

## 3.0.0 (May 1st 2016)
**The Raft log format in 3.0 is not compatible with the 2.0 series**. To upgrade from an earlier version to this release you should backup your 2.0 master node, and replay the database dump into a new 3.0 cluster. The HTTP API remains unchanged however.

- [PR #116](https://github.com/rqlite/rqlite/pull/116): Allow HTTP advertise address to be set.
- [PR #115](https://github.com/rqlite/rqlite/pull/115): Support advertising address different than Raft bind address.
- [PR #113](https://github.com/rqlite/rqlite/pull/113): Switch to in-memory SQLite databases by default.
- [PR #109](https://github.com/rqlite/rqlite/pull/109): Nodes broadcast meta to cluster via Raft.
- [PR #109](https://github.com/rqlite/rqlite/pull/109), [PR #111](https://github.com/rqlite/rqlite/pull/111): Leader redirection
- [PR #104](https://github.com/rqlite/rqlite/pull/104): Handle the `-join` option sensibly when already member of cluster.

## 2.2.2 (April 24th 2016)
- [PR #96](https://github.com/rqlite/rqlite/pull/96): Add build time to status output.
- [PR #101](https://github.com/rqlite/rqlite/pull/101): Fix restore to in-memory databases.

## 2.2.1 (April 19th 2016)
- [PR #95](https://github.com/rqlite/rqlite/pull/95): Correctly set HTTP authentication.

## 2.2.0 (April 18th 2016)
- [PR #84](https://github.com/rqlite/rqlite/pull/84): Encrypted API (HTTPS) now supported.
- [PR #85](https://github.com/rqlite/rqlite/pull/85): BasicAuth support.
- [PR #85](https://github.com/rqlite/rqlite/pull/85): User-level permissions support.
- Print rqlited logo on start-up.
- End-to-end single-node and multi-node unit tests.

## 2.1 (April 9th 2016)
- [PR #76](https://github.com/rqlite/rqlite/pull/76): Obey timing information display at database level.
- [PR #77](https://github.com/rqlite/rqlite/pull/77): Add version information to binary.

## 2.0 (April 5th 2016)
- `timings` URL param to control presence of timing information in response.
- [PR #74](https://github.com/rqlite/rqlite/pull/74): Use SQLite connection directly. Thanks @zmedico.
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
Check out [this tag](https://github.com/rqlite/rqlite/releases/tag/v1.0) for full details.

