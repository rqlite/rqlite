## 5.8.0 (unreleased)

### New features
- [PR #716](https://github.com/rqlite/rqlite/pull/716): Support HTTP/2 protocol over TLS. Fixes [issue #516](https://github.com/rqlite/rqlite/issues/516).

### Implementation changes and bug fixes
- [PR #711](https://github.com/rqlite/rqlite/pull/711), [PR# 712](https://github.com/rqlite/rqlite/pull/712): Ignore join addresses if node already part of cluster. Fixes [issue #710](https://github.com/rqlite/rqlite/issues/710).
- [PR #715](https://github.com/rqlite/rqlite/pull/715): Compress SQLite database in Raft snapshot.

## 5.7.0 (December 23rd 2020)

### New features
- [PR #696](https://github.com/rqlite/rqlite/pull/696): Benchmarking tool now supports query testing.

### Implementation changes and bug fixes
- [PR #694](https://github.com/rqlite/rqlite/pull/694): Display, in the CLI, the HTTP response body on HTTP status 503.
- [PR #703](https://github.com/rqlite/rqlite/pull/703): Fix potential panic during request parsing.
- [PR #705](https://github.com/rqlite/rqlite/pull/705): Use Protobuf for encoding Raft Log commands.

## 5.6.0 (November 17th 2020)

### New features
- [PR #692](https://github.com/rqlite/rqlite/pull/692): Support setting Raft leader lease timeout.

### Implementation changes and bug fixes
- [PR #693](https://github.com/rqlite/rqlite/pull/693): Upgrade Go mod dependencies, including upgrading Hashicorp Raft to v1.2.0.

## 5.5.1 (October 27th 2020)

- [PR #680](https://github.com/rqlite/rqlite/pull/680), [PR #681](https://github.com/rqlite/rqlite/pull/681): Add missing calls to set BasicAuth in CLI. Fixes [issue #678](https://github.com/rqlite/rqlite/issues/678).
- [PR #682](https://github.com/rqlite/rqlite/pull/682): Explicitly handle "no leader" during backup, and return redirect if necessary.
- [PR #683](https://github.com/rqlite/rqlite/pull/683): Restore request should re-read file every attempt.

## 5.5.0 (September 27th 2020)

### New features
- [PR #673](https://github.com/rqlite/rqlite/pull/673): Support parameterized statements. Fixes [issue #140](https://github.com/rqlite/rqlite/issues/140).

## 5.4.2 (September 25th 2020)

### Implementation changes and bug fixes
- [PR #672](https://github.com/rqlite/rqlite/pull/672): Fix issue causing HTTPS-only redirects.

## 5.4.1 (September 24th 2020)
_This release should not be used, due to a HTTP redirection bug._

### Implementation changes and bug fixes
- [PR #660](https://github.com/rqlite/rqlite/pull/660): Raft log size on disk now reported via status endpoint.
- [PR #670](https://github.com/rqlite/rqlite/pull/670): Add utilities for testing encrypted nodes.
- [PR #671](https://github.com/rqlite/rqlite/pull/671): Set Location for HTTP redirect properly for secure nodes.

## 5.4.0 (June 14th 2020)

### New features
- [PR #654](https://github.com/rqlite/rqlite/pull/654): Allow number of Join attempts and Join interval to be specified at startup. Fixes [issue #653](https://github.com/rqlite/rqlite/issues/653).

## 5.3.0 (May 13th 2020)

### New features
- [PR #641](https://github.com/rqlite/rqlite/pull/641): rqlite CLI now supports node removal.

## 5.2.0 (April 11th 2020)
This release fixes a very significant bug, whereby snapshotting was never occuring due to a zero snapshot-interval being passed to the Raft subsystem. This meant that the Raft log would grow without bound, and could result in very long start-up times if the Raft log was very large.

### New features
- [PR #637](https://github.com/rqlite/rqlite/pull/637): Allow the Raft snapshotting check interval to be set at launch time.

### Implementation changes and bug fixes
- [PR #637](https://github.com/rqlite/rqlite/pull/637): Set the Raft snapshot check interval to 30 seconds by default.

## 5.1.1 (March 28th 2020)
### Implementation changes and bug fixes
- [PR #636](https://github.com/rqlite/rqlite/pull/636): Check consistency level before freshness check. Thanks @wangfenjin

## 5.1.0 (January 10th 2020)

### New features
- [PR #613](https://github.com/rqlite/rqlite/pull/613): Support read-only, non-voting nodes. These provide read scalability for the system.
- [PR #614](https://github.com/rqlite/rqlite/pull/614): Support specifying minimum leader freshness with _None_ consistency queries.
- [PR #620](https://github.com/rqlite/rqlite/pull/620): Log level can be specified for Raft module.

## 5.0.0 (December 30th 2019)
This release uses a new Raft consensus version, with the move to Hashicorp Raft v1. As a result **the Raft system in 5.0 is not compatible with the 4.0 series**. To upgrade from an earlier version to this release you should backup your 4.0 leader node, and restore the database dump into a new 5.0 cluster.

The rqlite server also supports explicitly setting the node ID. While it's not required to set this, it's recommended for production clusters. See the [cluster documentation](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md) for more details.

The HTTP Query and Insert API remains unchanged in the 5.0 series relative to the 4.0 series.

### New features
- [PR #595](https://github.com/rqlite/rqlite/pull/595): rqlite CLI prints Welcome message on startup.
- [PR #608](https://github.com/rqlite/rqlite/pull/608): Add features list to status output.

### Implementation changes and bug fixes
- [PR #597](https://github.com/rqlite/rqlite/pull/597): Don't ignore any Join error, instead return it.
- [PR #598](https://github.com/rqlite/rqlite/pull/598): Ensure backup is correctly closed.
- [PR #600](https://github.com/rqlite/rqlite/pull/600): Move to Hashicorp Raft v1.1.1.
- [PR #601](https://github.com/rqlite/rqlite/pull/601): By default use Raft network address as node ID.
- [PR #602](https://github.com/rqlite/rqlite/pull/602): Add method to Store that returns leader ID.
- [PR #603](https://github.com/rqlite/rqlite/pull/603): Fix up status key name style.
- [PR #604](https://github.com/rqlite/rqlite/pull/604): JSON types are also text.
- [PR #605](https://github.com/rqlite/rqlite/pull/605): Broadcast Store meta via consensus.
- [PR #607](https://github.com/rqlite/rqlite/pull/607): Various Redirect fixes.
- [PR #609](https://github.com/rqlite/rqlite/pull/609): Simplify rqlite implementation.
- [PR #610](https://github.com/rqlite/rqlite/pull/610): Write node backup directly to HTTP response writer. Thanks @sum12.
- [PR #611](https://github.com/rqlite/rqlite/pull/611): Add varadic perm check functions to auth store.

## 4.6.0 (November 29th 2019)
_This release adds significant new functionality to the command-line tool, including much more control over backup and restore of the database. [Visit the Releases page](https://github.com/rqlite/rqlite/releases/tag/v4.6.0) to download this release._

### New features
- [PR #592](https://github.com/rqlite/rqlite/pull/592): Add _dump database_ command (to SQL text file) to CLI.
- [PR #585](https://github.com/rqlite/rqlite/pull/585): Add _backup_ command (to SQLite file) to CLI: Thanks @eariassoto.
- [PR #589](https://github.com/rqlite/rqlite/pull/589): Add _restore_ command (from SQLite file) to CLI. Thanks @eariassoto.
- [PR #584](https://github.com/rqlite/rqlite/pull/584): Support showing query timings in the CLI. Thanks @joaodrp.
- [PR #586](https://github.com/rqlite/rqlite/pull/586): rqlite CLI now supports command-recall via cursor key. Thanks @rhnvrm.
- [PR #564](https://github.com/rqlite/rqlite/pull/564): rqlite server supports specifying trusted root CA certificate. Thanks @zmedico.
- [PR #593](https://github.com/rqlite/rqlite/pull/593): rqlite CLI now supports [HTTP proxy (via Environment)](https://golang.org/pkg/net/http/#ProxyFromEnvironment). Thanks @paulstuart
- [PR #587](https://github.com/rqlite/rqlite/pull/587): Add [expvar](https://golang.org/pkg/expvar/) statistics to store.

### Implementation changes and bug fixes
- [PR #591](https://github.com/rqlite/rqlite/pull/591): Store layer supports generating SQL format backups.
- [PR #590](https://github.com/rqlite/rqlite/pull/590): DB layer supports generating SQL format backups.
- [PR #588](https://github.com/rqlite/rqlite/pull/588): Abort transaction if _load from backup_ operation fails.
- If joining a cluster via HTTP fails, try HTTPS. Fixes [issue #577](https://github.com/rqlite/rqlite/issues/577).

## 4.5.0 (April 24th 2019)
- [PR #551](https://github.com/rqlite/rqlite/pull/551): rqlite CLI supports specifying trusted root CA certificate. Thanks @zmedico.

## 4.4.0 (January 3rd 2019)
- Allow the Raft election timeout [to be set](https://github.com/rqlite/rqlite/commit/2e91858e1ee0feee19f4c20c6f56a21261bcd44a). 
 
## 4.3.1 (October 10th 2018)
- Allow a node to be re-added with same IP address and port, even though it was previously removed. Fixes [issue #534](https://github.com/rqlite/rqlite/issues/534).

## 4.3.0 (March 18th 2018)
- [PR #397](https://github.com/rqlite/rqlite/pull/397), [PR #399](https://github.com/rqlite/rqlite/pull/399): Support hashed passwords. Fixes [issue #395](https://github.com/rqlite/rqlite/issues/395). Thanks @sum12.

## 4.2.3 (February 21st 2018)
- [PR #389](https://github.com/rqlite/rqlite/pull/389): Log Store directory path on startup.
- [PR #392](https://github.com/rqlite/rqlite/pull/392): Return redirect if node removal attempted on follower. Fixes [issue #391](https://github.com/rqlite/rqlite/issues/391).

## 4.2.2 (December 7th 2017)
- [PR #383](https://github.com/rqlite/rqlite/pull/383): Fix unit tests after underlying SQLite master table changes.
- [PR #384](https://github.com/rqlite/rqlite/pull/384): "status" perm required to access Go runtime information.

## 4.2.1 (November 10th 2017)
- [PR #367](https://github.com/rqlite/rqlite/pull/367): Remove superflous leading space at CLI prompt.
- [PR #368](https://github.com/rqlite/rqlite/pull/368): CLI displays clear error message when not authorized.
- [PR #370](https://github.com/rqlite/rqlite/pull/370): CLI does not need to indent JSON when making requests.
- [PR #373](https://github.com/rqlite/rqlite/pull/373), [PR #374](https://github.com/rqlite/rqlite/pull/374): Add simple INSERT-only benchmarking tool.

## 4.2.0 (October 19th 2017)
- [PR #354](https://github.com/rqlite/rqlite/pull/354): Vendor Raft.
- [PR #354](https://github.com/rqlite/rqlite/pull/354): Move to Go 1.9.1.

## 4.1.0 (September 3rd 2017)
- [PR #342](https://github.com/rqlite/rqlite/pull/342): Add missing Store parameters to diagnostic output.
- [PR #343](https://github.com/rqlite/rqlite/pull/343): Support fetching [expvar](https://golang.org/pkg/expvar/) information via CLI.
- [PR #344](https://github.com/rqlite/rqlite/pull/344): Make read-consistency query param value case-insensitive.
- [PR #345](https://github.com/rqlite/rqlite/pull/345): Add unit test coverage for status and expvar endpoints.

## 4.0.2 (August 8th 2017)
- [PR #337](https://github.com/rqlite/rqlite/pull/337): Include any query params with 301 redirect URL.

## 4.0.1 (August 4th 2017)
- [PR #316](https://github.com/rqlite/rqlite/pull/316): CLI doesn't need pretty nor timed responses from node.
- [PR #334](https://github.com/rqlite/rqlite/pull/334): Set Content-Type to "application/octet-stream" for backup endpoint. Fixes [issue #333](https://github.com/rqlite/rqlite/issues/333).

## 4.0.0 (June 18th 2017)
**The 4.0 release has renamed command-line options relative to earlier releases.** This means that previous commands used to launch rqlited will not work. However the command-line changes are cosmetic, and each previous option maps 1-to-1 to a renamed option. Otherwise deployments of earlier releases can be upgraded to the 4.0 series without any other work.

- [PR #309](https://github.com/rqlite/rqlite/pull/309): Tweak start-up logo.
- [PR #308](https://github.com/rqlite/rqlite/pull/308): Move to clearer command-line options.
- [PR #307](https://github.com/rqlite/rqlite/pull/307): Support node-to-node encryption. Fixes [issue #93](https://github.com/rqlite/rqlite/issues/93).
- [PR #310](https://github.com/rqlite/rqlite/pull/310): HTTP service supports registration of Status providers; Mux is first client.
- [PR #315](https://github.com/rqlite/rqlite/pull/315): Add status and help commands to CLI.

## 3.14.0 (May 4th 2017)
- [PR #304](https://github.com/rqlite/rqlite/pull/304): Switch to Go 1.8.1.

## 3.13.0 (April 3rd 2017)
- [PR #296](https://github.com/rqlite/rqlite/pull/296): Log Go version at startup.
- [PR #294](https://github.com/rqlite/rqlite/pull/294): Support multiple explicit join addresses.
- [PR #297](https://github.com/rqlite/rqlite/pull/297): CLI should explicitly handle redirects due to Go1.8. Fixes [issue #295](https://github.com/rqlite/rqlite/issues/295).

## 3.12.1 (March 2nd 2017)
- [PR #291](https://github.com/rqlite/rqlite/pull/291): Don't access Discovery Service if node already part of cluster.

## 3.12.0 (March 1st 2017)
- [PR #286](https://github.com/rqlite/rqlite/pull/286): Tweak help output.
- [PR #283](https://github.com/rqlite/rqlite/pull/283): Main code should log to stderr.
- [PR #280](https://github.com/rqlite/rqlite/pull/280), [PR #281](https://github.com/rqlite/rqlite/pull/281): Integrate with new Discovery Service.
- [PR #282](https://github.com/rqlite/rqlite/pull/282): Retry cluster-join attempts on failure.
- [PR #289](https://github.com/rqlite/rqlite/pull/289): rqlite HTTP clients should not automatically follow redirects.

## 3.11.0 (February 12th 2017)
- [PR #268](https://github.com/rqlite/rqlite/pull/268): Allow Store to wait for application of initial logs. Fixes [issue #260](https://github.com/rqlite/rqlite/issues/260).
- [PR #272](https://github.com/rqlite/rqlite/pull/272): Add commit, branch, GOOS, and GOARCH, to output of `--version`.
- [PR #274](https://github.com/rqlite/rqlite/pull/274): Use Hashicorp function to read peers. Thanks @WanliTian
- [PR #278](https://github.com/rqlite/rqlite/pull/278): Add support for dot-commands `.tables` and `.schema` to rqlite CLI. Fixes [issue #277](https://github.com/rqlite/rqlite/issues/277).

## 3.10.0 (January 29th 2017)
- [PR #261](https://github.com/rqlite/rqlite/pull/261): Allow Raft Apply timeout to be configurable.
- [PR #262](https://github.com/rqlite/rqlite/pull/262): Log GOOS and GOARCH at startup.

## 3.9.2 (January 14th 2017)
- [PR #253](https://github.com/rqlite/rqlite/pull/254): Handle nil row returned by SQL execer. Fixes [issue #253](https://github.com/rqlite/rqlite/issues/253).
- [PR #258](https://github.com/rqlite/rqlite/pull/258): Remove check that all queries begin with SELECT. Fixes [issue #255](https://github.com/rqlite/rqlite/issues/255).

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
**The Raft log format in 3.0 is not compatible with the 2.0 series**. To upgrade from an earlier version to this release you should backup your 2.0 leader node, and replay the database dump into a new 3.0 cluster. The HTTP API remains unchanged however.

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
- New Raft consensus module, built on Hashicorp's implementation.
- Hot backup support.
- Selectable read-consistency levels of none, weak, and strong.
- SQLite file size added to HTTP API status endpoint.
- expvar support added to HTTP server.

## 1.0 (June 23rd 2015)
Check out [this tag](https://github.com/rqlite/rqlite/releases/tag/v1.0) for full details.

