## 5.0.0 (unreleased)
_To upgrade from an earlier version to this release you should backup the leader node, and load the resultant database dump into a new 5.0 leader node. Check out [BACKUP.md](https://github.com/rqlite/rqlite/blob/master/DOC/BACKUPS.md) for more details._

### New features
- [PR #406](https://github.com/rqlite/rqlite/pull/406), [PR #408](https://github.com/rqlite/rqlite/pull/408): CLI supports Basic Auth credentials. Fixes [issue #369](https://github.com/rqlite/rqlite/issues/369). Thanks @joaodrp.
- [PR #414](https://github.com/rqlite/rqlite/pull/414), [PR #416](https://github.com/rqlite/rqlite/pull/416): Support display of query timings in CLI. Fixes [issue #317](https://github.com/rqlite/rqlite/issues/317). Thanks @joaodrp.
- [PR #436](https://github.com/rqlite/rqlite/pull/436): Add backup command to CLI. Fixes [issue #432](https://github.com/rqlite/rqlite/issues/432). Thanks @eariassoto.
- [PR #444](https://github.com/rqlite/rqlite/pull/444): Add basic [expvar](https://golang.org/pkg/expvar/) statistics to the Store.
- [PR #450](https://github.com/rqlite/rqlite/pull/450): Add restore command to CLI. Fixes [issue #439](https://github.com/rqlite/rqlite/issues/439). Thanks @eariassoto.
- [PR #451](https://github.com/rqlite/rqlite/pull/451), [PR #453](https://github.com/rqlite/rqlite/pull/453): Support dumping database in SQL text format via the API. Fixes [issue #393](https://github.com/rqlite/rqlite/issues/369).
- [PR #455](https://github.com/rqlite/rqlite/pull/455): Add dump-as-SQL-text command to CLI.
- [PR #462](https://github.com/rqlite/rqlite/pull/462): Add Raft metadata to responses to requests that modify Raft log.

### Design and implementation changes
- [End-to-end integration test](https://github.com/rqlite/rqlite/blob/master/system_test/full_system_test.py) added. Written in Python, it is automatically run by CircleCI as the committed source changes.
- [PR #377](https://github.com/rqlite/rqlite/pull/377): Upgrade consensus system to Hashicorp Raft v1.0.
- [PR #401](https://github.com/rqlite/rqlite/pull/401): Always try to close temporary database file after backup. Fixes [issue #400](https://github.com/rqlite/rqlite/issues/400). Thanks @sum12.
- [PR #411](https://github.com/rqlite/rqlite/pull/411), [PR #412](https://github.com/rqlite/rqlite/pull/412): Remove any pre-existing node with a given ID, if that node rejoins with a new IP address. Fixes [issue #409](https://github.com/rqlite/rqlite/issues/409).
- [PR #425](https://github.com/rqlite/rqlite/pull/425): By default use Raft network address as node ID. Fixes [issue #422](https://github.com/rqlite/rqlite/issues/422).
- [PR #430](https://github.com/rqlite/rqlite/pull/430): Close Raft log on Store close. Fixes [issue #429](https://github.com/rqlite/rqlite/issues/429).
- [PR #431](https://github.com/rqlite/rqlite/pull/431): Add function to Store that returns Raft leader ID.
- [PR #434](https://github.com/rqlite/rqlite/pull/434): Broadcast Cluster metadata via Raft consensus mechanism. Fixes [issue #138](https://github.com/rqlite/rqlite/issues/138).
- [PR #437](https://github.com/rqlite/rqlite/pull/437), [PR #438](https://github.com/rqlite/rqlite/pull/438): Make keys in diagnostic status output more consistent.
- [PR #448](https://github.com/rqlite/rqlite/pull/448): Support Store aborting transaction on any Execute error. Fixes [issue #385](https://github.com/rqlite/rqlite/issues/385).
- [PR #452](https://github.com/rqlite/rqlite/pull/452): Move to Go 1.10.
- [PR #457](https://github.com/rqlite/rqlite/pull/457): Implement new connection-oriented database layer.
- [PR #463](https://github.com/rqlite/rqlite/pull/463): Fix DSN key in Status output.
- [PR #461](https://github.com/rqlite/rqlite/pull/461): Write node backup directly to HTTP response writer. Thanks @sum12.
- [PR #471](https://github.com/rqlite/rqlite/pull/471): Cache most recent Raft log entries.
- [PR #466](https://github.com/rqlite/rqlite/pull/466): Port HTTP service to new connection-oriented design.
- [PR #486](https://github.com/rqlite/rqlite/pull/486): Store connections in Raft snapshot.
- [PR #491](https://github.com/rqlite/rqlite/pull/491): Add varadic utility functions to Auth credential store.

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
- New Raft consensus module, built on Hashsicorp's implementation.
- Hot backup support.
- Selectable read-consistency levels of none, weak, and strong.
- SQLite file size added to HTTP API status endpoint.
- expvar support added to HTTP server.

## 1.0 (June 23rd 2015)
Check out [this tag](https://github.com/rqlite/rqlite/releases/tag/v1.0) for full details.

