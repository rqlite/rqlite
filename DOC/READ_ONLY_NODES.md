# Read-only nodes
rqlite supports addingh _read-only_ nodes. You can use this feature to add read scalability to the cluster if you need a high volume of reads, without adding write latency to the cluster.

What this means is that a read-only node doesn't participate in the Raft consensus system i.e. it doesn't contribute towards quorum, nor does it cast a vote during the leader election process. However it does subscribe to the stream of committed log entries, which are broadcast by the leader. It updates its own copy of the SQLite database, just like any other node in the cluster.

## Querying a read-only node
Any read request to a read-only node must specify [read-consistency](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md) level `none`. If any other consistency level is specified (or none at all) the read-only node will redirect the request to the leader.

## Enabling read-only mode
Pass `-raft-non-voter=true` to `rqlited` to enable read-only mode.