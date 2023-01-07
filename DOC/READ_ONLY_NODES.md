# Read-only nodes
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

rqlite supports adding _read-only_ nodes. You can use this feature to add read scalability to the cluster if you need a high volume of reads, or want to distribute copies of the data nearer to clients -- but don't want those nodes counted towards the quorum. These types of nodes are also known as _non-voting_ nodes.

What this means is that a read-only node doesn't participate in the Raft consensus system i.e. it doesn't contribute towards quorum, nor does it cast a vote during the leader election process. Just like voting nodes, however, read-only nodes still subscribe to the stream of committed log entries broadcast by the Leader, and update the SQLite database using the log entries they receive from the Leader.

## Querying a read-only node
Any read request to a read-only node must specify [read-consistency](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md) level `none`. If any other consistency level is specified (or no level is explicitly specified) the read-only node will redirect the request to the leader. 

To ensure a read-only node hasn't become completely disconnected from the cluster, you will probably want to set the [`freshness` query parameter](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md#limiting-read-staleness).

## Enabling read-only mode
Pass `-raft-non-voter=true` to `rqlited` to enable read-only mode.

## Read-only node management
Read-only nodes join a cluster in the [same manner as a voting node. They can also be removed using the same operations](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md).

### Handling failure
If a read-only node becomes unreachable, the leader will continually attempt to reconnect until the node becomes reachable again, or the node is removed from the cluster. This is exactly the same behaviour as when a voting node fails. However, since read-only nodes do not vote, a failed read-only node will not prevent the cluster commiting changes via the Raft consensus mechanism.
