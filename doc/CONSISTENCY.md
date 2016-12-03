# Read Consistency

Even though serving queries does not require consensus (because the database is not changed), [queries should generally be served by the leader](https://github.com/rqlite/rqlite/issues/5). Why is this? Because, without this check, queries on a node could return results that are significantly out-of-date.  This could happen for one, or both, of the following two reasons:

 * The node, while still part of the cluster, has fallen behind the leader in terms of updates to its underlying database.
 * The node is no longer part of the cluster, and has stopped receiving Raft log updates.

This is why rqlite offers selectable read consistency levels of _none_, _weak_, and _strong_. Each is explained below.

With _none_, the node simply queries its local SQLite database, and does not even check if it is the leader. This offers the fastest query response, but suffers from the potential issues listed above. _Weak_ instructs the node to check that it is the leader, before querying the local SQLite file. Checking leader state only involves checking local state, so is still very fast. There is, however, a very small window of time (milliseconds by default) during which the node may return stale data. This is because after the leader check, but before the local SQLite database is read, another node could be elected leader and make changes to the cluster. As result the node may not be quite up-to-date with the rest of cluster.

To avoid even this last possibility, rqlite also offers _strong_. In this mode, rqlite sends the query through the Raft consensus system, ensuring that the node remains the leader at all times during query processing. However, this will involve the leader contacting at least a quorum of nodes, and will therefore increase query response times.

_Weak_ is probably sufficient for most applications, and is the default read consistency level. To explicitly select consistency, set the query param `level` to the desired level.

## Example queries
Examples of enabling each read consistency level for a simple query is shown below.

```bash
curl -G 'localhost:4001/db/query?level=none' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query?level=weak' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query' --data-urlencode 'q=SELECT * FROM foo' # Same as weak
curl -G 'localhost:4001/db/query?level=strong' --data-urlencode 'q=SELECT * FROM foo'
```
