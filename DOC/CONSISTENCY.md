# Read Consistency

Even though serving queries does not require consensus (because the database is not changed), [queries should generally be served by the leader](https://github.com/rqlite/rqlite/issues/5). Why is this? Because, without this check, queries on a node could return results that are out-of-date i.e. _stale_.  This could happen for one, or both, of the following two reasons:

 * The node, while still part of the cluster, has fallen behind the leader in terms of updates to its underlying database.
 * The node is no longer part of the cluster, and has stopped receiving Raft log updates.

This is why rqlite offers selectable read consistency levels of _none_, _weak_, and _strong_. Each is explained below.

## None
With _none_, the node simply queries its local SQLite database, and does not even check if it is the leader. This offers the fastest query response, but suffers from the potential issues listed above.

### Limiting read staleness
You can tell the node not return results (effectively) older than a certain time, however. If a read request sets the query parameter `freshness` to a [Go duration string](https://golang.org/pkg/time/#Duration), the node serving the read will check that less time has passed since it was last in contact with the leader, than that specified via freshness. If more time has passed the node will return an error. `freshness` is ignored for all consistency levels except `none`, and is also ignored if set to zero.

If you decide to deploy [read-only nodes](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md) however, _none_ combined with `freshness` can be a particularly effective at adding read scalability to your system. You can use lots of read-only nodes, yet be sure that a given node serving a request has not fallen too far behind the leader (or even become disconnected from the cluster).

## Weak
_Weak_ instructs the node to check that it is the leader, before querying the local SQLite file. Checking leader state only involves checking local state, so is still very fast. There is, however, a very small window of time (milliseconds by default) during which the node may return stale data. This is because after the leader check, but before the local SQLite database is read, another node could be elected leader and make changes to the cluster. As result the node may not be quite up-to-date with the rest of cluster.

## Strong
To avoid even the issues associated with _weak_ consistency, rqlite also offers _strong_. In this mode, rqlite sends the query through the Raft consensus system, ensuring that the node remains the leader at all times during query processing. However, this will involve the leader contacting at least a quorum of nodes, and will therefore increase query response times.

# Which should I use?
_Weak_ is probably sufficient for most applications, and is the default read consistency level. To explicitly select consistency, set the query param `level` to the desired level. However you should use _none_ with read-only nodes, unless you want those nodes to actually redirect the query to the leader.

## Example queries
Examples of enabling each read consistency level for a simple query is shown below.

```bash
curl -G 'localhost:4001/db/query?level=none' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query?level=none&freshness=1s' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query?level=weak' --data-urlencode 'q=SELECT * FROM foo'
curl -G 'localhost:4001/db/query' --data-urlencode 'q=SELECT * FROM foo' # Same as weak
curl -G 'localhost:4001/db/query?level=strong' --data-urlencode 'q=SELECT * FROM foo'
```
