# Read Consistency
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

_rqlite has been run through Jepsen-style testing. You can read about it [here](https://github.com/wildarch/jepsen.rqlite/blob/main/doc/blog.md)._

Even though serving queries does not require Raft consensus (because the database is not changed), [queries should generally be served by the Leader](https://github.com/rqlite/rqlite/issues/5). Why is this? Because, without this check, queries on a node could return results that are out-of-date i.e. _stale_.  This could happen for one, or both, of the following two reasons:

 * The node, while still part of the cluster, has fallen behind the Leader in terms of updates to its underlying database.
 * The node is no longer part of the cluster, and has stopped receiving Raft log updates.

This is why rqlite offers selectable read consistency levels of _none_, _weak_, and _strong_. Each is explained below, and examples of each are shown at the end of this document.

## None
With _none_, the node simply queries its local SQLite database, and does not care if it's a Leader or Follower. This offers the fastest query response, but suffers from the potential issues listed above.

### Limiting read staleness
You can tell the node not to return results (effectively) older than a certain time, however. If a read request sets the query parameter `freshness` to a [Go duration string](https://golang.org/pkg/time/#Duration), the node serving the read will check that less time has passed since it was last in contact with the Leader, than that specified via freshness. If more time has passed the node will return an error. `freshness` is ignored for all consistency levels except `none`, and is also ignored if set to zero.

> :warning: **The `freshness` parameter is always ignored if the node serving the query is the Leader**. Any read, when served by the leader, is always going to be within any possible freshness bound.

If you decide to deploy [read-only nodes](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md) however, _none_ combined with `freshness` can be a particularly effective at adding read scalability to your system. You can use lots of read-only nodes, yet be sure that a given node serving a request has not fallen too far behind the Leader (or even become disconnected from the cluster).

## Weak
If a query request is sent to a follower, and _weak_ consistency is specified, the Follower will transparently forward the request to the Leader. The Follower waits for the response from the Leader, and then returns that response to the client.

_Weak_ instructs the Leader to check that it is the Leader, before querying the local SQLite file. Checking Leader state only involves checking state local to the Leader, so is still very fast. There is, however, a very small window of time (milliseconds by default) during which the node may return stale data. This is because after the local Leader check, but before the local SQLite database is read, another node could be elected Leader and make changes to the cluster. As result the node may not be quite up-to-date with the rest of cluster.

## Strong
If a query request is sent to a follower, and _strong_ consistency is specified, the Follower will transparently forward the request to the Leader. The Follower waits for the response from the Leader, and then returns that response to the client.

To avoid even the issues associated with _weak_ consistency, rqlite also offers _strong_. In this mode, the Leader sends the query through the Raft consensus system, ensuring that the Leader **remains** the Leader at all times during query processing. When using _strong_ you can be sure that the database reflects every change sent to it prior to the query. However, this will involve the Leader contacting at least a quorum of nodes, and will therefore increase query response times.

# Which should I use?
_Weak_ is probably sufficient for most applications, and is the default read consistency level. Unless the leader on your cluster is continually changing there will be no difference between _weak_ and _strong_ -- but using _strong_ will result in more Raft traffic, which is not what most people want.

To explicitly select consistency, set the query param `level` to the desired level. However, you should use _none_ with read-only nodes, unless you want those nodes to actually forward the query to the Leader.

## Example queries
Examples of enabling each read consistency level for a simple query is shown below.

```bash
# Query the node, telling it simply to read the SQLite database directly. No guarantees on how old the data is.
# In fact the node may not even be connected to the cluster.
curl -G 'localhost:4001/db/query?level=none' --data-urlencode 'q=SELECT * FROM foo'

# Query the node, telling it simply to read the SQLite database directly. The read request will be successful
# only if the node last heard from the leader no more than 1 second ago.
curl -G 'localhost:4001/db/query?level=none&freshness=1s' --data-urlencode 'q=SELECT * FROM foo'

# Default query options. The read request will be successful only if the node believes its the leader. 
curl -G 'localhost:4001/db/query?level=weak' --data-urlencode 'q=SELECT * FROM foo'

# Default query options. The read request will be successful only if the node believes it is the leader. Same as weak.
curl -G 'localhost:4001/db/query' --data-urlencode 'q=SELECT * FROM foo'

# The read request will be successful only if the node maintained cluster leadership during
# the entirety of query processing.
curl -G 'localhost:4001/db/query?level=strong' --data-urlencode 'q=SELECT * FROM foo'
```
