# Contents
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

* [General guidelines](#general-guidelines)
* [Creating a cluster](#creating-a-cluster)
* [Growing a cluster](#growing-a-cluster)
* [Modifying a node's Raft network addresses](#modifying-a-nodes-raft-network-addresses)
* [Removing or replacing a node](#removing-or-replacing-a-node)
* [Dealing with failure](#dealing-with-failure)

# General guidelines
This document describes, in detail, how to create and manage a rqlite cluster.

## Practical cluster size
Firstly, you should understand the basic requirement for systems built on the [Raft protocol](https://raft.github.io/). For a cluster of `N` nodes in size to remain operational, at least `(N/2)+1` nodes must be up and running, and be in contact with each other. For a single-node system (N=1) then (obviously) that single node must be running. For a 3-node cluster (N=3) at least 2 nodes must be running. For N=5, at least 3 nodes should be running, and so on.

Clusters of 3, 5, 7, or 9, nodes are most practical. Clusters of those sizes can tolerate failures of 1, 2, 3, and 4 nodes respectively.

Clusters with a greater number of nodes start to become unwieldy, due to the number of nodes that must be contacted before a database change can take place. There is no intrinsic limit to the number of nodes comprising a cluster, but the operational overload can increase with little benefit.

### Read-only nodes
It is possible to run larger clusters if you just need nodes [from which you only need to read from](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md). When it comes to the Raft protocol, these nodes do not count towards `N`, since they do not [vote](https://raft.github.io/).

## Clusters with an even-number of nodes
There is little point running clusters with even numbers of voting i.e. Raft nodes. To see why this is imagine you have one cluster of 3 nodes, and a second cluster of 4 nodes. In each case, for the cluster to reach consensus on a given change, a majority of nodes within the cluster are required to have agreed to the change.

Specifically, a majority is defined as `(N/2)+1` where `N` is the number of nodes in the cluster. For a 3-node a majority is 2; for a 4-node cluster a majority is 3. Therefore a 3-node cluster can tolerate the failure of a single node. However a 4-node cluster can also only tolerate a failure of a single node.

So a 4-node cluster is no more fault-tolerant than a 3-node cluster, so running a 4-node cluster provides no advantage over a 3-node cluster. Only a 5-node cluster can tolerate the failure of 2 nodes. An analogous argument applies to 5-node vs. 6-node clusters, and so on.

# Creating a cluster
_This section describes manually creating a cluster. If you wish rqlite nodes to automatically find other, and form a cluster, check out [auto-clustering](https://github.com/rqlite/rqlite/blob/master/DOC/AUTO_CLUSTERING.md)._

Let's say you have 3 host machines, _host1_, _host2_, and _host3_, and that each of these hostnames resolves to an IP address reachable from the other hosts. Instead of hostnames you can use the IP address directly if you wish.

To create a cluster you must first launch a node that can act as the initial leader. Do this as follows on _host1_:
```bash
host1:$ rqlited -node-id 1 -http-addr host1:4001 -raft-addr host1:4002 ~/node
```
With this command a single node is started, listening for API requests on port 4001 and listening on port 4002 for intra-cluster communication and cluster-join requests from other nodes. Note that the addresses passed to `-http-addr` and `-raft-addr` must be reachable from other nodes so that nodes can find each other over the network -- these addresses will be broadcast to other nodes during the _Join_ operation. If a node needs to bind to one address, but broadcast a different address, you must set `-http-adv-addr` and `-raft-adv-addr`.

`-node-id` can be any string, as long as it's unique for the cluster. It also shouldn't change, once chosen for this node. The network addresses can change however. This node stores its state at `~/node`.

To join a second node to this leader, execute the following command on _host2_:
```bash
host2:$ rqlited -node-id 2 -http-addr host2:4001 -raft-addr host2:4002 -join http://host1:4001 ~/node
```
_If a node receives a join request, and that node is not actually the leader of the cluster, the receiving node will automatically redirect the requesting node to the Leader node. As a result a node can actually join a cluster by contacting any node in the cluster. You can also specify multiple join addresses, and the node will try each address until joining is successful._

Once executed you now have a cluster of two nodes. Of course, for fault-tolerance you need a 3-node cluster, so launch a third node like so on _host3_:
```bash
host3:$ rqlited -node-id 3 -http-addr host3:4001 -raft-addr host3:4002 -join http://host1:4001 ~/node
```
_When simply restarting a node, there is no further need to pass `-join`. However, if a node does attempt to join a cluster it is already a member of, and neither its node ID or Raft network address has changed, then the cluster Leader will ignore the join request as there is nothing to do -- the joining node is already a fully-configured member of the cluster. However, if either the node ID or Raft network address of the joining node has changed, the cluster Leader will first automatically remove the joining node from the cluster configuration before processing the join request. For most applications this is an implementation detail which can be safely ignored, and cluster-joins are basically idempotent._

You've now got a fault-tolerant, distributed, relational database. It can tolerate the failure of any node, even the leader, and remain operational.

## Node IDs
You can set the Node ID (`-node-id`) to anything you wish, as long as it's unique for each node.

## Listening on all interfaces
You can pass `0.0.0.0` to both `-http-addr` and `-raft-addr` if you wish a node to listen on all interfaces. You must still pass an explicit network address to `-join` however. In this case you'll also want to set `-http-adv-addr` and `-raft-adv-addr` to the actual interface addresses, so other nodes learn the correct network address to use to reach the node listening on `0.0.0.0`.

## Through the firewall
On some networks, like AWS EC2 cloud, nodes may have an IP address that is not routable from outside the firewall. Instead these nodes are addressed using a different IP address. You can still form a rqlite cluster however -- check out [this tutorial](https://www.philipotoole.com/rqlite-v3-0-1-globally-replicating-sqlite/) for an example. The key thing is that you must set `-http-adv-addr` and `-raft-adv-addr` so a routable address is broadcast to other nodes.

# Growing a cluster
You can grow a cluster, at anytime, simply by starting up a new node (pick a never before used node ID) and having it explicitly join with the leader as normal. The new node will automatically pick up all changes that have occurred on the cluster since the cluster first started. In otherwords, after joining successfully, the new node will have a full copy of the SQLite database, just like every other node in the cluster.

# Modifying a node's Raft network addresses
It is possible to change a node's Raft address between restarts. Simply pass the new address on the command line. **You must also, however, explicitly tell the node to join the cluster again, by passing `-join` to the node**. In this case what the leader actually does is remove the previous record of the node, before adding a new record of the node. You can also change the HTTP API address of a node between restarts, but an explicit re-join is not required if just the HTTP API address changes.

**Note that this process only works if your cluster has, in addition to the node with the changing address, a quorum (at least) of nodes up running**. If your cluster does not meet this requirement, see the section titled _Dealing with failure_.

# Removing or replacing a node
If a node fails completely and is not coming back, or if you shut down a node because you wish to deprovision it, its record should also be removed from the cluster. To remove the record of a node from a cluster, execute the following command at the rqlite CLI:

```
127.0.0.1:4001> .remove <node raft ID>
```

You can also make a direct call to the HTTP API to remove a node:

```
curl -XDELETE http://host:4001/remove -d '{"id": "<node raft ID>"}'
```
where `host` is any node in the cluster. If you do not remove a failed node the lLader will continually attempt to communicate with that node. **Note that the cluster must be functional -- there must still be an operational Leader -- for this removal to be successful**. If, after a node failure, a given cluster does not have a quorum of nodes still running, you must bring back the failed node. Any attempt to remove it will fail as there will be no Leader to respond to the node-removal request.

If you cannot bring sufficient nodes back online such that the cluster can elect a leader, follow the instructions in the section titled _Dealing with failure_.

## Automatically removing failed nodes
> :warning: **This functionality was introduced in version 7.11.0. It does not exist in earlier releases.**

rqlite supports automatically removing both voting (the default type) and non-voting (read-only) nodes that have been non-reachable for a configurable period of time. A non-reachable node is defined as a node that the Leader cannot heartbeat with. To enable reaping of voting nodes set `-raft-reap-node-timeout` to a non-zero time interval. Likewise, to enable reaping of non-voting (read-only) nodes set `-raft-reap-read-only-node-timeout`.

It is recommended that these values be set conservatively, especially for voting nodes. Setting them too low may mean you don't account for the normal kinds of network outages and tempoary failures that can affect distributed systems such as rqlite. Note that the timeout clock is reset if a cluster elects a new Leader.

### Example configuration
Instruct rqlite to reap non-reachable voting nodes after 2 days, and non-reachable read-only nodes after 30 minutes:
```bash
rqlited -node-id 1 -raft-reap-node-timeout=48h -raft-reap-read-only-node-timeout=30m data
```
For reaping to work consistently you **must** set these flags on **every** voting node in the cluster -- in otherwords, every node that could potentially become the Leader. You can also set the flags on read-only nodes, but they will simply be silently ignored.

# Dealing with failure
It is the nature of clustered systems that nodes can fail at anytime. Depending on the size of your cluster, it will tolerate various amounts of failure. With a 3-node cluster, it can tolerate the failure of a single node, including the leader.

If an rqlite process crashes, it is safe to simply to restart it. The node will pick up any changes that happened on the cluster while it was down.

## Recovering a cluster that has permanently lost quorum
_This section borrows heavily from the Consul documentation._

In the event that multiple rqlite nodes are lost, causing a loss of quorum and a complete outage, partial recovery is possible using data on the remaining nodes in the cluster. There may be data loss in this situation because multiple servers were lost, so information about what's committed could be incomplete. The recovery process implicitly commits all outstanding Raft log entries, so it's also possible to commit data -- and therefore change the SQLite database -- that was uncommitted before the failure.

**You may also need to follow the recovery process if a cluster simply restarts, but all nodes (or a quorum of nodes) come up with different network identitiers. This can happen in certain deployment configurations.**

To begin, stop all remaining nodes. You can attempt a graceful node-removal, but it will not work in most cases. Do not worry if the remove operation results in an error. The cluster is in an unhealthy state, so this is expected.

The next step is to go to the _data_ directory of each rqlite node you wish to bring back up. Inside that directory, there will be a `raft/` sub-directory. You need to create a `peers.json` file within that directory, which will contain the desired configuration of your recovered rqlite cluster (which may be smaller than the original cluster, perhaps even just a single recovered node). This file should be formatted as a JSON array containing the node ID, `address:port`, and suffrage information of each rqlite node in the cluster.

Below is an example, of bringing a 3-node cluster back online. 

```json
[
  {
    "id": "1",
    "address": "10.1.0.1:8300",
    "non_voter": false
  },
  {
    "id": "2",
    "address": "10.1.0.2:8300",
    "non_voter": false
  },
  {
    "id": "3",
    "address": "10.1.0.3:8300",
    "non_voter": false
  }
]
```

`id` specifies the node ID of the server, which must not be changed from its previous value. The ID for a given node can be found in the logs when the node starts up if it was auto-generated. `address` specifies the desired Raft IP and port for the node, which does not need to be the same as previously. You can use hostnames instead of IP addresses if you prefer. `non_voter` controls whether the server is a read-only node. If omitted, it will default to false, which is typical for most rqlite nodes.

Next simply create entries for all the nodes you plan to bring up (in the example above that's 3 nodes). You must confirm that nodes you don't include here have indeed failed and will not later rejoin the cluster. Ensure that this file is the same across all remaining rqlite nodes. At this point, you can restart your rqlite cluster. In the example above, this means you'd start 3 nodes.

Once recovery is completed, the `peers.json` file is renamed to `peers.info`. `peers.info` will not trigger further recoveries, and simply acts as a record for future reference. It may be deleted at anytime.

# Example Cluster Sizes
_Quorum is defined as (N/2)+1 where N is the size of the cluster._

## 2-node cluster
Quorum of a 2-node cluster is 2.

If 1 node fails, quorum can no longer reached. The failing node must be recovered, as the failed node cannot be removed, and a new node cannot be added to the cluster to takes its place. This is why you shouldn't run 2-node clusters, except for testing purposes. In general it doesn't make much sense to run clusters with even-number of nodes at all.

If you remove a single node from a fully-functional 2-node cluster, quorum will be reduced to 1 since you will be left with a 1-node cluster.

## 3-node cluster
Quorum of a 3-node cluster is 2.

If 1 node fails, the cluster can still reach quorum. Remove the failing node, or restart it. If you remove the node, quorum remains at 2. You should add a new node to get the cluster back to 3 nodes in size. If 2 nodes fail, the cluster will not be able to reach quorum. You must instead restart at least one of the nodes.

If you remove a single node from a fully-functional 3-node cluster, quorum will be unchanged since you now have a 2-node cluster.

## 4-node cluster
Quorum of a 4-node cluster is 3.

The situation is similar for a 3-node cluster, in the sense that it can only tolerate the failure of a single node. If you remove a single node from a fully-functional 4-node cluster, quorum will decrease to 2 you now have a 3-node cluster.

## 5-node cluster
Quorum of a 5-node cluster is 3.

With a 5-node cluster, the cluster can tolerate the failure of 2 nodes. However if 3 nodes fail, at least one of those nodes must be restarted before you can make any change. If you remove a single node from a fully-functional 5-node cluster, quorum will be unchanged since you now have a 4-node cluster.
