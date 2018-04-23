# Cluster Guidelines
This document describes, in detail, how to create and manage a rqlite cluster.

## Practical cluster size
Clusters of 3, 5, 7, or 9, nodes are most practical. Clusters with a greater number start to become unweildy, due to the number of nodes that must be contacted before a database change can take place.

## Clusters with an even-number of nodes
There is little point running clusters with even numbers of nodes. To see why this is imagine you have one cluster of 3 nodes, and a second cluster of 4 nodes. In each case, for the cluster to reach consensus on a given change, a majority of nodes within the cluster are required to have agreed to the change.

A majority is defined as `N/2 + 1` where `N` is the number of nodes in the cluster. For a 3-node a majority is 2; for a 4-node cluster a majority is 3. Therefore a 3-node cluster can tolerate the failure of a single node. However a 4-node cluster can also only tolerate a failure of a single node.

So a 4-node cluster is no more fault-tolerant than a 3-node cluster, so running a 4-node cluster provides no advantage over a 3-node cluster. Only a 5-node cluster can tolerate the failure of 2 nodes. An analogous argument applies to 5-node vs. 6-node clusters, and so on.

# Creating a cluster
_Do not pass `-node-id` to release v4 and earlier, as it is not supported by those versions._

Let's say you have 3 host machines, _host1_, _host2_, and _host3_, and that each of these hostnames resolves to an IP address reachable from the other hosts. Instead of hostnames you can use the IP address directly if you wish.

To create a cluster you must first launch a node that can act as the initial leader. Do this as follows on _host1_:
```bash
host1:$ rqlited -node-id 1 -http-addr host1:4001 -raft-addr host1:4002 ~/node
```
With this command a single node is started, listening for API requests on port 4001 and listening on port 4002 for intra-cluster communication and cluster-join requests from other nodes. This node stores its state at `~/node`.

To join a second node to this leader, execute the following command on _host2_:
```bash
host2:$ rqlited -node-id 2 -http-addr host2:4001 -raft-addr host2:4002 -join http://host1:4001 ~/node
```
_If a node receives a join request, and that node is not actually the leader of the cluster, the receiving node will automatically redirect the requesting node to the leader node. As a result a node can actually join a cluster by contacting any node in the cluster. You can also specify multiple join addresses, and the node will try each address until joining is successful._

Once executed you now have a cluster of two nodes. Of course, for fault-tolerance you need a 3-node cluster, so launch a third node like so on _host3_:
```bash
host3:$ rqlited -node-id 3 -http-addr host3:4001 -raft-addr host3:4002 -join http://host1:4001 ~/node
```
_When restarting a node, there is no further need to pass `-join`. It will be ignored if a node is already a member of a cluster._

You've now got a fault-tolerant, distributed, relational database. It can tolerate the failure of any node, even the leader, and remain operational.

## Node IDs
Setting the Node ID (`-node-id`) is optional, and can be set to anything you wish, as long as it's unique for each node. If you do not set it, it will be set to the advertised Raft network address.

## Node network addresses
A node ID is what uniquely identifies a node. In contrast a node's network addresses can change between restarts, and an rqlite cluster will automatically update its configuration if a node rejoins a cluster with changed network addresses. If the nodes that comprise your cluster might change network addresses between restarts, explicitly setting node IDs is strongly recommended.

Note that if a node's network addresses change on a restart, that node **must explicitly rejoin the cluster** when it starts. Rejoining the cluster is how a node correctly informs the cluster that it has new network addresses. This is not difficult to do in practise -- simply tell a node to join the cluster *every time* it restarts by setting `-join`. A node can safely join a cluster of which it is already a member, even if its network adddresses have not changed.

## Listening on all interfaces
You can pass `0.0.0.0` to both `-http-addr` and `-raft-addr` if you wish a node to listen on all interfaces. You must still pass an explicit network address to `-join` however. In this case you'll also want to set `-http-adv-addr` to the actual interface address, so other nodes learn the correct network address to use to reach the node listening on `0.0.0.0`.

## Discovery Service
There is also a rqlite _Discovery Service_, allowing nodes to automatically connect and form a cluster. This can be much more convenient, allowing clusters to be dynamically created. Check out [the documentation](https://github.com/rqlite/rqlite/blob/master/DOC/DISCOVERY.md) for more details.

## Through the firewall
On some networks, like AWS EC2 cloud, nodes may have an IP address that is not routable from outside the firewall. Instead these nodes are addressed using a different IP address. You can still form a rqlite cluster however -- check out [this tutorial](http://www.philipotoole.com/rqlite-v3-0-1-globally-replicating-sqlite/) for more details.

# Dealing with failure
It is the nature of clustered systems that nodes can fail at anytime. Depending on the size of your cluster, it will tolerate various amounts of failure. With a 3-node cluster, it can tolerate the failure of a single node, including the leader.

If an rqlite process crashes, it is safe to simply to restart it. The node will pick up any changes that happened on the cluster while it was down.

# Growing a cluster
You can grow a cluster, at anytime, simply by starting up a new node and having it explicitly join with the leader as normal, or by passing it a discovery service ID. The new node will automatically pick up changes that have occurred on the cluster since the cluster first started.

# Removing or replacing a node
If a node fails completely and is not coming back, or if you shut down a node because you wish to deprovision it, its record should also be removed from the cluster. To remove the record of a node from a cluster, execute the following command:
```
curl -XDELETE http://localhost:4001/remove -d '{"id": "<node raft ID>"}'
```
assuming `localhost` is the address of the cluster leader. If you do not do this the leader will continually attempt to communicate with that node.

Removing a node does not change the number of nodes required to reach quorum, so you must add a new node to cluster as a replacement. To do so, simply follow the instructions for _Growing a cluster_.
