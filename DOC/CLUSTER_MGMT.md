# Cluster Guidelines
This document describes, in detail, how to create and manage a rqlite cluster.

## Practical cluster size
Firstly, you should understand the basic requirement for systems built on the [Raft protocol](https://raft.github.io/). For a cluster of `N` nodes in size to remain operational, at least `(N/2)+1` nodes must be up and running, and be in contact with each other.  

Clusters of 3, 5, 7, or 9, nodes are most practical. Clusters of those sizes can tolerate failures of 1, 2, 3, and 4 nodes respectively.

Clusters with a greater number of nodes start to become unweildy, due to the number of nodes that must be contacted before a database change can take place.

### Read-only nodes
It is possible to run larger clusters if you just need nodes [from which you only need to read from](https://github.com/rqlite/rqlite/blob/master/DOC/READ_ONLY_NODES.md). When it comes to the Raft protocol, these nodes do not count towards `N`, since they do not [vote](https://raft.github.io/).

## Clusters with an even-number of nodes
There is little point running clusters with even numbers of voting i.e. Raft nodes. To see why this is imagine you have one cluster of 3 nodes, and a second cluster of 4 nodes. In each case, for the cluster to reach consensus on a given change, a majority of nodes within the cluster are required to have agreed to the change.

Specifically, a majority is defined as `(N/2)+1` where `N` is the number of nodes in the cluster. For a 3-node a majority is 2; for a 4-node cluster a majority is 3. Therefore a 3-node cluster can tolerate the failure of a single node. However a 4-node cluster can also only tolerate a failure of a single node.

So a 4-node cluster is no more fault-tolerant than a 3-node cluster, so running a 4-node cluster provides no advantage over a 3-node cluster. Only a 5-node cluster can tolerate the failure of 2 nodes. An analogous argument applies to 5-node vs. 6-node clusters, and so on.

# Creating a cluster
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
_If a node receives a join request, and that node is not actually the leader of the cluster, the receiving node will automatically redirect the requesting node to the leader node. As a result a node can actually join a cluster by contacting any node in the cluster. You can also specify multiple join addresses, and the node will try each address until joining is successful._

Once executed you now have a cluster of two nodes. Of course, for fault-tolerance you need a 3-node cluster, so launch a third node like so on _host3_:
```bash
host3:$ rqlited -node-id 3 -http-addr host3:4001 -raft-addr host3:4002 -join http://host1:4001 ~/node
```
_When simply restarting a node, there is no further need to pass `-join`. In fact, if a node is already a member of cluster (even if the cluster is just that node) `-join` is then ignored. Any join request will also be ignored by the leader if the joining node is already a member of the cluster, and it will be treated as a simple restart by the node. This can be convenient because it means join requests are idempotent, allowing you to leave the initial command-line parameters unchanged, including any `join` options._

You've now got a fault-tolerant, distributed, relational database. It can tolerate the failure of any node, even the leader, and remain operational.

## Node IDs
You can set the Node ID (`-node-id`) to anything you wish, as long as it's unique for each node.

## Listening on all interfaces
You can pass `0.0.0.0` to both `-http-addr` and `-raft-addr` if you wish a node to listen on all interfaces. You must still pass an explicit network address to `-join` however. In this case you'll also want to set `-http-adv-addr` and `-raft-adv-addr` to the actual interface addresses, so other nodes learn the correct network address to use to reach the node listening on `0.0.0.0`.

## Discovery Service
There is also a rqlite _Discovery Service_, allowing nodes to automatically connect and form a cluster. This can be much more convenient, allowing clusters to be dynamically created. Check out [the documentation](https://github.com/rqlite/rqlite/blob/master/DOC/DISCOVERY.md) for more details.

## Through the firewall
On some networks, like AWS EC2 cloud, nodes may have an IP address that is not routable from outside the firewall. Instead these nodes are addressed using a different IP address. You can still form a rqlite cluster however -- check out [this tutorial](http://www.philipotoole.com/rqlite-v3-0-1-globally-replicating-sqlite/) for an example. The key thing is that you must set `-http-adv-addr` and `-raft-adv-addr` so a routable address is broadcast to ther nodes.

# Dealing with failure
It is the nature of clustered systems that nodes can fail at anytime. Depending on the size of your cluster, it will tolerate various amounts of failure. With a 3-node cluster, it can tolerate the failure of a single node, including the leader.

If an rqlite process crashes, it is safe to simply to restart it. The node will pick up any changes that happened on the cluster while it was down.

# Growing a cluster
You can grow a cluster, at anytime, simply by starting up a new node (pick a never before used node ID) and having it explicitly join with the leader as normal, or by passing it a discovery service ID. The new node will automatically pick up changes that have occurred on the cluster since the cluster first started.

# Modifying a node's network addresses
It is possible to change a node's HTTP(S) address or Raft address between restarts. Simply pass the new address on the command line. You must also, however, explicitly tell the node to join the cluster again, by passing `-join` to the node. In this case what the leader actually does is remove the previous record of the node, before adding a new record of the node.

# Removing or replacing a node
If a node fails completely and is not coming back, or if you shut down a node because you wish to deprovision it, its record should also be removed from the cluster. To remove the record of a node from a cluster, execute the following command at the rqlite CLI:

```
127.0.0.1:4001> .remove <node raft ID>
```

You can also make a direct call to the HTTP API to remove a node:

```
curl -XDELETE http://localhost:4001/remove -d '{"id": "<node raft ID>"}'
```
assuming `localhost` is the address of the cluster leader. If you do not do this the leader will continually attempt to communicate with that node. Note that the cluster must be functional -- there must still be an operational leader -- for this removal to be successful. If, after a node failure, a given cluster does not have a quorum of nodes still running, you must bring back the failed node. Any attempt to remove it will fail as there will be no leader to respond to the failure request

## Examples
_Quorum is defined as (N/2)+1 where N is the size of the cluster._

### 2-node cluster 
Quorum of a 2-node cluster is 2.

If 1 node fails, quorum can no longer reached. The failing node must be recovered, as the failed node cannot be removed, and a new node cannot be added to the cluster to takes its place. This is why you shouldn't run 2-node clusters, except for testing purposes. In general it doesn't make much sense to run clusters with even-number of nodes at all.

If you remove a single node from a fully-functional 2-node cluster, quorum will be reduced to 1 since you will be left with a 1-node cluster.

### 3-node cluster
Quorum of a 3-node cluster is 2.

If 1 node fails, the cluster can still reach quorum. Remove the failing node, or restart it. If you remove the node, quorum remains at 2. You should add a new node to get the cluster back to 3 nodes in size. If 2 nodes fail, the cluster will not be able to reach quorum. You must instead restart at least one of the nodes.

If you remove a single node from a fully-functional 3-node cluster, quorum will be unchanged since you now have a 2-node cluster.

### 4-node cluster
Quorum of a 4-node cluster is 3.

The situation is similar for a 3-node cluster, in the sense that it can only tolerate the failure of a single node. If you remove a single node from a fully-functional 4-node cluster, quorum will decrease to 2 you now have a 3-node cluster.

### 5-node cluster
Quorum of a 5-node cluster is 3.

With a 5-node cluster, the cluster can tolerate the failure of 2 nodes. However if 3 nodes fail, at least one of those nodes must be restarted before you can make any change. If you remove a single node from a fully-functional 5-node cluster, quorum will be unchanged since you now have a 4-node cluster.
