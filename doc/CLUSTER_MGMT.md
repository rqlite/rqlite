# Cluster Guidelines
This document describes, in detail, how to create and manage a rqlite cluster.

## Practical cluster size
Clusters of 3, 5, 7, or 9, nodes are most practical. Clusters with a greater number start to become unweildy, due to the number of nodes that must be contacted before a database change can take place.

## Clusters with an even-number of nodes
There is little point running clusters with even numbers of nodes. To see why this is imagine you have one cluster of 3 nodes, and a second cluster of 4 nodes. In each case, for the cluster to reach consensus on a given change, a majority of nodes within the cluster are required to have agreed to the change.

A majority is defined as `N/2 + 1` where `N` is the number of nodes in the cluster. For a 3-node a majority is 2; for a 4-node cluster a majority is 3. Therefore a 3-node cluster can tolerate the failure of a single node. However a 4-node cluster can also only tolerate a failure of a single node.

So a 4-node cluster is no more fault-tolerant than a 3-node cluster, so running a 4-node cluster provides no advantage over a 3-node cluster. Only a 5-node cluster can tolerate the failure of 2 nodes. An analogous argument applies to 5-node vs. 6-node clusters, and so on.

# Creating a cluster
Let's say you have 3 host machines, _host1_, _host2_, and _host3_, and that each of these hostnames resolves to an IP address reachable from the other hosts. Instead of hostnames you can use the IP address directly if you wish.

To create a cluster you must first launch a node that can act as the initial leader. Do this as follows on _host1_:
```bash
host1:$ rqlited -http host1:4001 -raft host1:4002 ~/node
```
With this command a single node is started, listening for API requests on port 4001 and listening on port 4002 for intra-cluster communication and cluster-join requests from other nodes. This node stores its state at `~/node`.

To join a second node to this leader, execute the following command on _host2_:
```bash
host2:$ rqlited -http host2:4001 -raft host2:4002 -join http://host1:4001 ~/node
```
_If a node receives a join request, and that node is not actually the leader of the cluster, the receiving node will automatically redirect the requesting node to the leader node. As a result a node can actually join a cluster by contacting any node in the cluster._

Once executed you now have a cluster of two nodes. Of course, for fault-tolerance you need a 3-node cluster, so launch a third node like so on _host3_:
```bash
host3:$ rqlited -http host3:4001 -raft host3:4002  -join http://host1:4001 ~/node
```
_When restarting a node, there is no further need to pass `-join`. It will be ignored if a node is already a member of a cluster._

You've now got a fault-tolerant, distributed, relational database. It can tolerate the failure of any node, even the leader, and remain operational.

## Through the firewall
On some networks, like AWS EC2 cloud, nodes may have an IP address that is not routable from outside the firewall. Instead these nodes are addressed using a different IP address. You can still form a rqlite cluster however -- check out [this tutorial](http://www.philipotoole.com/rqlite-v3-0-1-globally-replicating-sqlite/) for more details.

# Dealing with failure
It is the nature of clustered systems, nodes can fail at anytime. Depending on the size of your cluster, it will tolerate various amounts of failure. With a 3-node cluster, it can tolerate the failure of a single node, including the leader.

If an rqlite process crashes, it is safe to simply to restart it. The node will pick up any changes that happened on the cluster while it was down.

# Growing a cluster
You can grow a cluster simply by joining a new node to an existing cluster, via requests to the leader, at anytime. The new node will pick up changes that have occurred on the cluster since the cluster first started.

# Replacing a node
If a node fails completely and is not coming back -- a complete loss -- then it must be first removed from the cluster. To remove a node from a cluster, execute the following command:
```
curl -XDELETE http://localhost:4001/remove -d '{"addr": "<node raft address>"}'
```
assuming `localhost` is the address of the cluster leader. Removing a node does not change the number of nodes required to reach quorum, so you must add a new node to cluster. To do so, simply follow the instructions for _Growing a cluster_.
