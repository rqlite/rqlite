# Cluster Guidelines
This document describes, in detail, how to create and manage a rqlite cluster.

## Practical cluster size
Clusters of 3, 5, 7, or 9, nodes are most practical. Clusters with a greater number become unweildy.

## Clusters with an even-number of nodes
There is no point running clusters with even numbers of nodes. To see why this is imagine you have one cluster of 3 nodes, and a second of 4 nodes. In both cases a majority of nodes are required for the cluster to reach consensus. A majority is defined as `N/2 + 1` where `N` is the number of nodes in the cluster. For a 3-node a majority is 2; for a 4-node cluster a majority is 3. Therefore a 3-node cluster can tolerate the failure of a single node, and 4-node cluster can also only tolerate a failure of a single node.

So a 4-node cluster is  no more fault-tolerant than a 3-node cluster, so running a 4-node cluster provides no advantage over a 3-node cluster. Only a 5-node cluster can tolerate the failure of 2 nodes. An analogous argument applies to 5-node vs. 6-node clusters, and so on.

# Creating a cluster
To create a cluster you must launch a node that can act as leader. Do this as follows:
```bash
rqlited -http localhost:4001 -raft localhost:4002 ~/node.1
```
With this command a single node is started, listening for API requests on port 4001 and listening on port 4002 for intra-cluster communication. This node stores its state at `~/node.1`.

To join a second node to this leader, execute the following command:
```bash
rqlited -http localhost:4003 -raft localhost:4004 -join http://localhost:4001 ~/node.2
```
Once executed you now have a cluster of two nodes. Of course, for fault-tolerance you need a 3-node cluster, so launch a third node like so:
```bash
rqlited -http localhost:4005 -raft localhost:4006 -join http://localhost:4001 ~/node.3
```
This entire example assumes, for the sake of clarity, that all nodes are running on the same host.

# Dealing with failure
It is the nature of clustered systems, nodes can fail at anytime. Depending on the size of your cluster, it will tolerate various amounts of failure. With a 3-node cluster, it can tolerate the failure of a single node, including the leader.

If an rqlite process crashes, it is safe to simply to restart it. The node will pick up any changes that happened on the cluster while it was down.

# Growing a cluster

# Replacing a node
