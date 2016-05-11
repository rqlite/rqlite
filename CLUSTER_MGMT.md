# Cluster Guidelines
This document describes, in detail, how to create and manage a rqlite cluster.

## Practical cluster size
Clusters of 3, 5, 7, or 9, nodes are most practical. Clusters with a greater number become unweildy.

## Clusters with an even-number of nodes
There is no point running clusters with even numbers of nodes. To see why this is imagine you have one cluster of 3 nodes, and a second of 4 nodes. In both cases a majority of nodes are required for the cluster to reach consensus. A majority is defined as `N/2 + 1` where `N` is the number of nodes in the cluster. For a 3-node a majority is 2; for a 4-node cluster a majority is 3. Therefore a 3-node cluster can tolerate the failure of a single node, and 4-node cluster can also only tolerate a failure of a single node.

So a 4-node cluster is  no more fault-tolerant than a 3-node cluster, so running a 4-node cluster provides no advantage over a 3-node cluster. Only a 5-node cluster can tolerate the failure of 2 nodes. An analogous argument applies to 5-node vs. 6-node clusters, and so on.

# Creating a cluster

# Dealing with failure

# Growing a cluster

# Replacing a node
