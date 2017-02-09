# rqlite Cluster Discovery Service

To form a rqlite cluster, the joining node must be supplied with the IP address of some other node in the cluster. This requirement -- that one must know the IP address of other nodes to join a cluster -- can be inconvenient in various environments. For example, if you do not know which IP addresses will be assigned ahead of time, creating a cluster for the first time requires the following steps:

 * First start one node and determine its IP address.
 * Let it become the leader.
 * Start the next node, passing the IP address of the first node to it.
 * Repeat this previous step, until you have a cluster of the desired size.

To make all this easier, rqlite also supports _discovery_ mode. In this mode each node registers its IP address with an external service, and learns the _join_ addresses of other nodes from the same service.

## Creating a Discovery URL

To form a new cluster via discovery, you must first generate a unique Discovery URL (DU) for your cluster. This URL is then passed to each node on start-up, allowing the rqlite nodes to automatically connect to each other. To generate a DU using the free rqlite discovery service, hosted at `discovery.rqlite.com`, execute the following command:
```
curl -XPOST -w "\n" 'https://discovery.rqlite.com?size=3'
https://discovery.rqlite.com/204191ca-a8b4-4f03-b6b9-3f7a3a7546f8
```
The `size` paramters will be explained shortly. In the example above `https://discovery.rqlite.com/204191ca-a8b4-4f03-b6b9-3f7a3a7546f8` was returned by the service.

This DU is then provided to each node on start-up.
```
rqlited --disco https://discovery.rqlite.com/204191ca-a8b4-4f03-b6b9-3f7a3a7546f8
```
When each node accesses the DU, the current list of nodes that have registered using that DU. If the nodes is the first node to access the DU, it will receive an empty list and will elect itself leader. The discovery service will also add its _join_ address to the list of nodes it maintains. Subsequent nodes will then receive a non-empty list -- a list of join addresses of existing cluster nodes. These nodes will use one of the join addresses in the list to join the cluster.

### Restricting the cluster size
The `size` param allows you to restrict the number of nodes that can use the DU to join a cluster. In the example, if a fourth node attempts to register using the DU, it will be receive an error from the service.

### Controlling the registered IP address
By default each node registers the address passed in via the `--http` option. However if you instead set `--httpadv` when starting a node, the node will instead register that address.

## Limitations
If a node that is already part of a cluster, even a cluster of one, it will ignore any DU that is passed to it at startup.

## Example
Create a DU:
```
curl -XPOST -w "\n" 'https://discovery.rqlite.com/'
https://discovery.rqlite.com/b3da7185-725f-461c-b7a4-13f185bd5007
```
Pass the URL to 3 nodes, all of which can be started simultaneously:
```
rqlited --disco https://discovery.rqlite.com/b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.1
rqlited -http localhost:4003 -raft localhost:4004 --disco https://discovery.rqlite.com/b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.2
rqlited -http localhost:4005 -raft localhost:4006 --disco https://discovery.rqlite.com/b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.3
```
_This demonstration shows all 3 nodes running on the same host. In reality you probably wouldn't do this, and then you wouldn't need to select different -http and -raft ports for each rqlite node._
