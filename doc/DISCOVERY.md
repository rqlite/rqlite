# rqlite Cluster Discovery Service

To form a rqlite cluster, the joining node must be supplied with the network address of some other node in the cluster. This requirement -- that one must know the network address of other nodes to join a cluster -- can be inconvenient in various environments. For example, if you do not know which network addresses will be assigned ahead of time, creating a cluster for the first time requires the following steps:

 * First start one node and specify its network address.
 * Let it become the leader.
 * Start the next node, passing the network address of the first node to the second.
 * Repeat this previous step, until you have a cluster of the desired size.

To make all this easier, rqlite also supports _discovery_ mode. In this mode each node registers its network address with an external service, and learns the _join_ addresses of other nodes from the same service.

## Creating a Discovery Service ID

To form a new cluster via discovery, you must first generate a unique Discovery ID for your cluster. This ID is then passed to each node on start-up, allowing the rqlite nodes to automatically connect to each other. To generate an ID using the free rqlite discovery service, hosted at `discovery.rqlite.com`, execute the following command:
```
curl -XPOST -L -w "\n" 'http://discovery.rqlite.com'
```
The output of this command will be something like so:
```json
{"created_at": "2017-02-20 01:25:45.589277", "disco_id": "809d9ba6-f70b-11e6-9a5a-92819c00729a", "nodes": []}
```
In the example above `809d9ba6-f70b-11e6-9a5a-92819c00729a` was returned by the service.

This ID is then provided to each node on start-up.
```
rqlited --discoid 809d9ba6-f70b-11e6-9a5a-92819c00729a
```
When any node registers using the ID, it is returned the current list of nodes that have registered using that ID. If the nodes is the first node to access the service using the ID, it will receive a list that contains just itself -- and will subsequently elect itself leader. Subsequent nodes will then receive a list with more than 1 entry. These nodes will use one of the join addresses in the list to join the cluster.

### Controlling the registered join address
By default each node registers the address passed in via the `--http` option. However if you instead set `--httpadv` when starting a node, the node will instead register that address.

## Caveats
If a node is already part of a cluster, it will ignore any ID that is passed to it at startup.

## Example
Create a Discovery Service ID:
```
$ curl -XPOST -L -w "\n" 'http://discovery.rqlite.com/'
{"created_at": "2017-02-20 01:25:45.589277", "disco_id": "b3da7185-725f-461c-b7a4-13f185bd5007", "nodes": []}
```
Pass the ID to 3 nodes, all of which can be started simultaneously via the following commands:
```
$ rqlited --discoid b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.1
$ rqlited -http localhost:4003 -raft localhost:4004 --discoid b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.2
$ rqlited -http localhost:4005 -raft localhost:4006 --discoid b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.3
```
_This demonstration shows all 3 nodes running on the same host. In reality you probably wouldn't do this, and then you wouldn't need to select different -http and -raft ports for each rqlite node._
