# rqlite Cluster Discovery Service
_For full details on how the Discovery Service is implemented using AWS Lambda and DynamoDB check out [this blog post](http://www.philipotoole.com/building-a-cluster-discovery-service-with-aws-lambda-and-dynamodb/)._

To form a rqlite cluster, the joining node must be supplied with the network address of some other node in the cluster. This requirement -- that one must know the network address of other nodes to join a cluster -- can be inconvenient in various environments. For example, if you do not know which network addresses will be assigned ahead of time, creating a cluster for the first time requires the following steps:

 * First start one node and specify its network address.
 * Let it become the leader.
 * Start the next node, passing the network address of the first node to the second.
 * Repeat this previous step, until you have a cluster of the desired size.

To make all this easier, rqlite also supports _discovery_ mode. In this mode each node registers its network address with an external service, and learns the _join_ addresses of other nodes from the same service.

As a convenience, a free Discovery Service for rqlite is hosted at `discovery.rqlite.com`. Note that this service is provided on an _as-is_ basis, with no guarantees it will always be available (though it has been built in a highly-reliable manner). If you wish to run your own copy of the service, you can deploy the Discovery Service source code yourself.

## Creating a Discovery Service ID
To form a new cluster via discovery, you must first generate a unique Discovery ID for your cluster. This ID is then passed to each node on start-up, allowing the rqlite nodes to automatically connect to each other. To generate an ID using the rqlite discovery service, hosted at `discovery.rqlite.com`, execute the following command:
```
curl -XPOST -L -w "\n" 'http://discovery.rqlite.com'
```
The output of this command will be something like so:
```json
{
    "created_at": "2017-02-20 01:25:45.589277",
    "disco_id": "809d9ba6-f70b-11e6-9a5a-92819c00729a",
    "nodes": []
}
```
In the example above `809d9ba6-f70b-11e6-9a5a-92819c00729a` was returned by the service.

This ID is then provided to each node on start-up.
```shell
rqlited -disco-id 809d9ba6-f70b-11e6-9a5a-92819c00729a
```
When any node registers using the ID, it is returned the current list of nodes that have registered using that ID. If the nodes is the first node to access the service using the ID, it will receive a list that contains just itself -- and will subsequently elect itself leader. Subsequent nodes will then receive a list with more than 1 entry. These nodes will use one of the join addresses in the list to join the cluster.

### Controlling the registered join address
By default each node registers the address passed in via the `-http-addr` option. However if you instead set `-http-adv-addr` when starting a node, the node will instead register that address. This can be useful when telling a node to listen on all interfaces, but that is should be contacted at a specific address. For example:
```shell
rqlited -disco-id 809d9ba6-f70b-11e6-9a5a-92819c00729a -http-addr 0.0.0.0:4001 -http-adv-addr host1:4001
```
In this example, other nodes will contact this node at `host1:4001`.

## Caveats
If a node is already part of a cluster, addresses returned by the Discovery Service are ignored.

## Example
Create a Discovery Service ID:
```shell
$ curl -XPOST -L -w "\n" 'http://discovery.rqlite.com/'
{
    "created_at":
    "2017-02-20 01:25:45.589277",
    "disco_id": "b3da7185-725f-461c-b7a4-13f185bd5007",
    "nodes": []
}
```
To automatically form a 3-node cluster simply pass the ID to 3 nodes, all of which can be started simultaneously via the following commands:
```shell
$ rqlited -disco-id b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.1
$ rqlited -http-addr localhost:4003 -raft-addr localhost:4004 -disco-id b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.2
$ rqlited -http-addr localhost:4005 -raft-addr localhost:4006 -disco-id b3da7185-725f-461c-b7a4-13f185bd5007 ~/node.3
```
_This demonstration shows all 3 nodes running on the same host. In reality you probably wouldn't do this, and then you wouldn't need to select different -http-addr and -raft-addr ports for each rqlite node._

## Removing registered addresses
If you need to remove an address from the list of registered addresses, perhaps because a node has permanently left a cluster, you can do this via the following command (be sure to pass all the options shown to `curl`):
```shell
$ curl -XDELETE -L --post301 http://discovery.rqlite.com/<disco ID> -H "Content-Type: application/json" -d '{"addr": "<node address>"}'
```
For example:
```shell
$ curl -XDELETE -L --post301 http://discovery.rqlite.com/be0dd310-fe41-11e6-bb97-92e4c2da9b50 -H "Content-Type: application/json" -d '{"addr": "http://192.168.0.1:4001"}'
```
