# Automatic clustering
This document describes various ways to automatically form rqlite clusters, which is particularly useful for when you want to automate your deployment of rqlite.

> :warning: **This functionality was introduced in version 7.x. It does not exist in earlier releases.**

## Contents
* [Quickstart](#quickstart)
  * [Automatic Boostrapping](#automatic-bootstrapping)
  * [Consul](#consul)
  * [etcd](#etcd)
* [More details](more-details)
  * [Controlling Consul and etcd configuration](#controlling-consul-and-etcd-configuration)
  * [Running multiple different clusters](#running-multiple-different-clusters)
* [Design](design)

## Quickstart

### Automatic Boostrapping
While manually creating a cluster is simple, it does suffer one drawback -- you must start one node first and with different options, so it can become the Leader. _Automatic Bootstrapping_, in constrast, allows you to start all the nodes at once, and in a very similar manner. You just need to know the network addresses of the nodes ahead of time.

For simplicity, let's assume you want to run a 3-node rqlite cluster. To bootstrap the cluster, use the `-bootstrap-expect` option like so:

Node 1:
```bash
rqlited -http-addr=$IP1:$HTTP_PORT -raft-addr=$IP1:$RAFT_PORT \
-bootstrap-expect 3 -join http://$IP1:HTTP_PORT,http://$IP2:HTTP_PORT,http://$IP2:HTTP_PORT data
```
Node 2:
```bash
rqlited -http-addr=$IP2:$HTTP_PORT -raft-addr=$IP2:$RAFT_PORT \
-bootstrap-expect 3 -join http://$IP1:HTTP_PORT,http://$IP2:HTTP_PORT,http://$IP2:HTTP_PORT data
```
Node 3:
```bash
rqlited -http-addr=$IP3:$HTTP_PORT -raft-addr=$IP3:$RAFT_PORT \
-bootstrap-expect 3 -join http://$IP1:HTTP_PORT,http://$IP2:HTTP_PORT,http://$IP2:HTTP_PORT data
```

`-bootstrap-expect` should be set to the number of nodes that must be available before the bootstrapping process will commence, in this case 3. You also set `-join` to the HTTP URL of all 3 nodes in the cluster.

After the cluster has formed, you can launch more nodes with the same options. A node will always attempt to first perform a normal cluster-join using the given join addresses, before trying the bootstrap approach.

### Consul
This approach makes use of the key-value store offered by [Consul](https://www.consul.io/). The advantage of this approach is that you do need to know the network addresses of the nodes ahead of time.

Let's assume your Consul cluster is running at `http://example.com:8500`. Let's also assume that you are going to run 3 rqlite nodes, each node on a different machine. Launch your rqlite nodes as follows:

Node 1:
```bash
rqlited -http-addr=$IP1:$HTTP_PORT -raft-addr=$IP1:$RAFT_PORT \
-disco-mode consul-kv -disco-config '{"address": "example.com:8500"}' data
```
Node 2:
```bash
rqlited -http-addr=$IP2:$HTTP_PORT -raft-addr=$IP2:$RAFT_PORT \
-disco-mode consul-kv -disco-config '{"address": "example.com:8500"}' data
```
Node 3:
```bash
rqlited -http-addr=$IP3:$HTTP_PORT -raft-addr=$IP3:$RAFT_PORT \
-disco-mode consul-kv -disco-config '{"address": "example.com:8500"}' data
```

These three nodes will automatically find each other, and cluster. You can start the nodes in any order and at anytime. Furthermore, the cluster Leader will continually update Consul with its address. This means other nodes can be launched later and automatically join the cluster, even if the Leader changes.

#### Docker
It's even easier with Docker, as you can launch every node almost identically:
```bash
docker run rqlite/rqlite -disco-mode=consul-kv -disco-config '{"address": "example.com:8500"}'
```

### etcd
This approach makes use of the key-value store offered by [etcd](https://etcd.io/). Autoclustering with etcd is very similar to Consul. Like when you use Consul, the advantage of this approach is that you do need to know the network addresses of the nodes ahead of time.

Let's assume etcd is available at `example.com:2379`.

Node 1:
```bash
rqlited -http-addr=$IP1:$HTTP_PORT -raft-addr=$IP1:$RAFT_PORT \
	-disco-mode etcd-kv -disco-config '{"endpoints": ["example.com:2379"]}' data
```
Node 2:
```bash
rqlited -http-addr=$IP2:$HTTP_PORT -raft-addr=$IP2:$RAFT_PORT \
	-disco-mode etcd-kv -disco-config '{"endpoints": ["example.com:2379"]}' data
```
Node 3:
```bash
rqlited -http-addr=$IP3:$HTTP_PORT -raft-addr=$IP3:$RAFT_PORT \
	-disco-mode etcd-kv -disco-config '{"endpoints": ["example.com:2379"]}' data
```
 Like with Consul autoclustering, the cluster Leader will continually report its address to etcd.

 #### Docker
```bash
docker run rqlite/rqlite -disco-mode=etcd-kv -disco-config '{"endpoints": ["example.com:2379"]}'
```

## More Details
### Controlling Consul and etcd configuration
For both Consul and etcd, `-disco-confg` can either be an actual JSON string, or a path to a file containing a JSON-formatted configuration. The former option may be more convenient if the configuration you need to supply is very short, as in the example above.

The example above demonstrates a simple configuration, and most real deployments will require more configuration information for Consul and etcd. For example, your Consul system might be reachable over HTTPS. To more fully configure rqlite for Discovery, consult the relevant configuration specification below. You must create a JSON-formatted file which matches that described in the source code.

- [Full Consul configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/consul/config.go)
- [Full etcd configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/etcd/config.go)

#### Running multiple different clusters
If you wish a single Consul or etcd system to support multiple rqlite clusters, then set the `-disco-key` command line argument to a different value for each cluster.

## Design
When using Automatic Bootstrapping, each node notifies all other nodes of its existence. The first node to have a record of enough nodes (set by `-boostrap-expect`) forms the cluster. Only one node can ever form a cluster, any node that attempts to do so later will fail, and instead become Followers in the new cluster.

When using either Consul or etcd for automatic clustering, rqlite uses the key-value store of each system. In each case the Leader atomically sets its HTTP URL, allowing other nodes to discover it. To prevent multiple nodes updating the Leader key at once, nodes uses a check-and-set operation, only updating the Leader key if it's value has not changed since it was last read by the node. See [this blog post](https://www.philipotoole.com/rqlite-7-0-designing-node-discovery-and-automatic-clustering/) for more details on the design.
