# Automatic clustering via node-discovery
This document describes how to use [Consul](https://www.consul.io/) and [etcd](https://etcd.io/) to automatically form rqlite clusters. 

> :warning: **This functionality was introduced in version 7.x. It does not exist in earlier releases.**

## Contents
* [Quickstart](#quickstart)
  * [Consul](#consul)
  * [etcd](#etcd)
* [More details](more-details)
  * [Controlling configuration](#controlling-configuration)
  * [Running multiple different clusters](#running-multiple-different-clusters)

## Quickstart

### Consul
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
It's even easier with Docker, as you can launch every node identically:
```bash
docker run rqlite/rqlite -disco-mode=consul-kv -disco-config '{"address": "example.com:8500"}'
```

### etcd
Autoclustering with etcd is very similar. Let's assume etcd is available at `example.com:2379`.

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
### Controlling configuration
For both Consul and etcd, `-disco-confg` can either be an actual JSON string, or a path to a file containing a JSON-formatted configuration. The former option may be more convenient if the configuration you need to supply is very short, as in the example above.

- [Full Consul configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/consul/config.go)
- [Full etcd configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/etcd/config.go)

### Running multiple different clusters
If you wish a single Consul or etcd system to support multiple rqlite clusters, then set the `-disco-key` command line argument to a different value for each cluster.

### Design
When using either Consul or Ectd for automatic clustering, rqlite uses the key-value store of each system. In each case the Leader atomically sets its HTTP URL, allowing other nodes to discover it. To prevent multiple nodes updating the Leader key at once, nodes uses a check-and-set operation, only updating the Leader key if it's value has not changed since it was last read by the node.
