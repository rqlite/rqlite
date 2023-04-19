# Automatic clustering
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

This document describes various ways to dynamically form rqlite clusters, which is particularly useful for automating your deployment of rqlite.

> :warning: **This functionality was introduced in version 7.0. It does not exist in earlier releases.**

## Contents
* [Quickstart](#quickstart)
  * [Automatic Bootstrapping](#automatic-bootstrapping)
  * [Using DNS for Bootstrapping](#using-dns-for-bootstrapping)
    * [DNS SRV](#dns-srv)
  * [Kubernetes](#kubernetes)
  * [Consul](#consul)
  * [etcd](#etcd)
* [Next steps](#next-steps)
  * [Customizing your configuration](#customizing-your-configuration)
    * [Running multiple different clusters](#running-multiple-different-clusters)
* [Design](#design)

## Quickstart

### Automatic Bootstrapping
While [manually creating a cluster](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md) is simple, it does suffer one drawback -- you must start one node first and with different options, so it can become the Leader. _Automatic Bootstrapping_, in constrast, allows you to start all the nodes at once, and in a very similar manner. **You just need to know the network addresses of the nodes ahead of time**.

For simplicity, let's assume you want to run a 3-node rqlite cluster. The network addresses of the nodes are `$HOST1`, `$HOST2`, and `$HOST3`. To bootstrap the cluster, use the `-bootstrap-expect` option like so:

Node 1:
```bash
rqlited -node-id 1 -http-addr=$HOST1:4001 -raft-addr=$HOST1:4002 \
-bootstrap-expect 3 -join http://$HOST1:4001,http://$HOST2:4001,http://$HOST3:4001 data
```
Node 2:
```bash
rqlited -node-id 2 -http-addr=$HOST2:4001 -raft-addr=$HOST2:4002 \
-bootstrap-expect 3 -join http://$HOST1:4001,http://$HOST2:4001,http://$HOST3:4001 data
```
Node 3:
```bash
rqlited -node-id 3 -http-addr=$HOST3:4001 -raft-addr=$HOST3:4002 \
-bootstrap-expect 3 -join http://$HOST1:4001,http://$HOST2:4001,http://$HOST3:4001 data
```

`-bootstrap-expect` should be set to the number of nodes that must be available before the bootstrapping process will commence, in this case 3. You also set `-join` to the HTTP URL of all 3 nodes in the cluster. **It's also required that each launch command has the same values for `-bootstrap-expect` and `-join`.**

After the cluster has formed, you can launch more nodes with the same options. A node will always attempt to first perform a normal cluster-join using the given join addresses, before trying the bootstrap approach.

#### Docker
With Docker you can launch every node identically:
```bash
docker run rqlite/rqlite -bootstrap-expect 3 -join http://$HOST1:4001,http://$HOST2:4001,http://$HOST3:4001
```
where `$HOST[1-3]` are the expected network addresses of the containers.

__________________________

### Using DNS for Bootstrapping
You can also use the Domain Name System (DNS) to bootstrap a cluster. This is similar to automatic clustering, but doesn't require you to specify the network addresses of other nodes at the command line. Instead you create a DNS record for the host `rqlite.local`, with an [A Record](https://www.cloudflare.com/learning/dns/dns-records/dns-a-record/) for each rqlite node's IP address. 

To launch a node with node ID `$ID` and network address `$HOST`, using DNS for cluster bootstrap, execute the following (example) command:
```bash
rqlited -node-id $ID -http-addr=$HOST:4001 -raft-addr=$HOST:4002 \
-disco-mode=dns -disco-config='{"name":"rqlite.local"}' -bootstrap-expect 3 data
```
You would launch other nodes similarly, setting `$ID` and `$HOST` as required for each node. In the example above, resolving `rqlite.local` should result in 3 IP addresses.

#### DNS SRV
Using [DNS SRV](https://www.cloudflare.com/learning/dns/dns-records/dns-srv-record/) gives you more control over the rqlite node address details returned by DNS, including the HTTP port each node is listening on. This means that unlike using just simple DNS records, each rqlite node can be listening on a different HTTP port. Simple DNS records are probably good enough for most situations, however.

To launch a node using DNS SRV bootstrap, execute the following (example) command:
```bash
rqlited -node-id $ID  -http-addr=$HOST:4001 -raft-addr=$HOST:4002 \
-disco-mode=dns-srv -disco-config='{"name":"rqlite.local","service":"rqlite-svc"}' -bootstrap-expect 3 data
```
You would launch other nodes similarly, setting `$ID` and `$HOST` as required for each node. You would launch other nodes similarly. In the example above rqlite will lookup SRV records at `_rqlite-svc._tcp.rqlite.local`
__________________________

### Kubernetes
DNS-based approaches can be quite useful for many deployment scenarios, in particular systems like Kubernetes. To learn how to deploy rqlite on Kubernetes, check the [Kubernetes deployment guide](https://github.com/rqlite/rqlite/blob/master/DOC/KUBERNETES.md).
__________________________

### Consul
Another approach uses [Consul](https://www.consul.io/) to coordinate clustering. The advantage of this approach is that you do not need to know the network addresses of all nodes ahead of time.

Let's assume your Consul cluster is running at `http://example.com:8500`. Let's also assume that you are going to run 3 rqlite nodes, each node on a different machine. Launch your rqlite nodes as follows:

Node 1:
```bash
rqlited -node-id $ID1 -http-addr=$HOST1:4001 -raft-addr=$HOST1:4002 \
-disco-mode consul-kv -disco-config '{"address":"example.com:8500"}' data
```
Node 2:
```bash
rqlited -node-id $ID2 -http-addr=$HOST2:4001 -raft-addr=$HOST2:4002 \
-disco-mode consul-kv -disco-config '{"address":"example.com:8500"}' data
```
Node 3:
```bash
rqlited -node-id $ID3 -http-addr=$HOST3:4001 -raft-addr=$HOST3:4002 \
-disco-mode consul-kv -disco-config '{"address":"example.com:8500"}' data
```

These three nodes will automatically find each other, and cluster. You can start the nodes in any order and at anytime. Furthermore, the cluster Leader will continually update Consul with its address. This means other nodes can be launched later and automatically join the cluster, even if the Leader changes. Refer to the [_Next Steps_](#next-steps) documentation below for further details on Consul configuration.

#### Docker
It's even easier with Docker, as you can launch every node almost identically:
```bash
docker run rqlite/rqlite -disco-mode=consul-kv -disco-config '{"address":"example.com:8500"}'
```
__________________________

### etcd
A third approach uses [etcd](https://www.etcd.io/) to coordinate clustering. Autoclustering with etcd is very similar to Consul. Like when you use Consul, the advantage of this approach is that you do not need to know the network addresses of all the nodes ahead of time.

Let's assume etcd is available at `example.com:2379`.

Node 1:
```bash
rqlited -node-id $ID1 -http-addr=$HOST1:4001 -raft-addr=$HOST1:4002 \
	-disco-mode etcd-kv -disco-config '{"endpoints":["example.com:2379"]}' data
```
Node 2:
```bash
rqlited -node-id $ID2 -http-addr=$HOST2:4001 -raft-addr=$HOST2:4002 \
	-disco-mode etcd-kv -disco-config '{"endpoints":["example.com:2379"]}' data
```
Node 3:
```bash
rqlited -node-id $ID3 -http-addr=$HOST3:4001 -raft-addr=$HOST3:4002 \
	-disco-mode etcd-kv -disco-config '{"endpoints":["example.com:2379"]}' data
```
 Like with Consul autoclustering, the cluster Leader will continually report its address to etcd.  Refer to the [_Next Steps_](#next-steps) documentation below for further details on etcd configuration.

 #### Docker
```bash
docker run rqlite/rqlite -disco-mode=etcd-kv -disco-config '{"endpoints":["example.com:2379"]}'
```

## Next Steps
### Customizing your configuration
For detailed control over Discovery configuration `-disco-confg` can either be an actual JSON string, or a path to a file containing a JSON-formatted configuration. The former option may be more convenient if the configuration you need to supply is very short, as in the examples above.

The examples above demonstrates simple configurations, and most real deployments may require more detailed configuration. For example, your Consul system might be reachable over HTTPS. To more fully configure rqlite for Discovery, consult the relevant configuration specification below. You must create a JSON-formatted configuration which matches that described in the source code.

- [Full Consul configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/consul/config.go)
- [Full etcd configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/etcd/config.go)
- [Full DNS configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/dns/config.go)
- [Full DNS SRV configuration description](https://github.com/rqlite/rqlite-disco-clients/blob/main/dnssrv/config.go)

#### Running multiple different clusters
If you wish a single Consul or etcd key-value system to support multiple rqlite clusters, then set the `-disco-key` command line argument to a different value for each cluster. To run multiple rqlite clusters with DNS, use a different domain name per cluster.

## Design
When using Automatic Bootstrapping, each node notifies all other nodes of its existence. The first node to have a record of enough nodes (set by `-bootstrap-expect`) forms the cluster. Only one node can bootstrap the cluster, any other node that attempts to do so later will fail, and instead become a Follower in the new cluster.

When using either Consul or etcd for automatic clustering, rqlite uses the key-value store of each system. In each case the Leader atomically sets its HTTP URL, allowing other nodes to discover it. To prevent multiple nodes updating the Leader key at once, nodes uses a check-and-set operation, only updating the Leader key if it's value has not changed since it was last read by the node. See [this blog post](https://www.philipotoole.com/rqlite-7-0-designing-node-discovery-and-automatic-clustering/) for more details on the design.

For DNS-based discovery, the rqlite nodes simply resolve the hostname, and use the returned network addresses, once the number of returned addresses is at least as great as the `-bootstrap-expect` value. Clustering then proceeds as though the network addresses were passed at the command line via `-join`.
