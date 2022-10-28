# Running rqlite on Kubernetes
This document provides an example of how to run rqlite as a Kubernetes [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/). The [source for the configuration below is available in the Kubernetes Configuration repo](https://github.com/rqlite/kubernetes-configuration).

## Creating a cluster 
### Create Services
The first thing to do is to create two [Kubernetes _Services_](https://kubernetes.io/docs/concepts/services-networking/service). The first service, `rqlite-svc-internal`, is [_Headless_](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) and allows the nodes to find each other and cluster automatically. It shouldn't be used by rqlite clients. It is the second service, `rqlite-svc`, that is for clients to talk to the cluster -- this service will get a Cluster IP address which those clients can connect to.

A key difference between `rqlite-svc-internal` and `rqlite-svc` is that the second will only contain Pods that are ready to serve traffic. This makes it most suitable for use by end-users of rqlite.
```bash
curl -s https://raw.githubusercontent.com/rqlite/kubernetes-configuration/master/service.yaml -O rqlite-service.yaml
kubectl apply -f rqlite-service.yaml
```

### Create a StatefulSet
For a rqlite cluster to function properly in a production environment, the rqlite nodes require a persistent network identifier and storage. This is what a _StatefulSet_ can provide. Retrieve the Stateful set configuration, apply it, and a 3-node rqlite cluster will be created.
```bash
curl -s https://raw.githubusercontent.com/rqlite/kubernetes-configuration/master/statefulset-3-node.yaml -o rqlite-3-nodes.yaml
kubectl apply -f rqlite-3-nodes.yaml
```

Note the `args` passed to rqlite in the YAML file. The arguments tell rqlite to use `dns` discovery mode, and to resolve the DNS name `rqlite-svc-internal` to find the IP addresses of other nodes in the cluster. Furthermore it tells rqlite to wait until three nodes are available (counting itself as one of those nodes) before attempting to form a cluster.

## Scaling the cluster
You can grow the cluster at anytime, simply by setting **both** the replica count **and** `bootstrap-expect` argument to the desired cluster size. Shrinking the cluster, however, will require some manual intervention. As well reducing `replicas` and `bootstrap-expect`, you also need to [explicitly remove](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md#removing-or-replacing-a-node) the deprovisioned nodes, or the Leader will continually attempt to contact those nodes.

> :warning: **Be careful that you don't reduce the replica count such that there is no longer a quorum of nodes available. If you do this you will render your cluster unusable, and need to perform a manual recovery.** The manual recovery process is [fully documented](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md#dealing-with-failure).
