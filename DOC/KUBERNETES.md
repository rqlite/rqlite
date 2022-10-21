# Running rqlite on Kubernetes
This document provides an example of how to run rqlite as a Kubernetes [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/).

## Creating a cluster 
### Create Services
The first thing to do is to create two [Kubernetes _Services_](https://kubernetes.io/docs/concepts/services-networking/service). The first service, `rqlite-svc-internal`, is [_Headless_](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) and allows the nodes to find each other and cluster automatically. It shouldn't be used by rqlite clients. It is the second service, `rqlite-svc`, that is for clients to talk to the cluster -- this service will get a Cluster IP address which those clients can connect to.

A key difference between `rqlite-svc-internal` and `rqlite-svc` is that the second will only contain Pods that are ready to serve traffic. This makes it most suitable for use by end-users of rqlite.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rqlite-svc-internal
spec:
  clusterIP: None
  publishNotReadyAddresses: True
  selector:
    app: rqlite
  ports:
    - protocol: TCP
      port: 4001
      targetPort: 4001
---
apiVersion: v1
kind: Service
metadata:
  name: rqlite-svc
spec:
  selector:
    app: rqlite
  ports:
    - protocol: TCP
      port: 4001
      targetPort: 4001
```
Apply the configuration above to your Kubernetes deployment.

```bash
kubectl apply -f headless-service.yaml
```
where the file `headless-service.yaml` contains the configuration shown above.

### Create a StatefulSet
For a rqlite cluster to function properly in a production environment, the rqlite nodes require a persistent network identifier and storage. This is what a _StatefulSet_ can provide. The example belows shows you how to configure a 3-node rqlite cluster.
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: rqlite
spec:
  selector:
    matchLabels:
      app: rqlite # has to match .spec.template.metadata.labels
  serviceName: rqlite-svc-internal
  replicas: 3 # by default is 1
  podManagementPolicy: "Parallel"
  template:
    metadata:
      labels:
        app: rqlite # has to match .spec.selector.matchLabels
    spec:
      terminationGracePeriodSeconds: 10
      containers:
      - name: rqlite
        image: rqlite/rqlite
        args: ["-disco-mode=dns","-disco-config={\"name\":\"rqlite-svc-internal\"}","-bootstrap-expect","3", "-join-interval=1s", "-join-attempts=120"]
        ports:
        - containerPort: 4001
          name: rqlite
        readinessProbe:
          httpGet:
            scheme: HTTP
            path: /readyz
            port: 4001
          initialDelaySeconds: 1
          periodSeconds: 5
        volumeMounts:
        - name: rqlite-file
          mountPath: /rqlite/file
  volumeClaimTemplates:
  - metadata:
      name: rqlite-file
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: "standard"
      resources:
        requests:
          storage: 1Gi
```
Apply this configuration to your Kubernetes system, and a 3-node rqlite cluster will be created.
```bash
kubectl apply -f stateful-set.yaml
```
where the file `stateful-set.yaml` contains the configuration shown above.

Note the `args` passed to rqlite. The arguments tell rqlite to use `dns` discovery mode, and to resolve the DNS name `rqlite-svc-internal` to find the IP addresses of other nodes in the cluster. Furthermore it tells rqlite to wait until three nodes are available (counting itself as one of those nodes) before attempting to form a cluster.

## Scaling the cluster
You can grow the cluster at anytime, simply by increasing the replica count. Shrinking the cluster, however, will require some manual intervention. As well reducing the `replicas` value, you also need to [explicitly remove](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md#removing-or-replacing-a-node) the deprovisioned nodes, or the Leader will continually attempt to contact those nodes.

> :warning: **Be careful that you don't reduce the replica count such that there is no longer a quorum of nodes available. If you do this you will render your cluster unusable, and need to perform a manual recovery.** The manual recovery process is [fully documented](https://github.com/rqlite/rqlite/blob/master/DOC/CLUSTER_MGMT.md#dealing-with-failure).
