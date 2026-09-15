# Cluster Package Design

## Purpose

The `cluster` package handles all of rqlite's intra-cluster control-plane traffic: forwarding writes from a follower to the leader, fetching node metadata, joining and removing nodes, triggering leader stepdowns, broadcasting CDC high-water marks, and shipping backups between nodes. It also owns the bootstrap process that brings a new node into a cluster — discovering peers via DNS, Consul, etcd, or a static address list, and choosing whether to join an existing cluster or help form a new one.

Above this package, callers treat the cluster as opaque. The HTTP layer says "execute this on the leader." The CDC service says "tell every node to advance its watermark to N." The CLI operator says "remove node X." This package is what makes those instructions actually reach the right node.

It does *not* carry Raft's own RPCs — those go through the hashicorp/raft library's transport. This package only contributes the mux header byte that lets Raft and the cluster service share a single TCP port.

## The Wire and the Multiplexed Port

Every node listens on a single TCP port for *all* internode traffic. The first byte of any inbound connection determines which subsystem handles it: `MuxRaftHeader` (1) routes to Raft's transport, `MuxClusterHeader` (2) routes to this package's `Service`. The mux itself lives in `tcp/`; this package contributes the header constants and the listener that the cluster service binds to.

The choice of one port instead of two is operational. Operators already have to open and firewall a Raft port and an HTTP port; making them open a third for the cluster control plane would be real friction with no end-user payoff. Internode traffic is opaque to users — the multiplexing is an implementation detail. The HTTP port stays on a separate listener because end-user traffic has fundamentally different networking requirements (TLS termination, public exposure in some setups) from the closed mesh between rqlite nodes.

The wire format above the mux header is straightforward: an 8-byte little-endian length prefix followed by a marshaled `proto.Command` protobuf. Responses are framed the same way. Most messages are a single request/response round trip; `COMMAND_TYPE_BACKUP_STREAM` is the one streaming exception, noted below.

## Service

`Service` (in `service.go`) is the receive side. It accepts mux-tagged connections, reads `proto.Command` messages off the wire, dispatches by `Type`, and writes responses. Each connection runs on its own goroutine and serves multiple commands in sequence — clients reuse connections from a pool, so request-per-connection would multiply handshake cost. A 30-second read deadline (`connTimeout`) is reset before each read so an idle or stalled client cannot hold a goroutine indefinitely.

The handler set:

- **`GET_NODE_META`** — returns the node's HTTP API URL, software version, and Raft commit index. Used by the HTTP layer to build redirect responses, by the bootstrap path to learn what's reachable, and by stale-read clients to gauge how far behind a follower is.
- **`EXECUTE` / `QUERY` / `REQUEST`** — request forwarding. A non-leader node receives a write or strong read on its HTTP port and pushes it through this path to the leader, which executes and returns the response unchanged.
- **`BACKUP` / `BACKUP_STREAM`** — fetch a SQLite-format backup from a remote node. See *Backup Transport* below.
- **`LOAD`** — push a SQLite file to a node, which routes it through Raft so every replica picks it up.
- **`JOIN` / `NOTIFY` / `REMOVE_NODE`** — cluster membership control. `JOIN` returns the leader address as a hint when the receiver is a follower; the client follows the redirect itself.
- **`STEPDOWN`** — request that the receiver give up leadership, optionally to a named successor. Implemented by calling through to the underlying Raft library's transfer-leadership primitive.
- **`HIGHWATER_MARK_UPDATE`** — non-blocking notification used by the CDC layer to advance the per-node watermark on remote nodes.

Permission checks happen per message via the `CredentialStore` interface (`AA(user, pass, perm)`); when no credential store is configured every check passes. There is no session — each command carries its own credentials, so there is no in-memory authorization state to invalidate.

## Client

`Client` (in `client.go`) is the send side, used by the HTTP layer (to forward to the leader), by the bootstrap and removal flows, and by the CDC HWM broadcaster. It maintains a `pool.Pool` per remote address (`maxPoolCapacity = 64`), reused across requests, so every forward does not pay the cost of a new TCP+TLS handshake.

A `SetLocal(nodeAddr, *Service)` shortcut lets the client serve `GetNodeMeta` for the local node without a network round-trip. The HTTP layer holds a single `Client` per process; asking it about *self* — which happens often enough — should not touch the network.

### Connection Pool Retries

Every retry-eligible request goes through `Client.retry`. The base loop is unsurprising: dial from the pool, write/read, return on success, mark the connection unusable on error and try again. The wrinkle is the *last* retry. If every previous attempt has failed, the client does not just take another connection from the pool — it forces a brand-new dial via `pl.New()`.

This catches a real production failure pattern that motivated the v10 fix. When a remote node restarts, the local pool can be holding sixty-three TCP connections that all *look* fine but are actually dead. Each retry takes one of those, fails, marks it unusable, and moves on, but if the pool is replenished as fast as retries deplete it the request can fail without ever proving the remote is reachable. The "force new on last retry" path bypasses the pool entirely so a single fresh connection definitively distinguishes "node down" from "all my pool entries are stale." It only triggers after retries are otherwise exhausted, so the cost is negligible in steady state.

Cluster management operations (`Join`, `Notify`, `RemoveNode`, `Stepdown`) deliberately *do not* retry at this layer. Operator-initiated commands should fail visibly so the operator knows what happened, and higher layers (the `Bootstrapper`, the HTTP handler) are the right places to add policy-aware retries.

### The Join Redirect Dance

`Join` is the one wire-level method that loops on its own. If the receiver returns `"not leader"` along with a `leader` address, the client retargets to the named leader and tries again. The address provider may hand the bootstrapper a follower; the redirect lets the join succeed without bothering the caller.

## Bootstrapping

A new rqlite node, given a list of peer addresses, has two possibilities: a cluster already exists out there (in which case it should *join*), or there isn't one yet (in which case it might need to *form* one with its peers). `Bootstrapper` (in `bootstrap.go`) handles both cases inside a single loop without requiring the operator to know which one applies.

The loop:

1. Look up the current target list via the `AddressProvider` (DNS, Consul, etcd, or a static slice).
2. Try `Joiner.Do(targets, ...)` — iterate every target, send a `JOIN`, follow leader redirects. If any target accepts, the node has joined an existing cluster and the bootstrap finishes with status `BootJoin`.
3. If every join failed *and the node is a Voter*, send a `NOTIFY` to every target. Each remote node accumulates notifying peers; when one of them reaches its `bootstrap-expect` count, it triggers `raft.BootstrapCluster` with the full set, and the new cluster includes this node.
4. Sleep (jittered) and retry. Stop when `done()` returns true (the Store has reached steady state), the context is cancelled, or the timeout fires.

The "join first, then notify" ordering is the load-bearing operational property. Operators can use the same command line every time — first launch, restart after the cluster exists, scaling from three to five nodes — and the bootstrapper does the right thing. If a cluster exists, the new node joins it; if not, the notify fan-out converges all the starting nodes onto a single bootstrap. Without this, every operator's automation would need an "if cluster exists, join, otherwise bootstrap" branch — exactly the kind of conditional that breaks under Kubernetes-style "every pod looks the same" deployments.

Non-voters never call `Notify`. Raft has no concept of a non-voting member at bootstrap time — the initial cluster configuration is voters-only. A non-voter that fails to join has no productive next step at this layer; it just retries the join.

`Joiner.Do` is the join-only piece, exposed separately because the explicit `-join` command-line flag and the auto-bootstrap path both need it. It iterates every target until one accepts.

## Removal

`Remover` (in `remove.go`) is the dual of bootstrapping: send a `REMOVE_NODE` request to the current leader, with retries on transient failures (`removeRetries=5`, `removeDelay=250ms`). A `Control` interface (`WaitForLeader`, `WaitForRemoval`) lets the remover wait for a leader to exist before sending and, optionally, confirm the configuration change has propagated before returning. Unlike the wire-level `Client.RemoveNode`, the `Remover` *does* retry — it operates above the wire and represents an operator's intent that should be carried to completion.

## Discovery (`disco/`)

The `disco/` subpackage is rqlite's "two nodes can find each other through a third party" layer. It's placed under `cluster/` because discovery is operationally part of cluster formation — once a cluster is up, discovery quiets down — and pulling it out into another top-level package would add to a list that was already getting long.

`disco.Service` orchestrates the interaction with whatever KV store is in use; the actual Consul and etcd clients live elsewhere and satisfy the `disco.Client` interface (`GetLeader`, `InitializeLeader`, `SetLeader`).

The flow is two-phased:

- **`Register`** — on startup, every node polls the discovery service for the current leader. If one is set, the node returns that address (the bootstrapper then joins it). If no leader is set and the node is a Voter, it tries `InitializeLeader` (a check-and-set), claiming itself as the inaugural leader; whichever node wins that CAS becomes the seed for the cluster.
- **`StartReporting`** — once a node *is* leader, it periodically writes its address back to the KV store. Two goroutines coordinate this: one watches Raft leadership transitions (via the Store's `RegisterLeaderChange`) and triggers an immediate report on the rising edge; the other reports on a periodic ticker so a missed update — slow KV, transient error — gets corrected without waiting for the next leadership change.

Reports are non-blocking — if the report channel is full the update is dropped, since the periodic ticker will pick up any miss.

## CDC HighwaterMark Broadcast

CDC delivery is at-least-once, and downstream consumers acknowledge by advancing a "highwater mark" — the Raft index up to which they've successfully delivered. The leader broadcasts this watermark to every other node so non-leader replicas can prune stale CDC entries from their local queues even though they're not the ones producing the deliveries.

`Client.BroadcastHWM` does the fan-out: dial each target in parallel, apply per-target retries, collect responses (or errors) into a map. The receive side is intentionally tiny — `Service.handleConn` does a non-blocking send into the channel registered via `RegisterHWMUpdate`; if the channel is full the update is dropped (with a stat increment), since HWM updates are frequent enough that missing one is fine. The CDC layer owns the channel and decides what to do with each update.

## Backup Transport

Two backup paths exist for historical and operational reasons:

- **`BACKUP`** marshals the entire backup into a single response, gzip-compresses the whole protobuf, and writes it length-prefixed. The server has to materialize the full backup in memory before responding.
- **`BACKUP_STREAM`** writes a small header response (with any error), then streams the backup bytes directly down the connection. The stream is *always* gzip-compressed, regardless of whether the requester asked for compression, so the client can detect end-of-stream by EOF on the gzip reader. If the user wanted uncompressed bytes, the client decompresses on its end.

`Client.Backup` always uses the stream variant; `BACKUP` exists for older protocol compatibility and for callers that want a single-buffer response.

## Key Design Decisions and Trade-offs

- **Single port for all internode traffic.** Both Raft and the cluster control plane share one TCP listener with mux byte tags. The trade-off is a small amount of demux glue; the payoff is one fewer port for operators to manage and firewall. The HTTP port stays separate because end-user traffic has fundamentally different networking requirements.

- **Per-node connection pool with last-retry bypass (v10).** Pooled connections amortize TCP/TLS setup. Pooled connections after a *remote restart*, however, can all be silently dead. The v10 "force a fresh dial on the last retry" path catches that case without giving up the pool's steady-state benefits. This was a real production bug; a node restart could cause the local node's forwarding requests to fail until the entire pool churned through.

- **Cluster-management operations do not auto-retry at the wire.** Joins, removals, notifies, and stepdowns are operator-driven; auto-retrying them silently would obscure failures. Higher layers (`Bootstrapper`, `Remover`, the HTTP handler) add their own retry policy where it makes sense.

- **Join-first-then-notify bootstrap.** A node always tries to join an existing cluster before trying to form a new one. The same command line works for first launch, restart, and scale-up, so operators don't have to encode "is there a cluster yet?" in their automation. Non-voters never notify because Raft bootstraps only with voting members.

- **Per-message credential checks, no session.** Each command carries its own credentials and its own permission check. There is no in-memory authorization state to invalidate when credentials change.

- **Length-prefixed protobuf, no streaming framing.** Almost every command is a single request/response, so the framing is correspondingly minimal. The streaming exceptions (`BACKUP_STREAM`, the channel-based HWM broadcast) handle their own end-of-stream detection.

- **`disco/` lives under `cluster/`.** Discovery is part of cluster formation, and placing it under `cluster/` keeps related code together without adding to the list of top-level packages. The actual KV-store clients (Consul, etcd) live elsewhere and satisfy the `disco.Client` interface.

- **Local shortcut for self-meta lookups.** `Client` knows its own node address and its own `Service` instance, so `GetNodeMeta` for "self" returns immediately without a network round-trip. Cheap and frequent enough to matter.
