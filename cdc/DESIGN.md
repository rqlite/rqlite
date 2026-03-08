# CDC Service Design

## Purpose

The `cdc` package implements real-time Change Data Capture for rqlite. It streams row-level changes (INSERT, UPDATE, DELETE) from the SQLite database to a configurable webhook endpoint as JSON over HTTP. The delivery guarantee is **at-least-once**: every committed change will be delivered to the webhook at minimum once, but duplicates are possible during leadership transitions or restarts.

## Background: Why a Separate CDC Queue?

The obvious approach to CDC would be to replay changes from the Raft log. However, Raft truncates its log during snapshotting, and rqlite snapshots frequently — especially with incremental snapshots. Blocking snapshotting until a potentially slow CDC subscriber has consumed all events would degrade cluster performance. Coupling CDC delivery to the Raft log would force a choice between timely log truncation and reliable change delivery.

Instead, the CDC service maintains its own persistent queue (a BoltDB-backed FIFO) that is independent of the Raft log. Changes are written to the FIFO as they are committed, and the FIFO is pruned only after the leader confirms successful delivery to the webhook. This decouples CDC delivery from Raft log management entirely — snapshotting can proceed freely without risking event loss.

## Architecture Overview

The CDC service runs on every node in the cluster, but only the **leader** transmits events to the webhook. Every node captures changes and persists them to its local FIFO. This ensures that if a follower becomes leader, it already has the events queued and ready to send.

### Data Flow

```
SQLite commit hooks (during FSM Apply)
    |
    v
in channel (CDCIndexedEventGroup protobufs)
    |
    v
Batcher (collects events up to batch size or time limit)
    |
    v
FIFO (BoltDB-backed persistent queue, compressed with flate)
    |
    v
Leader loop (reads from FIFO, sends to webhook)
    |
    v
Sink (HTTP POST to webhook endpoint)
```

### Change Capture

Changes are captured via SQLite's pre-update hook and commit hook, registered by the `db` package. The pre-update hook captures the before/after state of each affected row. The commit hook fires when the transaction commits. These hooks execute during `FSM.Apply`, so each event group is tagged with the **Raft log index** of the command that triggered the changes.

The Raft index serves two purposes:
- It provides a total ordering of changes across the cluster.
- It allows downstream consumers to **deduplicate** events by tracking the highest index they have processed.

### Event Format

Events are delivered as JSON via HTTP POST. The envelope structure is:

```json
{
  "service_id": "optional-id",
  "node_id": "node-abc",
  "payload": [
    {
      "index": 42,
      "commit_timestamp": 1709827200000,
      "events": [
        {
          "op": "INSERT",
          "table": "users",
          "new_row_id": 7,
          "before": null,
          "after": {"id": 7, "name": "Alice"}
        }
      ]
    }
  ]
}
```

Each payload entry corresponds to a single Raft log entry (a committed SQL statement) and contains all row-level changes it produced. The `service_id` field allows downstream consumers to differentiate events from multiple rqlite clusters.

A **row-IDs-only mode** is available (`row_ids_only` in the config) which omits the before/after column data, reducing transmission overhead when consumers only need to know which rows changed.

**Table filtering** is supported via a regular expression (`table_filter` in the config). When set, only changes to tables whose names match the pattern are captured.

## Key Components

### Service

The `Service` is the top-level coordinator. It is instantiated with a `Config`, a `Cluster` interface for leadership and high watermark operations, and a channel on which to receive CDC events from the FSM.

On `Start`, the service launches two goroutines:

- **writeToBatcher**: Reads events from the input channel and writes them to an in-memory batcher. This runs on every node regardless of leadership status. It also handles snapshot synchronization (see below).

- **mainLoop**: Manages leadership transitions and reads batched events from the batcher into the persistent FIFO. It switches between a leader loop and a follower loop based on leadership notifications.

### Batcher

The batcher (`queue.Queue`) collects individual events into batches before writing them to the FIFO. Batching is controlled by two parameters:

- `MaxBatchSz`: Maximum number of events per batch (default 10).
- `MaxBatchDelay`: Maximum time to wait before flushing a partial batch (default 200ms).

Events are JSON-marshaled and compressed with flate before being written to the FIFO. Compression reduces BoltDB disk I/O, which is important because rqlite is sensitive to disk I/O — the same disk typically serves Raft log writes.

### FIFO Queue

The `Queue` is a persistent, disk-backed FIFO built on BoltDB. It has several properties that make it suitable for CDC:

- **Persistent**: Survives process restarts. On startup, queued events that haven't been confirmed as delivered are still available for transmission.

- **Read does not delete**: Reading an event from the queue's channel does not remove it. Events are only removed via `DeleteRange` when the high watermark advances. This ensures events survive failed transmission attempts.

- **Duplicate-aware**: The queue tracks the highest index ever enqueued (persisted in a metadata bucket). Any attempt to enqueue an event with an index at or below this value is silently ignored. Since the queue stores changes keyed by Raft log index, repeated enqueue attempts for the same index contain identical data and can be safely dropped.

- **Channel-based emission**: Events are emitted on a channel (`C`), allowing the leader loop to select on events alongside stop signals and other channels.

The queue is managed by a single goroutine that serializes all operations (enqueue, delete, query, emit), avoiding lock contention on the BoltDB file.

### Leader Loop

When a node becomes leader, the `leaderLoop` starts:

1. Read the next event from the FIFO channel.
2. Skip it if its index is at or below the current high watermark (already delivered).
3. Decompress the event data.
4. POST the JSON payload to the webhook endpoint via the sink.
5. Retry on failure according to the configured retry policy (linear or exponential backoff).
6. On success, advance the local high watermark.

A concurrent `leaderHWMLoop` periodically:
- **Broadcasts** the high watermark to all voting nodes in the cluster.
- **Prunes** the local FIFO by deleting all events up to the high watermark.

The high watermark is broadcast continuously (even when unchanged) to ensure new or recovering nodes receive the current value.

### Follower Loop

When a node is a follower, the `followerLoop` listens for high watermark updates broadcast by the leader. When a new high watermark arrives, the follower:
1. Updates its local high watermark.
2. Prunes its local FIFO up to that point.

This keeps follower FIFO sizes bounded and ensures that if a follower becomes leader, it only needs to transmit events that haven't been delivered yet.

### Sink

The `Sink` interface abstracts the delivery target. Two implementations exist:

- **HTTPSink**: POSTs JSON payloads to an HTTP/HTTPS endpoint. Supports TLS configuration including mutual TLS.
- **StdoutSink**: Writes events to stdout, useful for debugging and testing.

The sink is selected based on the configured endpoint string. Using `"stdout"` as the endpoint activates the StdoutSink.

### Cluster Interface

The `Cluster` interface decouples the CDC service from the concrete Raft and cluster implementations. It provides:

- `RegisterLeaderChange`: Receive notifications when leadership changes.
- `RegisterSnapshotSync`: Synchronize with the snapshotting process (see below).
- `RegisterHWMUpdate`: Receive high watermark updates from the leader.
- `BroadcastHighWatermark`: Broadcast the high watermark to all voting nodes.

The `CDCCluster` type is the production implementation, bridging the CDC service to `store.Store` and `cluster.Service`.

## Snapshot Synchronization

Snapshotting deletes Raft log entries. If the CDC batcher has events in memory that haven't been written to the FIFO yet, those events could be lost if the corresponding log entries are truncated during snapshotting.

To prevent this, the CDC service registers a snapshot synchronization channel. Before a snapshot proceeds, the store sends a channel on `snapshotCh`. The CDC service responds by:

1. Writing a flush marker to the batcher.
2. Flushing the batcher, forcing all pending events to be written to the FIFO.
3. Waiting for the flush to complete (the flush marker propagates through the batcher and back).
4. Closing the received channel, signaling that snapshotting can proceed.

This ensures all committed changes are safely in the BoltDB FIFO before any Raft log truncation occurs. The synchronization is brief — it only blocks snapshotting for the time it takes to flush the in-memory batcher, not for the time it takes to deliver events to the webhook.

## Leadership Transitions and Duplicate Handling

Leadership transitions can cause duplicate deliveries. The scenario:

1. Leader sends event at index 100 to webhook successfully.
2. Leader updates local high watermark to 100.
3. Before the high watermark broadcast reaches followers, leadership changes.
4. New leader's FIFO still contains index 100 (its follower loop hadn't pruned it yet).
5. New leader resends index 100.

This is by design — the system guarantees at-least-once delivery. Downstream consumers can deduplicate by tracking the highest Raft index they have processed and ignoring events with lower or equal indices.

During normal operation (stable leadership), duplicates do not occur. The high watermark broadcast ensures that once delivery is confirmed, events are pruned from all nodes' FIFOs.

## Configuration

The CDC service is configured via `-cdc-config`, which accepts either:
- A URL (e.g., `http://localhost:8080/cdc`) — uses default settings with that endpoint.
- The string `"stdout"` — writes events to stdout.
- A file path — reads a JSON configuration file with full control over all parameters.

### Configurable Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `endpoint` | (required) | Webhook URL or `"stdout"` |
| `service_id` | (empty) | Optional identifier for multi-cluster setups |
| `row_ids_only` | false | Omit before/after column data |
| `table_filter` | (none) | Regex to filter which tables are captured |
| `max_batch_size` | 10 | Max events per HTTP request |
| `max_batch_delay` | 200ms | Max time before flushing a partial batch |
| `high_watermark_interval` | 1s | How often the leader broadcasts the HWM |
| `transmit_timeout` | 5s | HTTP request timeout |
| `transmit_max_retries` | (forever) | Max retries before dropping; nil = infinite |
| `transmit_retry_policy` | linear | `linear` or `exponential` backoff |
| `transmit_min_backoff` | 1s | Initial retry delay |
| `transmit_max_backoff` | 30s | Maximum retry delay (exponential only) |

TLS configuration (CA cert, client cert/key, server name, skip verify) is available under the `tls` key in the JSON config file.

## Key Design Decisions and Trade-offs

- **Independent FIFO over Raft log replay.** Storing CDC events in a separate BoltDB queue consumes additional disk space but completely decouples CDC delivery from Raft log truncation. A slow webhook endpoint cannot block snapshotting or degrade cluster performance.

- **Leader-only transmission.** Only the leader sends events to the webhook, preventing N-fold event multiplication in an N-node cluster. The trade-off is that leadership transitions require the new leader to resume from its FIFO, which may cause brief delays or duplicates.

- **At-least-once, not exactly-once.** Exactly-once delivery would require two-phase commit with the webhook endpoint, adding complexity and latency. At-least-once is simpler and sufficient — downstream consumers can deduplicate using the Raft index, which provides a total order.

- **Compression in the FIFO.** Events are flate-compressed before writing to BoltDB. This reduces disk I/O at the cost of CPU for compression/decompression. Since rqlite nodes are typically disk I/O bound (Raft log fsync is the primary bottleneck), this is a favorable trade-off.

- **Batcher before FIFO.** Batching events before writing to BoltDB amortizes the cost of BoltDB transactions. Without batching, every individual CDC event would require a separate BoltDB write transaction with fsync.

- **High watermark broadcast, not consensus.** The HWM is broadcast to followers via the cluster service rather than written through Raft consensus. This avoids generating Raft log entries for CDC bookkeeping, which would themselves trigger CDC events and create a feedback loop. The trade-off is that the HWM can lag behind during network partitions, potentially causing more duplicates after leadership transitions.
