# Queued Writes
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

## Usage

rqlite exposes a special API flag, which will instruct rqlite to queue up write-requests and execute them asynchronously. This allows clients to send multiple distinct requests to a rqlite node, and have rqlite automatically do the batching and bulk insert for the client, without the client doing any extra work. The net result is as if the client wrote a single Bulk request containing all the queued statements. For the same reason that using the [Bulk API](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md) results in much higher write performance, **using the _Queued Writes_ API will also result in much higher write performance**.

This functionality is best illustrated by an example, showing two requests being queued.
```bash
$ curl -XPOST 'localhost:4001/db/execute?queue' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "fiona"],
    ["INSERT INTO foo(name) VALUES(?)", "sinead"]
]'
{
    "results": [],
    "sequence_number": 1653314298877648934
}
$ curl -XPOST 'localhost:4001/db/execute?queue' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "declan"]
]'
{
    "results": [],
    "sequence_number": 1653314298877649973
}
$
```
Setting the URL query parameter `queue` enables queuing mode, adding the request data to an internal queue whch rqlite manages for you. 

rqlite will merge the requests, once a batch-size of them has been queued on the node or a configurable timeout expires, and execute them as though they had been both contained in a single request. 

Each response includes a monotonically-increasing `sequence_number`, which allows you to track when this request is actually persisted to the Raft log. The `/status` [diagnostics](https://github.com/rqlite/rqlite/blob/master/DOC/DIAGNOSTICS.md) endpoint includes the sequence number of the latest request successfully written to Raft.

### Waiting for a queue to flush
You can explicitly tell the request to wait until the queue has persisted all pending requests. To do this, add the parameter `wait` to the request like so:
```bash
$ curl -XPOST 'localhost:4001/db/execute?queue&wait&timeout=10s' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "bob"]
]'
```
This example also shows setting a timeout. If the queue has not emptied after this time, the request will return with an error. If not set, the time out is set to 30 seconds.

### Configuring queue behaviour
The behaviour of the queue rqlite uses to batch the requests is configurable at rqlite launch time. You can change the minimum number of requests that must be present in the queue before they are written, as well as the timeout after which whatever is in the queue will be written regardless of queue size. Pass `-h` to `rqlited` to see the queue defaults, and list all command-line options.

## Caveats
Like most databases there is a trade-off to be made between write-performance and durability, but for some applications these trade-offs are worth it.

Because the API returns immediately after queuing the requests **but before the data is commited to the Raft log** there is a small risk of data loss in the event the node crashes before queued data is persisted. You can make this window arbitrarily small by adjusting the queuing parameters, at the cost of write performance.

In addition, when the API returns `HTTP 200 OK`, that simply acknowledges that the data has been queued correctly. It does not indicate that the SQL statements will actually be applied successfully to the database. Be sure to check the node's logs and diagnostics if you have any concerns about failed queued writes.

By default, writes from the queue are not performed within a transaction. If you wish to change this, it must be set at launch time via the command line option `-write-queue-tx`.
