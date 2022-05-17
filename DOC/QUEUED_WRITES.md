# Queued Writes API
> :warning: **This functionality was introduced in release 7.5.0. It does not exist in earlier releases.**

rqlite exposes a special API, which will queue up write-requests and execute them in bulk. This allows clients to send multiple distinct requests to a rqlite node, and have rqlite automatically do the batching and bulk insert for the client, without the client doing any extra work. This functionality is best illustrated by an example, showing two requests being queued.
```bash
curl -XPOST 'localhost:4001/db/execute?queue' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "fiona"],
    ["INSERT INTO foo(name) VALUES(?)", "sinead"]
]'
curl -XPOST 'localhost:4001/db/execute?queue' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "declan"]
]'
```
Setting the URL query parameter `queue` enables queuing mode, adding the request data to an internal queue whch rqlite manages for you.

rqlite will merge queued requests, and execute them as though they had been both contained in a single request. The net result is as if the client wrote a single Bulk request (assuming the queue timeout doesn't expire and result in the queue doing more that one Bulk update). For the same reason that using the [Bulk API](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md) results in much higher write performance, using the _Queued Writes_ API will also result in much higher write performance.

The behaviour of the queue rqlite uses to batch the requests is configurable at rqlite launch time. You can change the minimum number of SQL statements that must be present in the queue before they are written, as well as a timeout after which whatever is in the queue will be written regardless of queue size. Pass `-h` to `rqlited` to see the queue defaults, and list all command-line options.

## Caveats
Like most databases there is a trade-off to be made between write-performance and durability, but for some applications these trade-offs are worth it.

Because the API returns immediately after queuing the requests **but before the data is commited to the Raft log** there is a small risk of data loss in the event the node crashes before queued data is persisted. You can make this window arbitrarily small by adjusting the queuing parameters, at the cost of write performance.

In addition, when the API returns `HTTP 200 OK`, that simply acknowledges that the data has been queued correctly. It does not indicate that the SQL statements will actually be applied successfully to the database. Be sure to check the node's logs and diagnostics if you have any concerns about failed queued writes.

By default, writes from the queue are not performed within a transaction. If you wish to change this, it must be set at launch time via the command line option `-write-queue-tx`.
