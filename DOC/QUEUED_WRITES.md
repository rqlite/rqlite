# Queued Writes API
> :warning: **This functionality was introduced in version 7.5. It does not exist in earlier releases.**

rqlite exposes a special API, which will queue up write-requests and execute them in bulk. This allows clients to send multiple distinct requests to a rqlite node, and have rqlite automatically do the batching and bulk insert for the client, without the client doing any extra work. This functionality is best illustrated by an example, showing two requests being queued.
```bash
curl -XPOST 'localhost:4001/db/execute/queue/_default' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "fiona"],
    ["INSERT INTO foo(name) VALUES(?)", "sinead"]
]'
curl -XPOST 'localhost:4001/db/execute/queue/_default' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "declan"]
]'
```
rqlite will merge these requests, and execute them as though they had been both contained in a single request. For the same reason that using the [Bulk API](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md) results in much higher write performance, using the _Queued Writes_ API will also result in much higher write performance.

The behaviour of the queue rqlite uses to batch the requests is configurable at rqlite launch time. Pass `-h` to `rqlited` to list all configuration options.

## Caveats
Because the API returns immediately after queuing the requests **but before the data is commited to the SQLite database** there is a risk of data loss in the event the node crashes before queued data is persisted.

Like most databases there is a trade-off to be made between write-performance and durability. In addition, when the API returns `HTTP 200 OK`, that simply acknowledges that the data has been queued correctly. It does not indicate that the SQL statements will actually be applied successfully to the database. Be sure to check the node's logs if you have any concerns about failed queued writes.

