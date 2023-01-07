# Bulk API
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

The bulk API allows multiple updates or queries to be executed in a single request. Both non-paramterized and parameterized requests are supported by the Bulk API. The API does not support mixing the parameterized and non-parameterized form in a single request.

A bulk update is contained within a single Raft log entry, so round-trips between nodes are at a minimum. This should result in much better throughput, if it is possible to use this kind of update. You can also ask rqlite to do the batching for you automatically, through the use of [_Queued Writes_](https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md). This relieves the client of doing any batching before transmitting a request to rqlite.

## Updates
Bulk updates are supported. To execute multiple statements in one HTTP call, simply include the statements in the JSON array:

_Non-parameterized example:_
```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
```
_Parameterized example:_
```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d '[
    ["INSERT INTO foo(name) VALUES(?)", "fiona"],
    ["INSERT INTO foo(name) VALUES(?)", "sinead"]
]'
```

The response is of the form:

```json
{
    "results": [
        {
            "last_insert_id": 1,
            "rows_affected": 1,
            "time": 0.00759015
        },
        {
            "last_insert_id": 2,
            "rows_affected": 1,
            "time": 0.00669015
        }
    ],
    "time": 0.869015
}
```
### Atomicity
Because a bulk operation is contained within a single Raft log entry, and only one Raft log entry is ever processed at one time, a bulk operation will never be interleaved with other requests.

### Transaction support
You may still wish to set the `transaction` flag when issuing a bulk update. This ensures that if any error occurs while processing the bulk update, all changes will be rolled back.

## Queries
If you want to execute more than one query per HTTP request then perform a POST, and place the queries in the body of the request as a JSON array. For example:

```bash
curl -XPOST 'localhost:4001/db/query?pretty' -H "Content-Type: application/json" -d '[
    "SELECT * FROM foo",
    "SELECT * FROM bar"
]'
```
Parameterized statements are also supported.
