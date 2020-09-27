# Bulk API
The bulk API allows multiple updates or queries to be executed in a single request. Both non-paramterized and parameterized requests are supported by the Bulk API. The API does not support mixing the parameterized and non-parameterized form in a single request.

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
A bulk update is contained within a single Raft log entry, so the network round-trips between nodes in the cluster are amortized over the bulk update. This should result in better throughput, if it is possible to use this kind of update.

### Atomicity
Because a bulk operation is contained within a single Raft log entry, and only one Raft log entry is every processed at one time, a bulk operation will never be interleaved with other requests.

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
