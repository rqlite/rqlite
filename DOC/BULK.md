# Bulk API
The bulk API allows multiple updates or queries to be executed in a single request. 

## Updates
Bulk updates are supported. To execute multipe statements in one HTTP call, simply include the statements in the JSON array:

```bash
curl -XPOST 'localhost:4001/db/execute?pretty&timings' -H "Content-Type: application/json" -d "[
    \"INSERT INTO foo(name) VALUES('fiona')\",
    \"INSERT INTO foo(name) VALUES('sinead')\"
]"
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
    "time": 0.869015,
    "raft": {
        "index": 32,
        "node_id": "14f66175-eb70-4b64-a716-d5a16c91fcaa"
    }
}
```

A bulk update is contained within a single Raft log entry (as evidenced by the single `raft` section), so the network round-trips between nodes in the cluster are amortized over the bulk update. This should result in better throughput, if it is possible to use this kind of update.

## Queries
If you want to execute more than one query per HTTP request then perform a POST, and place the queries in the body of the request as a JSON array. For example:

```bash
curl -XPOST 'localhost:4001/db/query?pretty' -H "Content-Type: application/json" -d '[
    "SELECT * FROM foo",
    "SELECT * FROM bar"
]'
```
