# Change Data Capture (CDC) in rqlite

## Overview

Change Data Capture (CDC) is a process that identifies and captures changes made to data in a database, then delivers those changes to other systems or processes. rqlite's CDC feature provides a mechanism to observe row-level changes (INSERTs, UPDATEs, DELETEs) within the database.

When enabled, rqlite will log these changes as structured JSON events. This allows users or external systems to monitor database modifications, which can be useful for various purposes such as:

*   Auditing and logging changes.
*   Data synchronization with external data stores.
*   Real-time analytics and event processing.
*   Cache invalidation.

This is an initial experimental implementation, and its capabilities and output mechanisms may evolve.

## Enabling CDC

To enable Change Data Capture, start the `rqlited` server with the `-cdc` command-line flag:

```bash
rqlited -cdc <other_rqlite_options> /path/to/node/data
```

By default, CDC is disabled. When the `-cdc` flag is provided, rqlite will register an internal hook with the SQLite database that gets triggered before any row is inserted, updated, or deleted.

## Event Format

CDC events are currently logged as JSON strings to the standard rqlite log output (usually stderr). Each log entry containing a CDC event will be prefixed, for example, with "CDC Event: ".

The JSON object for a CDC event has the following structure:

*   **`operation`**: `string` - The type of database operation.
    *   Enum values: `"OPERATION_TYPE_INSERT"`, `"OPERATION_TYPE_UPDATE"`, `"OPERATION_TYPE_DELETE"`.
    *   *Note: The field name in the JSON output is `operation`, derived from the protobuf field name. This might be `operation_type` in future revisions for consistency with common naming conventions.*
*   **`table_name`**: `string` - The name of the table where the change occurred.
*   **`old_row_id`**: `integer` (int64) - The ROWID of the row before the operation.
    *   Present for `UPDATE` and `DELETE` operations.
    *   For `INSERT` operations, this field will typically be `0` or absent if not applicable.
*   **`new_row_id`**: `integer` (int64) - The ROWID of the row after the operation.
    *   Present for `INSERT` and `UPDATE` operations.
    *   For `DELETE` operations, this field will typically be `0` or absent if not applicable.
*   **`old_row`**: `object` - A JSON object representing the data of the row *before* the change.
    *   Present for `UPDATE` and `DELETE` operations.
    *   The keys of this object are the column names, and the values are the corresponding column data.
    *   If a column was `NULL`, its key might be absent or its value might be `null`, depending on the specific data and ongoing refinements to NULL handling.
*   **`new_row`**: `object` - A JSON object representing the data of the row *after* the change.
    *   Present for `INSERT` and `UPDATE` operations.
    *   Similar structure to `old_row`.
*   **`timestamp`**: `integer` (int64) - A Unix timestamp in nanoseconds since the epoch, indicating when the event was captured by rqlite.

### Data Type Representation in `old_row` and `new_row`

Column values within `old_row` and `new_row` are represented as follows in the JSON:
*   **INTEGER**: JSON number (e.g., `123`)
*   **REAL**: JSON number (e.g., `3.14159`)
*   **TEXT**: JSON string (e.g., `"hello world"`)
*   **BLOB**: JSON string, base64-encoded (e.g., `"AQIDBA=="` for `\x01\x02\x03\x04`)
*   **NULL**: Represented by the JSON value `null` if the column was explicitly NULL, or the key might be absent from the row object if the column was not part of the update/insert data that was NULL. The exact representation for `NULL` values (especially distinguishing between an omitted field and an explicit `NULL`) is an area that might see refinement. The underlying protobuf uses a `oneof` for values, and an unset `oneof` for a specific column value results in its key being omitted from the JSON representation of that `CDCValue` object.

### Example Events

**INSERT Event:**

A new row is inserted into `users` table with `id=1`, `name="Alice"`, `email="alice@example.com"`.

```json
{
  "operation": "OPERATION_TYPE_INSERT",
  "table_name": "users",
  "new_row_id": 1,
  "new_row": {
    "values": [
      {"i": 1},
      {"s": "Alice"},
      {"s": "alice@example.com"}
    ]
  },
  "timestamp": 1678886400123456789
}
```
*(Note: `old_row_id` might be 0 or absent. `old_row` is absent.)*
*(The `values` array in `new_row` corresponds to the column order in the table schema at the time of the event.)*

**UPDATE Event:**

The user with `id=1` changes their email from `"alice@example.com"` to `"alice_new@example.com"`.

```json
{
  "operation": "OPERATION_TYPE_UPDATE",
  "table_name": "users",
  "old_row_id": 1,
  "new_row_id": 1,
  "old_row": {
    "values": [
      {"i": 1},
      {"s": "Alice"},
      {"s": "alice@example.com"}
    ]
  },
  "new_row": {
    "values": [
      {"i": 1},
      {"s": "Alice"},
      {"s": "alice_new@example.com"}
    ]
  },
  "timestamp": 1678886405123456789
}
```

**DELETE Event:**

The user with `id=1` is deleted.

```json
{
  "operation": "OPERATION_TYPE_DELETE",
  "table_name": "users",
  "old_row_id": 1,
  "old_row": {
    "values": [
      {"i": 1},
      {"s": "Alice"},
      {"s": "alice_new@example.com"}
    ]
  },
  "timestamp": 1678886410123456789
}
```
*(Note: `new_row_id` might be 0 or absent. `new_row` is absent.)*

## Consuming Events

Currently, CDC events are written as JSON strings to the standard rqlite log output (typically stderr). To consume these events, users need to:

1.  Monitor the rqlite log file or stream.
2.  Identify log lines corresponding to CDC events (e.g., by looking for a "CDC Event: " prefix).
3.  Parse the JSON string from the identified log line into a structured format for further processing.

Future enhancements to rqlite may include more direct methods for consuming CDC events, such as:
*   Writing events to a dedicated CDC log file.
*   Streaming events to systems like Kafka or NATS.
*   Providing an API endpoint to subscribe to CDC events.

## Limitations and Considerations

*   **Experimental Feature:** This is an initial implementation of CDC. The feature, its configuration, and the event format may evolve in future releases.
*   **Log-Based Output:** Relying on parsing general log output for critical event consumption can be fragile. Users should implement robust parsing and error handling.
*   **Performance:** Enabling CDC introduces additional processing for each database write operation (INSERT, UPDATE, DELETE) due to the pre-update hook mechanism and JSON serialization. While designed to be lightweight, users with extremely high write throughput should benchmark the impact.
*   **Schema Changes:** This CDC implementation captures row-level data changes. It does not explicitly capture Data Definition Language (DDL) changes (e.g., `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`) as distinct event types. While the effects of some DDL (like `DROP TABLE` causing row deletions) might result in row-level events, the DDL operation itself is not recorded as a specific CDC event.
*   **Transaction Context:** Individual row changes are logged. There is currently no explicit grouping of events by transaction. If multiple rows are changed in a single transaction, each row change will appear as a separate CDC event.
*   **ROWID Dependency:** The feature relies on SQLite's `ROWID` (or its alias for `INTEGER PRIMARY KEY` columns) to identify rows. Tables created `WITHOUT ROWID` are not currently supported by this CDC mechanism.

Feedback on this feature is welcome via the rqlite GitHub repository issues.
