CDC Service Design

Change-data-capture has been added to the database module of rqlite. This allows one to register two callbacks -- one when rows are about to be updated, and one which is called when the updates are actually committed. The changes has been modeled using protobufs, and the events are sent to a Go channel.

The next task is to design the "CDC Service" which will read this channel and transmit the events to a configurable webhook. rqlite is written in Go.

The events contain both "before" and "after" data for the changed rows, as well as the index of the Raft log containing the SQL statement that triggered the changes.

General structure

The CDC Service (CDCS) will be instantiated with:
- a URL. Only HTTP or HTTPS will be supported for now as the scheme, but other schemes will be supported in the future.
- a tls.Config object, allowing control over such things as mutual TLS. This object may be nil.
- retry policy include number of times to retry POSTing a given event. 
- the CDCS can be configured to drop events after the retry limit is reached, or keeping retrying forever (though this will result in a growing data storage need -- see later).
- a channel on which to read 

When launched the first thing the CDCS will do is create a table call "_rqlite_cdc_state" in the SQLite database, via Raft consensus. This table is used by the CDCS for its own state management. The CDCS will start a goroutine to read the channel. As events are read they are added to a disk-backed, or BoltDB-backed, FIFO queue. The reason for this queue will be explained in a moment.

A second goroutine reads from the FIFO queue.  It continues to read as many events are available (up to a configurable limit so there is a maximum delay with sending CDC events), creates a batch and then checks if it is the Leader. If it is the Leader it performs a HTTP POST to the endpoint with the body containing the JSON marshaled batch, implementing any retries as needed. If it is not the Leader it does not perform the POST, because it assumes the actualy Leader will transmit the events. It then returns to reading from the queue.

Assuming it is the Leader it records in memory the highest Raft Index corresponding to events it has successfully transmitted to the webhook -- call this the CDC high watermark. Concurrently a third goroutine (which has been launched at CDCS startup) periodically writes this high watermark to the SQLite database via Raft consensus to the _rqlite_cdc_state table. In this manner each node learns which events have been transmitted successfully, and this minimizes duplicate events to the webhook.

This means that in addition to checking if it (the CDCS) is leader, it also checks if it's reading events with a Raft index lower than the high watermark. If it does, it doesn't just skip sending those events, but actually deletes them from the queue. 

Leadership changes

The CDCS will listen for leadership changes -- this functionality is already offered by the Raft subsystem. When a node becomes a leader it will start reading the queue from the start, implementing the scheme above. This ensures that every event is transmitted at least once. Careful reading of the above will show that there is a race -- a node could check that is is leader, determine it isn't, and not send an event. However leadership could be gained between the leader check and the decision not to transmit. So when leadership changes a node must resend all events that have not been positively recorded via the high watermark. This means that leadership changes may result in a duplicates.

# Snapshot xfer issues

See https://github.com/rqlite/rqlite/pull/2106/files#r2143667669