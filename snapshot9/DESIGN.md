Refential Snapshots
=============
Raft Log Truncation allows a Raft-based system to truncate its log by request the system using Raft -- rqlite in this case -- to supply the full state of its system at a point in time. Today rqlite does this by maintaining a second copy of the SQLite database that the main rqlite program is managing (called the "primary"). _Referential Snapshotting_ aims to eliminate this copy and instead have a Snapshot store a pointer to the primary SQLite database.

At a high level _Referential Snapshotting_ will work as follows:
- At startup rqlite will create an instance of a new type called RefentialSnapshotStore. This type will be supplied a directory path for storage, and a file to the primary SQLite database file. RefentialSnapshotStore must implement the following interface:

// SnapshotStore interface is used to allow for flexible implementations
// of snapshot storage and retrieval. For example, a client could implement
// a shared state store such as S3, allowing new nodes to restore snapshots
// without streaming from the leader.
type SnapshotStore interface {
	// Create is used to begin a snapshot at a given index and term, and with
	// the given committed configuration. The version parameter controls
	// which snapshot version to create.
	Create(version SnapshotVersion, index, term uint64, configuration Configuration,
		configurationIndex uint64, trans Transport) (SnapshotSink, error)

	// List is used to list the available snapshots in the store.
	// It should return then in descending order, with the highest index first.
	List() ([]*SnapshotMeta, error)

	// Open takes a snapshot ID and provides a ReadCloser. Once close is
	// called it is assumed the snapshot is no longer needed.
	Open(id string) (*SnapshotMeta, io.ReadCloser, error)
}

- When the Raft system requests that a snapshot of the SQLite dataase be created, a directory will be created in the Raft Snapshot store as usual. The directory name will be of the format XX-YY-ZZ where XX is the Raft term, YY is the Raft log index, and ZZ is a timestamp.
- The snapshotting process will create a file in the Snapshot directory, and, when the Snapshot is completed, that file will contain information about the primary SQLite file including:
  - the timestamp and size (in bytes) of the SQLite.
  - a CRC32 of the primary SQLite database.
- The Snapshot-create process will return a new object called Sink to the Raft system, abstracing away some of the newly-created Snapshot object (which remains in a temporary mode until the Sink is "closed")

The Sink will implement the following interface:

// SnapshotSink is returned by StartSnapshot. The FSM will Write state
// to the sink and call Close on completion. On error, Cancel will be invoked.
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

- rqlite will then do the following:
  - It's critical to know that rqlite runs the primary SQLite database in WAL mode, with auto-checkpoint disabled, via the SQLite command PRAGMA wal_autocheckpoint=0.
  - At snapshot time rqlite will checkpoint the WAL, using TRUNCATE mode, so that the WAL's contents are checkpointed into the SQLite database file, and then the WAL file is truncated to zero bytes in length.
  - Create an instance of a new Protobuf type (called StateMeta) which contains the timestamp and size of the primary SQLite file, as well CRC32 of the primary SQLite file.
  - Set a flag in the Protobuf flagging that the actual data for the primary SQLite database will not be sent.
  - Calculate the size of the marhshaled version of the Protobuf instance.
  - Write the length of the marshalled data as a little-endian 8-byte value
  - Then write the protobuf instance
  - Close the Sink.
  - Writes to rqlite can take place once the WAL is checkpointed, as all writes will go the WAL, but only the SQLite file itself is used in the snapshotting process, and it's not changed. 

  rqlite will do this by implementing the FSM interface:

  type FSM interface {
	// Apply is called once a log entry is committed by a majority of the cluster.
	//
	// Apply should apply the log to the FSM. Apply must be deterministic and
	// produce the same result on all peers in the cluster.
	//
	// The returned value is returned to the client as the ApplyFuture.Response.
	Apply(*Log) interface{}

	// Snapshot returns an FSMSnapshot used to: support log compaction, to
	// restore the FSM to a previous state, or to bring out-of-date followers up
	// to a recent log index.
	//
	// The Snapshot implementation should return quickly, because Apply can not
	// be called while Snapshot is running. Generally this means Snapshot should
	// only capture a pointer to the state, and any expensive IO should happen
	// as part of FSMSnapshot.Persist.
	//
	// Apply and Snapshot are always called from the same thread, but Apply will
	// be called concurrently with FSMSnapshot.Persist. This means the FSM should
	// be implemented to allow for concurrent updates while a snapshot is happening.
	Snapshot() (FSMSnapshot, error)

	// Restore is used to restore an FSM from a snapshot. It is not called
	// concurrently with any other command. The FSM must discard all previous
	// state before restoring the snapshot.
	Restore(snapshot io.ReadCloser) error
}

and FSMSnapshot interface:

type FSMSnapshot interface {
	// Persist should dump all necessary state to the WriteCloser 'sink',
	// and call sink.Close() when finished or call sink.Cancel() on error.
	Persist(sink SnapshotSink) error

	// Release is invoked when we are finished with the snapshot.
	Release()
}

Once the Snapshotting process is complete we have a directory in the RefentialSnapshotStore named after the snapshot which contains the StateMeta. The RefentialSnapshotStore also has the path to the primary SQLite file, which acts as the actual data that has been snapshot. In this way the primary SQLite database file acts as both the database that can serve reads and writes (along with the WAL file) as well as the "state" that the Raft Snapshot store needs.

Now, say a Snapshot is opened. This can happen if a second node requires a Snapshot from the Leader so that the second node can catch up. This is where RefentialSnapshotStore's implementation of Open(id string) (*SnapshotMeta, io.ReadCloser, error) comes in. In this case Open will return an io.ReadCloser object than returns the following data when read:
- an 8-byte value, which is the little-endian encoded length of a StateMeta Protobuf
- a marshalled value of the StateMeta Protobuf. In this case however a flag will be set to indicate that the SQLite data follows the marshalled protobuf data
- then the SQLite database file will be returned via subsequent Read() calls to the Snapshot object.

An important thing to note is that RefentialSnapshotStore will have locking on it, such that the following invariants are true:
- 0, 1, or more Snapshots may be open at any time. The same Snapshot may be open more than once.
- While the number of open snapshots is greater than 0, snapshots cannot be created.
- While a snapshot is being created (Create was called, but the returned Sink has not been closed yet), no Snapshot may be open.
- So, in a way,  RefentialSnapshotStore supports multiple concurrent readers, or a single writer.

Some benefits of this approach:
- 50% reduction in disk usage (at least)
- Much simpler snapshotting process.
- Much faster snapshotting process -- it will be as fast as SQLite WAL checkpoint is (which is very fast)
- Snapshotting, since it's fast, can take place more often, allowing us to work with smaller Raft logs, which means more disk savings.

Downsides:
- The primary copy of the SQLite database cannot be lost. Today the copy in the Snapshot cannot be lost, so we're not introducing any new real risks. Database operators must be careful with their systems and manage them well.

