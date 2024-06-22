Refential Snapshots
=============
Raft Log Truncation allows a Raft-based system to truncate its log by requesting that the system using Raft -- rqlite in this case -- to supply the full state of its system at a point in time. This full state is known as a "Snapshot". Today rqlite does this by generating a second copy of the SQLite database that rqlite is managing (known as the "primary" database -- the database that actually serves read and write requests). _Referential Snapshotting_ aims to eliminate this second copy as much as possible, and instead have the Snapshot store a pointer to the primary SQLite database.

This will work because rqlite operarates SQLite in WAL mode, meaning that the primary database file doesn't change between Snapshotting, and all writes are actually stored in the WAL.

Detailed Design
=============

At a high level _Referential Snapshotting_ will work as follows: at startup rqlite will create an instance of a new type called RefentialSnapshotStore. This type will be supplied a directory path for storage, and a "handle" to the primary SQLite database file ("handle" be precisely defined later). As per the Hashicorp Raft GoDocs RefentialSnapshotStore must implement the following interface:

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

- When the Raft system requests that a snapshot of the SQLite dataase be created, a directory will be created in the storage directory of RefentialSnapshotStore. The new directory name will be of the format XX-YY-ZZ where XX is the Raft term, YY is the Raft log index, and ZZ is a timestamp, all corresponding to when the Snapshot is triggered.
- The snapshotting process will create a file in the Snapshot directory, called the "proof" file and, when the Snapshot is completed, that file will contain information about the primary SQLite file including:
  - the timestamp and size (in bytes) of the SQLite.
  - a CRC32 of the primary SQLite database.
  - a boolean flag indicating whether this Snapshot contains an actual copy of SQLite data, or if that data should come from the primary database. While most Snapshots will set this flag to true, occassionally it will be false (see later) and a Snapshot will also contain an actual copy of SQLite data.

This Proto defining the Proof file is as follows:

message Proof {
    bool referential = 1;
    uint64 size_bytes = 2;
    uint64 unix_millis = 3;
    fixed32 CRC32 = 4;
}

- The Snapshot-create process will return a new object called Sink to the Raft system, abstracing away some of the newly-created Snapshot object (which remains in a temporary mode, with the snapshot directory named suffixed with ".tmp", until the Sink is "closed")

The Sink will implement the following interface, as required by the Hashicorp Raft specification:

// SnapshotSink is returned by StartSnapshot. The FSM will Write state
// to the sink and call Close on completion. On error, Cancel will be invoked.
type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}


Sink design
=============
Let's talk about the implementation of the Sink. When Sink is created it must know the Snapshot directory, to it knows where to write the information it receives. RefentialSnapshotStore creates Sinks at RefentialSnapshotStore.Open() so will supply that information then.

Secifically the implementation of the Write() method.

Sink.Write() must do the following:
- read the first 8 bytes, so it can determine the length of the Proof protobuf
- interpret those 8 bytes as a little-endian encoded uint64. Call then length N.
- next, read N bytes.
- Using those N bytes, unmarshal a new Proof Protobuf instance.
- Write the Proof to the Snapshot directory.
- Now, the next thing that happens depends on whether Proof.referential is true or not. If false, then Sink.Write() is done, and no more data should be expected. This will be the case when a node is doing its own snapshotting, as part of normal Raft Log Truncastion.
- If referential is false, then the Write should expect more data -- the SQLite data, which should be stored alongside the Proof file. This is the case when a snapshot is being streamed from another node, so that the receiving node can catch up with that other node. That SQLite data may be stored in compressed form, and decompressed as needed later when it's installed into rqlite.

rqlite snapshotting
=================
- On rqlite side during snapshotting rqlite will then do the following:
  - As mentioned earlier, it's critical to know that rqlite runs the primary SQLite database in WAL mode, with auto-checkpoint disabled, via the SQLite command PRAGMA wal_autocheckpoint=0.
  - At snapshot time rqlite will checkpoint the WAL, using TRUNCATE mode, so that the WAL's contents are checkpointed into the SQLite database file, and then the WAL file is truncated to zero bytes in length.
  - Create an instance of a new Protobuf type (the Proof) which contains the timestamp and size of the primary SQLite file, as well CRC32 of the primary SQLite file. It also contains the refential flag, which is set to true.
  - Calculate the size of the marhshaled version of the Protobuf instance.
  - Write the length of the marshalled data as a little-endian 8-byte value
  - Then write the protobuf instance
  - Close the Sink.
  - Writes to rqlite can take commence once the WAL is checkpointed, as all writes will go the WAL, but only the SQLite file itself is used in the snapshotting process, and it's not changed. 

  rqlite will need to implement the following interface, as specified by the Hashicorp Raft specification:

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

FSMSnapshot.Persist() will be implemented such that when it is writing to the Sink it will know whether Refential is true or not, allowing it to know whether it should write the SQLite data to the Sink. This can be done in FSM.Snapshot(). By definition when this function is called, it's for the purposes of creating a Snapshot for the purposes of truncation, and the node will know to generate a refential snapshot i.e. don't write the data containined in the primary SQLite file to the sink.

Once the Snapshotting process is complete we have a directory in the RefentialSnapshotStore named after the snapshot which contains the Proof. The RefentialSnapshotStore has a 
"handle" to the primary SQLite file, allowing it to generate and stream the full Snapshot to any node that requests it. In this way the primary SQLite database file acts as both the database that can serve reads and writes (along with the WAL file) as well as the "data" that the Raft Snapshot store needs for its Snapshot.

When a Snapshot is created, any older Snapshots are deleted. This is called "reaping". At anytime only one Snapshot will be valid on a given node in the cluster, and the valid Snapshot is always the most recently-created snapshot.

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

Managing inconsistent states
=================
The processes outlined above can be interrupted due to crashes, power loss, and so on. RefentialSnapshotStore will need to peform a check at start-up time to check for this, and repair itself as needed, which could include discarding incomplete snapshots. This is why Snapshot directories are created with a .tmp suffix. The .tmp suffix is not removed until the Snapshot is 100% complete.


Upgrade paths
=================

Upgrade from earlier versions will be seamless. RefentialSnapshotStore will automatically upgrade any existing Snapshots it finds during initialization time. Also, as long as the nodes running earlier versions are all up, and caught up with the leader, a rolling upgrade will be supported. However operators will be instructed to make this upgrade quickly, so that any cluster is running a mix of releases for the shortest time possible.



Design v2
===================
"just write the file, sink should be simple".

Let's do a scenario-based approach, outline each interface implementation:

SnapshotStore.Open():
- examines "state.bin" in Snapshot directory
- if it's a SQLite file, then wrap io.ReadCloser around it, and hand it back.
- if it's a Proof file, then wrap SnapshotSink around the "primary" database. How to get at it? Pass in "Provider" interface at SnapshotStore init time. Confirm that the "Proof" returned by "Provider" matches Proof in Snapshot directory. Exit if no match?
- either way the io.ReadCloser wraps a SQLite database.
- SnapShotMeta is created on the fly, just store in the Snapshot directory only what needed (term, index, cfg, cfg index). Size can be figured out.

SnapshotStore.Create():
- create snapshot directory
- create a SnapshotSink which simply writes whatever it receives to "state.bin"
- write necesssary meta (don't worry about size in meta)

SnapshotStore.List():
- easy, same as today.

FSM.Snapshot()
- switch to synchronous=FULL
- perform WAL checkpoints
- switch back to synchronous=OFF (which should be done at startup too)
- Create Proof object (size, time), encapsulate in FSMSnapshot

FSM.Restore()
- Called in two situations -- receiving a snapshot from another node, and at start-up.
- At rqlited start, check if the latest Snapshot has a Proof matching the Proof of the primary SQLite database. If so, set NoSnapshotRestoreOnStart=true. If doesn't match, exit with error.
- So only situation is streaming. Assume io.ReadCloser() passed to Restore wraps a SQLite file (verify!) and do a normal restore. Basically same as today.

FSMSnapshot.Persist()
- Calculate CRC32 at this point, add to Proof object encapsulated in FSMSnapshot object
- Write encapsulated Proof to Sink.

rqlited shutdown (graceful)
- do a snapshot at shutdown to checkpoint WAL.

rqlited startup
- If no snapshots present at all, delete any primary SQLite files.
- Confirm that primary SQLite database Proof matches any in Snapshot store. If no match, exit with error.
- If there are any WAL files, delete them. Sign of hard crash.

Open questions:
- should the SQLite database, if stored in the Snapshot store, be compressed? If so, when does compression and decompression happen? Use a single byte to flag type!
- when do I add/drop CRC32? For large databases it will be slow. Still check modified-time and size. 
- implications of Vacuum and Auto-vacuum?

Open tasks:
- upgrade from v8 snapshot stores
- if snapshotting is interrrupted, must be detectable and fixable.
- remove option to explicitly set SQLite path. Too dangerous now.
