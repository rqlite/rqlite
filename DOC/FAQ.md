## What exactly does rqlite do?
rqlitem is about replicating a set of data, most likely modeled in a relational manner (after all, it uses SQLite as its storage engine). It's replicated for fault tolerance i.e. your data is so important that you want multiple copies distributed in different places.

On top of that, you want strict guarantees about what state any copy of that data is in, with respect to the leader. That is where Raft comes in. It prevents divergent copies, and ensures there is an "authoritative" copy of that data at all times.
