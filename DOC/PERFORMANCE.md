# Understanding Performance
> :warning: **This page is no longer maintained. Visit [rqlite.io](https://www.rqlite.io) for the latest docs.**

rqlite replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced relative to a standalone SQLite database due to the nature of distributed systems. _There is no such thing as a free lunch_.

rqlite performance -- defined as the number of database updates performed in a given period of time -- is primarily determined by two factors:
- Disk performance
- Network latency

Depending on your machine (particularly its IO performance) and network, individual INSERT performance could be anything from 10 operations per second to more than 200 operations per second.

## Disk
Disk performance is the single biggest determinant of rqlite performance. This is because every change to the system must go through the Raft subsystem, and the Raft subsystem calls `fsync()` after every write to its log. Raft does this to ensure that the change is safely persisted in permanent storage before applying those changes to the SQLite database. This is why rqlite runs with an in-memory database by default, as using as on-disk SQLite database would put even more load on the disk, reducing the disk throughput available to Raft.

## Network
When running a rqlite cluster, network latency is also a factor. This is because Raft must contact every node **twice** before a change is committed to the Raft log. Obviously the faster your network, the shorter the time to contact each node.

# Improving Performance

There are a few ways to improve performance, but not all will be suitable for a given application.

## Batching
The more SQLite statements you can include in a single request to a rqlite node, the better the system will perform. 

By using the [bulk API](https://github.com/rqlite/rqlite/blob/master/DOC/BULK.md), transactions, or both, throughput will increase significantly, often by 2 orders of magnitude. This speed-up is due to the way Raft and SQLite work. So for high throughput, execute as many operations as possible within a single transaction.

## Queued Writes
If you can tolerate a small risk of some data loss in the event that a node crashes, you could consider using the [Queued Writes API](https://github.com/rqlite/rqlite/blob/master/DOC/QUEUED_WRITES.md). Using Queued Writes can easily give you orders of magnitude improvement in performance.

## Use more powerful hardware
Obviously running rqlite on better disks, better networks, or both, will improve performance.

## Use a memory-backed filesystem
It is possible to run rqlite entirely on-top of a memory-backed file system. This means that **both** the Raft log and SQLite database would be stored in memory only. For example, on Linux you can create a memory-based filesystem like so:
```bash
mount -t tmpfs -o size=512m tmpfs /mnt/ramdisk
```
**This comes with risks, however**. The behavior of rqlite when a node fails, but committed entries the Raft log have not actually been permanently persisted, **is not defined**. But if your policy is to completely deprovision your rqlite node, or rqlite cluster, in the event of any node failure, this option may be of interest to you. Perhaps you always rebuild your rqlite cluster from a different source of data, so can recover an rqlite cluster regardless of its state. Testing shows that using rqlite with a memory-only file system can result in 100x improvement in performance.

## Improving read-write concurrency
SQLite can offer better concurrent read and write support when using an on-disk database, compared to in-memory databases. But as explained above, using an on-disk SQLite database can significant impact performance. But since the database-update performance will be so much better with an in-memory database, improving read-write concurrency may not be needed in practise.

However, if you enable an on-disk SQLite database, but then place the SQLite database on a memory-backed file system, you can have the best of both worlds. You can dedicate your disk to the Raft log, but still get better read-write concurrency with SQLite. You can specify the SQLite database file path via the `-on-disk-path` flag.

An alternative approach would be to place the SQLite on-disk database on a disk different than that storing the Raft log, but this is unlikely to be as performant as an in-memory file system for the SQLite database.

# In-memory Database Limits

> :warning: **rqlite was not designed for very large datasets**: While there are no hardcoded limits in the rqlite software, the nature of Raft means that the entire SQLite database is periodically copied to disk, and occasionally copied, in full, between nodes. Your hardware may not be able to process those large data operations successfully. You should test your system carefully when working with multi-GB databases.

In-memory SQLite databases (the default configuration) are currently limited to 2GiB in size. One way to get around this limit is to use an on-disk database, by passing `-on-disk` to `rqlited`. But this could impact write-performance significantly, since disk is slower than memory. If you switch to on-disk SQLite, and find write-performance suffers, there are a couple of ways to address this. One option is to place the Raft log on one disk, and the SQLite database on a different disk.

Another option is to run rqlite in on-disk mode but place the SQLite database file on a memory-backed filesystem. That way you can use larger databases, and still have performance similar to running with an in-memory SQLite database.

In either case to control where rqlite places the SQLite database file, set `-on-disk-startup` and `-on-disk-path` when launching `rqlited`. **Note that you should still place the `data` directory on an actual disk, so that the Raft log is always on a physical disk, to ensure your data is not lost if a node restarts.** 

## Linux example
An example of running rqlite with a SQLite file on a memory-backed file system, and keeping the data directory on persistent disk, is shown below. The data directory is where the Raft log is stored. The example below would allow up to a 4GB SQLite database.
```bash
# Create a RAM disk, and then launch rqlite, telling it to put the SQLite database on the RAM disk.
mount -t tmpfs -o size=4096m tmpfs /mnt/ramdisk
rqlited -on-disk -on-disk-startup -on-disk-path /mnt/ramdisk/node1/db.sqlite /path_to_persistent_disk/data
```
where `/path_to_persistent_disk/data` is a directory path on your persistent disk.

Setting `-on-disk-startup` is also important because it disables an optimization rqlite performs at startup, when using an on-disk SQLite database. rqlite, by default, initially builds any on-disk database in memory first, before moving it to disk. It does this to reduce startup times. But with databases larger than 2GiB, this optimization can cause rqlite to fail to start. To avoid this issue, you can disable this optimization via the flag, but your startup times may be noticeably longer.

