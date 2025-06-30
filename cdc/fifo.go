package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

// bucketName is the name of the BoltDB bucket where queue items will be stored.
var bucketName = []byte("fifo_queue")

// ErrQueueEmpty is returned when First() is called on an empty queue.
var ErrQueueEmpty = errors.New("queue is empty")

// Queue is a persistent, disk-backed FIFO queue.
// It is safe for concurrent use by multiple goroutines.
type Queue struct {
	db   *bbolt.DB
	mu   sync.Mutex
	cond *sync.Cond // cond is used to signal waiting Dequeue calls.
}

// NewQueue creates or opens a new persistent queue at the given file path.
func NewQueue(path string) (*Queue, error) {
	// Open the BoltDB database file at the given path.
	// It will be created if it doesn't exist.
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open boltdb: %w", err)
	}

	// Start a read-write transaction to ensure our bucket exists.
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close() // Close the db if bucket creation fails.
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	q := &Queue{
		db: db,
	}
	// Initialize the condition variable with the queue's mutex.
	// This is crucial for coordinating the Dequeue and Enqueue operations.
	q.cond = sync.NewCond(&q.mu)

	return q, nil
}

// Close closes the underlying BoltDB database.
func (q *Queue) Close() error {
	return q.db.Close()
}

// Enqueue adds an item to the queue with a given index.
// The FIFO order is determined by the index; lower indices are considered "earlier" in the queue.
func (q *Queue) Enqueue(idx uint64, item []byte) error {
	// Lock the mutex to ensure only one Enqueue operation happens at a time
	// and to safely signal the condition variable.
	q.mu.Lock()
	defer q.mu.Unlock()

	// Convert the uint64 index into a byte slice.
	// We use BigEndian to ensure that the byte representation sorts lexicographically
	// in the same order as the numerical value of the index. This is how BoltDB
	// maintains key order.
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, idx)

	// Start a read-write transaction to insert the new item.
	err := q.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.Put(key, item)
	})

	if err != nil {
		return fmt.Errorf("failed to enqueue item: %w", err)
	}

	// After successfully adding an item, we signal the condition variable.
	// This will wake up ONE goroutine that is waiting in a Dequeue() call.
	// If no goroutines are waiting, this signal is a no-op.
	q.cond.Signal()

	return nil
}

// First returns the first item in the queue (the one with the lowest index)
// and its index, without removing it. If the queue is empty, it returns ErrQueueEmpty.
func (q *Queue) First() (uint64, []byte, error) {
	var key, val []byte

	// Start a read-only transaction.
	err := q.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()

		// Move the cursor to the first key in the bucket.
		// Because we use BigEndian for keys, this is the item with the lowest index.
		k, v := c.First()
		if k == nil {
			return ErrQueueEmpty // No items in the queue.
		}
		
		// BoltDB keys/values are only valid inside the transaction.
		// We must copy them to new slices to use them after the transaction completes.
		key = make([]byte, len(k))
		copy(key, k)
		val = make([]byte, len(v))
		copy(val, v)
		
		return nil
	})

	if err != nil {
		return 0, nil, err
	}

	// Convert the key back to a uint64 index.
	idx := binary.BigEndian.Uint64(key)

	return idx, val, nil
}

// Dequeue removes and returns the next available item from the queue.
// If the queue is empty, Dequeue blocks until an item is enqueued.
func (q *Queue) Dequeue() ([]byte, error) {
	// The mutex is locked for the entire duration of the check-and-wait-or-delete logic.
	// This is the core of the coordination.
	q.mu.Lock()
	defer q.mu.Unlock()

	var key, val []byte

	// This loop is the standard pattern for using sync.Cond.
	// It repeatedly checks the condition (is the queue non-empty?) until it's true.
	for {
		// Check if an item exists within a read-only transaction.
		err := q.db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(bucketName).Cursor()
			k, v := c.First()
			if k == nil {
				// Queue is empty. We will wait.
				return nil
			}
			
			// Item found. Copy the key and value so we can use them after the
			// view transaction closes and before the update transaction starts.
			key = make([]byte, len(k))
			copy(key, k)
			val = make([]byte, len(v))
			copy(val, v)

			return nil
		})

		if err != nil {
			// This indicates a DB error, not an empty queue.
			return nil, fmt.Errorf("failed to check queue: %w", err)
		}
		
		if key != nil {
			// An item was found, so we break out of the waiting loop.
			break
		}

		// If key is nil, it means the queue was empty.
		// We call cond.Wait(), which does three things atomically:
		// 1. Unlocks the mutex (q.mu).
		// 2. Puts the current goroutine to sleep.
		// 3. When woken up by q.cond.Signal(), it re-locks the mutex before returning.
		// This atomic unlock/wait/relock prevents the "lost wakeup" problem.
		q.cond.Wait()
	}

	// At this point, we have found an item (key and val are populated) and we still
	// hold the mutex lock. Now we can safely delete it.
	err := q.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Delete(key)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to delete dequeued item: %w", err)
	}
	
	return val, nil
}

// Delete removes a specific item from the queue by its index.
// It is safe for concurrent use.
func (q *Queue) Delete(idx uint64) error {
	// Lock the mutex to ensure thread safety, consistent with other methods.
	q.mu.Lock()
	defer q.mu.Unlock()

	// Convert the uint64 index into the 8-byte big-endian key.
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, idx)

	// Start a read-write transaction to delete the item.
	err := q.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		// Delete the key. If the key doesn't exist, this is a no-op
		// and does not return an error.
		return b.Delete(key)
	})

	if err != nil {
		return fmt.Errorf("failed to delete item with index %d: %w", idx, err)
	}

	return nil
}

// DeleteRange removes all items from the queue with an index
// between lowerIdx and upperIdx, inclusive.
// If lowerIdx > upperIdx, the operation is a no-op.
func (q *Queue) DeleteRange(lowerIdx, upperIdx uint64) error {
	// A forgiving check: if the range is invalid, do nothing.
	if lowerIdx > upperIdx {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	minKey := make([]byte, 8)
	binary.BigEndian.PutUint64(minKey, lowerIdx)

	maxKey := make([]byte, 8)
	binary.BigEndian.PutUint64(maxKey, upperIdx)

	err := q.db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()

		// We repeatedly seek to the beginning of the range.
		// After we delete an item, the next seek will find the
		// new lowest item in the range. The loop terminates when
		// seeking to minKey lands on a key outside our maxKey boundary.
		for k, _ := c.Seek(minKey); k != nil && bytes.Compare(k, maxKey) <= 0; k, _ = c.Seek(minKey) {
			if err := c.Delete(); err != nil {
				// If delete fails, we should abort the transaction.
				return err
			}
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed during range delete from %d to %d: %w", lowerIdx, upperIdx, err)
	}

	return nil
}
