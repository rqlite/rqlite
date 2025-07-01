package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"go.etcd.io/bbolt"
)

// bucketName is the name of the BoltDB bucket where queue items will be stored.
var bucketName = []byte("fifo_queue")
var metaBucketName = []byte("fifo_queue_meta")

// ErrQueueEmpty is returned when First() is called on an empty queue.
var ErrQueueEmpty = errors.New("queue is empty")

// Queue is a persistent, disk-backed FIFO queue.
// It is safe for concurrent use by multiple goroutines.
type Queue struct {
	db *bbolt.DB

	nextKey []byte
	mu      sync.Mutex
	cond    *sync.Cond // cond is used to signal waiting Dequeue calls.
}

// NewQueue creates or opens a new persistent queue at the given file path.
func NewQueue(path string) (*Queue, error) {
	// Open the BoltDB database file at the given path.
	// It will be created if it doesn't exist.
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open boltdb: %w", err)
	}

	var nextKey []byte
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}

		if _, err := tx.CreateBucketIfNotExists(metaBucketName); err != nil {
			return fmt.Errorf("failed to create meta bucket: %w", err)
		}

		// Ensure the meta bucket has a "max_key" entry.
		// This is used to track the highest index in the queue.
		metaBucket := tx.Bucket(metaBucketName)
		if metaBucket.Get([]byte("max_key")) == nil {
			if err := metaBucket.Put([]byte("max_key"), uint64tob(0)); err != nil {
				return fmt.Errorf("failed to initialize max_key in meta bucket: %w", err)
			}
		}

		// if the queue is not empty, we need to set nextkey
		c := tx.Bucket(bucketName).Cursor()
		if k, _ := c.First(); k != nil {
			// Set nextKey to the first item in the queue.
			// This ensures that the next Dequeue call will return the first item.
			// We store it as a byte slice for consistency with how we store keys.
			nextKey = make([]byte, len(k))
			copy(nextKey, k)
		}

		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	q := &Queue{
		db:      db,
		nextKey: nextKey,
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

	// Only insert an item if its key is "newer" i.e. greater index than the last item we added.
	var highestKey uint64
	err := q.db.View(func(tx *bbolt.Tx) error {
		idx, innerErr := q.getHighestKey(tx)
		if innerErr != nil {
			return fmt.Errorf("failed to get highest key: %w", innerErr)
		}
		highestKey = idx
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read highest key: %w", err)
	}
	if idx <= highestKey {
		return nil
	}

	// Convert the uint64 index into a byte slice.
	// We use BigEndian to ensure that the byte representation sorts lexicographically
	// in the same order as the numerical value of the index. This is how BoltDB
	// maintains key order.
	key := uint64tob(idx)

	// Start a read-write transaction to insert the new item.
	err = q.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(bucketName).Put(key, item); err != nil {
			return fmt.Errorf("failed to put item in bucket: %w", err)
		}

		// Record the highest key we've inserted so far.
		if err := q.setHighestKey(tx, idx); err != nil {
			return fmt.Errorf("failed to set highest key: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue item: %w", err)
	}

	if q.nextKey == nil {
		q.nextKey = key
	}

	// After successfully adding an item, we signal the condition variable.
	// This will wake up ONE goroutine that is waiting in a Dequeue() call.
	// If no goroutines are waiting, this signal is a no-op.
	q.cond.Signal()

	return nil
}

// Dequeue removes and returns the next available item from the queue.
// If the queue is empty, Dequeue blocks until an item is enqueued.
func (q *Queue) Dequeue() (uint64, []byte, error) {
	// The mutex is locked for the entire duration of the check-and-wait-or-delete logic.
	// This is the core of the coordination.
	q.mu.Lock()
	defer q.mu.Unlock()

	var retIdx uint64
	var retVal []byte
	var err error

	// This loop is the standard pattern for using sync.Cond.
	// It repeatedly checks the condition (is the queue non-empty?) until it's true.
	for {
		err = q.db.View(func(tx *bbolt.Tx) error {
			if q.nextKey == nil {
				return nil
			}

			c := tx.Bucket(bucketName).Cursor()
			k, v := c.Seek(q.nextKey)
			if k == nil {
				return fmt.Errorf("next key does not exist")
			}

			// Item found. Copy the key and value so we can use them after the
			// view transaction closes and before the update transaction starts.
			retIdx = btouint64(k)
			retVal = make([]byte, len(v))
			copy(retVal, v)

			// Now, move the nextKey to the next item in the queue.
			// If there is no next item, we set nextKey to nil.
			q.nextKey, _ = c.Next()
			return nil
		})

		if err != nil {
			// This indicates a DB error, not an empty queue.
			return 0, nil, fmt.Errorf("failed to check queue: %w", err)
		}

		if retIdx != 0 {
			// We fetched a "next" item from the queue.
			break
		}

		// There is no next item in the queue, so let's wait.
		// 1. Unlocks the mutex (q.mu).
		// 2. Puts the current goroutine to sleep.
		// 3. When woken up by q.cond.Signal(), it re-locks the mutex before returning.
		// This atomic unlock/wait/relock prevents the "lost wakeup" problem.
		q.cond.Wait()
	}
	return retIdx, retVal, nil
}

// DeleteRange deletes all items in the queue with indices less than or equal to idx.
func (q *Queue) DeleteRange(idx uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucketName)
		}

		key := uint64tob(idx)
		c := b.Cursor()
		for k, _ := c.Seek(key); k != nil; k, _ = c.Prev() {
			if err := b.Delete(k); err != nil {
				return fmt.Errorf("failed to delete key %s: %w", k, err)
			}
		}
		return nil
	})
}

// HighestKey returns the index of the highest item every inserted into the queue.
func (q *Queue) HighestKey() (uint64, error) {
	var highestKey uint64
	err := q.db.View(func(tx *bbolt.Tx) error {
		var err error
		highestKey, err = q.getHighestKey(tx)
		if err != nil {
			return fmt.Errorf("failed to get highest key: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to read highest key: %w", err)
	}
	return highestKey, nil
}

func (q *Queue) setHighestKey(tx *bbolt.Tx, idx uint64) error {
	key := uint64tob(idx)
	return tx.Bucket(metaBucketName).Put([]byte("max_key"), key)
}

func (q *Queue) getHighestKey(tx *bbolt.Tx) (uint64, error) {
	b := tx.Bucket(metaBucketName)
	if b == nil {
		return 0, fmt.Errorf("meta bucket not found")
	}

	key := b.Get([]byte("max_key"))
	if key == nil {
		return 0, fmt.Errorf("no max key found")
	}

	return btouint64(key), nil
}

func btouint64(b []byte) uint64 {
	if len(b) != 8 {
		panic(fmt.Sprintf("byte slice must be exactly 8 bytes long it is %d bytes long", len(b)))
	}
	return binary.BigEndian.Uint64(b)
}

func uint64tob(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}
