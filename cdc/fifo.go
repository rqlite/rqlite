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

// metaBucketName is the name of the BoltDB bucket where metadata like highest key will be stored.
var metaBucketName = []byte("fifo_queue_meta")

// ErrQueueClosed is returned when an operation is attempted on a closed queue.
var ErrQueueClosed = errors.New("queue is closed")

var queueBufferSize = 100 // Size of the buffered channels for enqueue requests

// Queue is a persistent, disk-backed FIFO queue managed by a single goroutine.
// It is safe for concurrent use.
type Queue struct {
	db *bbolt.DB

	// Channels for communicating with the managing goroutine
	enqueueChan     chan enqueueReq
	dequeueChan     chan dequeueReq
	deleteRangeChan chan deleteRangeReq
	queryChan       chan queryReq
	done            chan struct{}

	wg sync.WaitGroup
}

// NewQueue creates or opens a new persistent queue at the given file path.
func NewQueue(path string) (*Queue, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1})
	if err != nil {
		return nil, fmt.Errorf("failed to open boltdb: %w", err)
	}

	// Prepare the database buckets in a single transaction.
	var nextKey []byte
	var highestKey uint64
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketName); err != nil {
			return fmt.Errorf("failed to create queue bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists(metaBucketName); err != nil {
			return fmt.Errorf("failed to create meta bucket: %w", err)
		}
		// Ensure the meta bucket has a "max_key" entry.
		metaBucket := tx.Bucket(metaBucketName)
		if metaBucket.Get([]byte("max_key")) == nil {
			if err := metaBucket.Put([]byte("max_key"), uint64tob(0)); err != nil {
				return fmt.Errorf("failed to initialize max_key: %w", err)
			}
		}

		// Initialize state from the DB.
		var innerErr error
		c := tx.Bucket(bucketName).Cursor()
		nextKey, _ = c.First()
		highestKey, innerErr = getHighestKey(tx)
		if innerErr != nil {
			return fmt.Errorf("failed to get highest key: %w", innerErr)
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	q := &Queue{
		db:              db,
		enqueueChan:     make(chan enqueueReq, queueBufferSize),
		dequeueChan:     make(chan dequeueReq),
		deleteRangeChan: make(chan deleteRangeReq),
		queryChan:       make(chan queryReq),
		done:            make(chan struct{}),
	}

	q.wg.Add(1)
	go q.run(nextKey, highestKey)
	return q, nil
}

// Close gracefully shuts down the queue, ensuring all pending operations are finished.
func (q *Queue) Close() {
	close(q.done)
	q.wg.Wait()
}

// run is a single goroutine that serializes all access to the database.
func (q *Queue) run(nextKey []byte, highestKey uint64) {
	defer q.wg.Done()
	defer q.db.Close()

	var waitingDequeues []dequeueReq // A list of callers waiting for an item.
	for {
		// If the queue is empty, we can't process a dequeue request.
		// So we only listen on the dequeueChan if there are items.
		var activeDequeueChan chan dequeueReq
		if len(waitingDequeues) > 0 || nextKey != nil {
			activeDequeueChan = q.dequeueChan
		}

		select {
		case req := <-q.enqueueChan:
			// No need to check highestKey if idx is 0
			if req.idx <= highestKey {
				req.respChan <- enqueueResp{err: nil}
				continue // Ignore duplicate/old items
			}

			key := uint64tob(req.idx)
			err := q.db.Update(func(tx *bbolt.Tx) error {
				if err := tx.Bucket(bucketName).Put(key, req.item); err != nil {
					return err
				}
				if req.idx > highestKey {
					highestKey = req.idx
					return setHighestKey(tx, highestKey)
				}
				return nil
			})
			if err != nil {
				req.respChan <- enqueueResp{err: fmt.Errorf("enqueue failed: %w", err)}
				continue
			}

			// If this is the first item added to a previously empty queue,
			// it becomes the next item to be dequeued.
			if nextKey == nil {
				nextKey = key
			}

			// Fulfill any waiting dequeue request immediately.
			if len(waitingDequeues) > 0 && nextKey != nil {
				waiter := waitingDequeues[0]
				waitingDequeues = waitingDequeues[1:] // Pop from waitlist

				var resp dequeueResp
				err := q.db.View(func(tx *bbolt.Tx) error {
					c := tx.Bucket(bucketName).Cursor()
					_, val := c.Seek(nextKey)
					if val == nil {
						return fmt.Errorf("item not found for key %x", nextKey)
					}
					resp.idx = btouint64(nextKey)
					resp.val = make([]byte, len(val))
					copy(resp.val, val)

					c.Seek(nextKey)
					nk, _ := c.Next()
					if nk != nil {
						copy(nextKey, nk)
					} else {
						nextKey = nil // No more items available
					}
					return nil
				})
				resp.err = err
				waiter.respChan <- resp
			}
			req.respChan <- enqueueResp{err: err}

		case req := <-activeDequeueChan:
			// If a request arrived but nextKey is nil it means there are no items available.
			if nextKey == nil {
				waitingDequeues = append(waitingDequeues, req)
				continue
			}

			var resp dequeueResp
			err := q.db.View(func(tx *bbolt.Tx) error {
				c := tx.Bucket(bucketName).Cursor()
				_, val := c.Seek(nextKey)
				if val == nil {
					return fmt.Errorf("item not found for key %x", nextKey)
				}

				resp.idx = btouint64(nextKey)
				resp.val = make([]byte, len(val))
				copy(resp.val, val)

				c.Seek(nextKey)
				nk, _ := c.Next()
				if nk != nil {
					copy(nextKey, nk)
				} else {
					nextKey = nil // No more items available
				}
				return nil
			})
			resp.err = err
			req.respChan <- resp

		case req := <-q.deleteRangeChan:
			err := q.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(bucketName)
				c := b.Cursor()

				// Seek to the key and iterate backwards, deleting items.
				for k, _ := c.Seek(uint64tob(req.idx)); k != nil; k, _ = c.Prev() {
					if err := b.Delete(k); err != nil {
						return err
					}
				}

				// Check if our cached 'nextKey' was deleted.
				if nextKey != nil && b.Get(nextKey) == nil {
					k, _ := c.First() // Find the new first key
					if k != nil {
						nextKey = make([]byte, len(k))
						copy(nextKey, k)
					} else {
						nextKey = nil
					}
				}
				return nil
			})
			req.respChan <- err

		case req := <-q.queryChan:
			var isEmpty bool
			err := q.db.View(func(tx *bbolt.Tx) error {
				c := tx.Bucket(bucketName).Cursor()
				k, _ := c.First()
				isEmpty = k == nil // If no items, isEmpty is true
				return nil
			})
			req.respChan <- queryResp{
				err:        err,
				hasNext:    nextKey != nil,
				isEmpty:    isEmpty,
				highestKey: highestKey,
			}

		case <-q.done:
			for _, waiter := range waitingDequeues {
				waiter.respChan <- dequeueResp{err: ErrQueueClosed}
			}
			return
		}
	}
}

// Enqueue adds an item to the queue.
func (q *Queue) Enqueue(idx uint64, item []byte) error {
	req := enqueueReq{idx: idx, item: item, respChan: make(chan enqueueResp)}
	q.enqueueChan <- req
	resp := <-req.respChan
	return resp.err
}

// Dequeue removes and returns the next available item from the queue.
// If the queue is empty, Dequeue blocks until an item is available.
func (q *Queue) Dequeue() (uint64, []byte, error) {
	req := dequeueReq{respChan: make(chan dequeueResp)}
	q.dequeueChan <- req
	resp := <-req.respChan
	return resp.idx, resp.val, resp.err
}

// DeleteRange deletes all items in the queue with indices less than or equal to idx.
func (q *Queue) DeleteRange(idx uint64) error {
	req := deleteRangeReq{
		idx:      idx,
		respChan: make(chan error),
	}
	q.deleteRangeChan <- req
	return <-req.respChan
}

// HighestKey returns the index of the highest item ever inserted into the queue.
func (q *Queue) HighestKey() (uint64, error) {
	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.highestKey, nil
}

// Empty checks if the queue contains no items.
func (q *Queue) Empty() (bool, error) {
	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.isEmpty, resp.err
}

// HasNext checks if there is at least one item available to dequeue.
func (q *Queue) HasNext() bool {
	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.hasNext
}

func getHighestKey(tx *bbolt.Tx) (uint64, error) {
	key := tx.Bucket(metaBucketName).Get([]byte("max_key"))
	if key == nil {
		return 0, fmt.Errorf("max_key not found")
	}
	return btouint64(key), nil
}

func setHighestKey(tx *bbolt.Tx, idx uint64) error {
	return tx.Bucket(metaBucketName).Put([]byte("max_key"), uint64tob(idx))
}

type enqueueReq struct {
	idx      uint64
	item     []byte
	respChan chan enqueueResp
}

type enqueueResp struct {
	err error
}

type dequeueReq struct {
	respChan chan dequeueResp
}

type dequeueResp struct {
	idx uint64
	val []byte
	err error
}

type deleteRangeReq struct {
	idx      uint64
	respChan chan error
}

type queryReq struct {
	respChan chan queryResp
}

type queryResp struct {
	hasNext    bool
	isEmpty    bool
	highestKey uint64
	err        error
}

func btouint64(b []byte) uint64 {
	if len(b) != 8 {
		panic(fmt.Sprintf("expected 8 bytes, got %d", len(b)))
	}
	return binary.BigEndian.Uint64(b)
}

func uint64tob(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}
