package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

// bucketName is the name of the BoltDB bucket where queue items will be stored.
var bucketName = []byte("fifo_queue")

// metaBucketName is the name of the BoltDB bucket where metadata like highest key will be stored.
var metaBucketName = []byte("fifo_queue_meta")

// ErrQueueClosed is returned when an operation is attempted on a closed queue.
var ErrQueueClosed = errors.New("queue is closed")

var queueBufferSize = 100 // Size of the buffered channels for enqueue requests

// Event represents a dequeued item from the queue with its index and data.
type Event struct {
	Index uint64
	Data  []byte
}

// Queue is a persistent, disk-backed FIFO queue managed by a single goroutine.
//
// It is safe for concurrent use. It has some particular properties that make it
// suitable for the CDC service.
//   - The queue is persistent and can be used to recover from crashes or restarts.
//   - The queue will emit items as events on the Events channel.
//   - Reading an item from the Events channel does not remove it from the queue.
//     Only when DeleteRange is called will items be removed from the queue. This
//     allows the CDC service to explicitly delete items only when it is sure they
//     have been successfully transmitted.
//   - The queue remembers -- even after restarts -- the highest index of any item ever
//     enqueued. Since this queue is to be used to store changes associated with Raft
//     log entries, once a given index has been written to the queue any further
//     attempts to enqueue an item with that index will be ignored because those
//     repeated enqueue attempts contain identical information as the original.
type Queue struct {
	db *bbolt.DB

	// Channels for communicating with the managing goroutine
	enqueueChan     chan enqueueReq
	deleteRangeChan chan deleteRangeReq
	queryChan       chan queryReq
	done            chan struct{}

	// C is the channel for consuming queue events.
	C <-chan *Event

	// eventsChan is the write-side of the C channel.
	eventsChan chan *Event

	wg sync.WaitGroup
}

// NewQueue creates or opens a new persistent queue at the given file path.
func NewQueue(path string) (*Queue, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: time.Second, NoFreelistSync: true})
	if err != nil {
		return nil, fmt.Errorf("failed to open boltdb: %w", err)
	}

	// Prepare the database buckets in a single transaction.
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
		highestKey, innerErr = getHighestKey(tx)
		if innerErr != nil {
			return fmt.Errorf("failed to get highest key: %w", innerErr)
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	eventsChan := make(chan *Event)
	q := &Queue{
		db:              db,
		enqueueChan:     make(chan enqueueReq, queueBufferSize),
		deleteRangeChan: make(chan deleteRangeReq),
		queryChan:       make(chan queryReq),
		done:            make(chan struct{}),
		eventsChan:      eventsChan,
		C:               eventsChan,
	}

	q.wg.Add(1)
	go q.run(highestKey)
	return q, nil
}

// Close gracefully shuts down the queue, ensuring all pending operations are finished.
func (q *Queue) Close() {
	select {
	case <-q.done:
		// Already closed
		return
	default:
		close(q.done)
	}
	q.wg.Wait()
}

// Enqueue adds an item to the queue. Do not call Enqueue on a closed queue.
func (q *Queue) Enqueue(ev *Event) error {
	if ev == nil {
		return errors.New("event cannot be nil")
	}

	select {
	case <-q.done:
		return ErrQueueClosed
	default:
	}

	req := enqueueReq{idx: ev.Index, item: ev.Data, respChan: make(chan enqueueResp)}
	q.enqueueChan <- req
	resp := <-req.respChan
	return resp.err
}

// DeleteRange deletes all items in the queue with indices less than or equal to idx.
func (q *Queue) DeleteRange(idx uint64) error {
	select {
	case <-q.done:
		return ErrQueueClosed
	default:
	}

	req := deleteRangeReq{
		idx:      idx,
		respChan: make(chan error),
	}
	q.deleteRangeChan <- req
	return <-req.respChan
}

func (q *Queue) FirstKey() (uint64, error) {
	select {
	case <-q.done:
		return 0, ErrQueueClosed
	default:
	}
	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.firstKey, resp.err
}

// HighestKey returns the index of the highest item ever inserted into the queue.
func (q *Queue) HighestKey() (uint64, error) {
	select {
	case <-q.done:
		return 0, ErrQueueClosed
	default:
	}
	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.highestKey, resp.err
}

// Empty checks if the queue contains no items.
func (q *Queue) Empty() (bool, error) {
	select {
	case <-q.done:
		return false, ErrQueueClosed
	default:
	}

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

// Len returns the number of items currently in the queue.
func (q *Queue) Len() int {
	select {
	case <-q.done:
		return 0
	default:
	}

	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.len
}

func (q *Queue) run(highestKey uint64) {
	defer q.wg.Done()
	defer q.db.Close()
	defer close(q.eventsChan)

	var (
		nextEv   *Event
		outCh    chan *Event
		nextFrom uint64 // next index to emit
	)

	loadHead := func() error {
		if nextEv != nil {
			outCh = q.eventsChan
			return nil
		}
		return q.db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(bucketName).Cursor()
			var k, v []byte
			k, v = c.Seek(uint64tob(nextFrom))
			if k == nil {
				nextEv = nil
				outCh = nil
				return nil
			}
			e := &Event{Index: btouint64(k)}
			e.Data = append([]byte(nil), v...)
			nextEv = e
			outCh = q.eventsChan
			return nil
		})
	}

	advanceHead := func() error {
		// We just emitted nextEv; define the next read position.
		nextFrom = nextEv.Index + 1

		nextEv = nil
		outCh = nil
		return q.db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(bucketName).Cursor()
			k, v := c.Seek(uint64tob(nextFrom))
			if k != nil {
				e := &Event{Index: btouint64(k)}
				e.Data = append([]byte(nil), v...)
				nextEv = e
				outCh = q.eventsChan
			}
			return nil
		})
	}

	if err := loadHead(); err != nil {
		panic(fmt.Sprintf("failed to load initial head: %v", err))
	}

	for {
		select {
		case outCh <- nextEv:
			if err := advanceHead(); err != nil {
				panic(fmt.Sprintf("failed to advance head: %v", err))
			}

		case req := <-q.enqueueChan:
			if req.idx <= highestKey {
				req.respChan <- enqueueResp{err: nil}
				stats.Add(numFIFOIgnored, 1)
				continue
			}
			key := uint64tob(req.idx)
			err := q.db.Update(func(tx *bbolt.Tx) error {
				if err := tx.Bucket(bucketName).Put(key, req.item); err != nil {
					return err
				}
				if req.idx > highestKey {
					if err := setHighestKey(tx, req.idx); err != nil {
						return err
					}
				}
				return nil
			})
			if err == nil && req.idx > highestKey {
				highestKey = req.idx
			}
			req.respChan <- enqueueResp{err: err}
			if err == nil && nextEv == nil {
				if err := loadHead(); err != nil {
					panic(fmt.Sprintf("failed to load head after enqueue: %v", err))
				}
			}

		case req := <-q.deleteRangeChan:
			var deletedHead bool
			err := q.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(bucketName)
				c := b.Cursor()

				if nextEv != nil && nextEv.Index <= req.idx {
					deletedHead = true
				}

				var keysToDelete [][]byte
				// First, collect all keys that need to be deleted.
				for k, _ := c.First(); k != nil && btouint64(k) <= req.idx; k, _ = c.Next() {
					// The key slice `k` is only valid during the transaction. We must copy it.
					keyCopy := make([]byte, len(k))
					copy(keyCopy, k)
					keysToDelete = append(keysToDelete, keyCopy)
				}

				// Now, delete the keys.
				for _, k := range keysToDelete {
					if derr := b.Delete(k); derr != nil {
						return derr
					}
				}
				return nil
			})
			// Ensure cursor moves past deleted range
			if err == nil && nextFrom != 0 && nextFrom <= req.idx {
				nextFrom = req.idx + 1
			}
			req.respChan <- err

			if err == nil {
				if deletedHead {
					nextEv = nil
					outCh = nil
				}
				if err := loadHead(); err != nil {
					panic(fmt.Sprintf("failed to load head after delete: %v", err))
				}
			}

		case req := <-q.queryChan:
			var isEmpty bool
			var l int
			firstKey := uint64(0)
			err := q.db.View(func(tx *bbolt.Tx) error {
				bucket := tx.Bucket(bucketName)
				st := bucket.Stats()
				l = st.KeyN
				isEmpty = (l == 0)
				if !isEmpty {
					c := bucket.Cursor()
					k, _ := c.First()
					if k != nil {
						firstKey = btouint64(k)
					}
				}
				return nil
			})
			req.respChan <- queryResp{
				err:        err,
				firstKey:   firstKey,
				hasNext:    nextEv != nil,
				isEmpty:    isEmpty,
				len:        l,
				highestKey: highestKey,
			}

		case <-q.done:
			return
		}
	}
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

type deleteRangeReq struct {
	idx      uint64
	respChan chan error
}

type queryReq struct {
	respChan chan queryResp
}

type queryResp struct {
	hasNext    bool
	firstKey   uint64
	isEmpty    bool
	len        int
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
