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

// run serializes access and streams items to q.eventsChan.
func (q *Queue) run(highestKey uint64) {
	defer q.wg.Done()
	defer q.db.Close()
	defer close(q.eventsChan)

	var (
		nextEv *Event      // buffered head event (nil if none)
		outCh  chan *Event // toggled: q.eventsChan when nextEv != nil, else nil
	)

	// loadHead buffers the current head (oldest k/v) into nextEv.
	loadHead := func() error {
		if nextEv != nil {
			outCh = q.eventsChan
			return nil
		}
		return q.db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(bucketName).Cursor()
			k, v := c.First()
			if k == nil {
				nextEv = nil
				outCh = nil // Prevents a stream of nils to the queue consumer.
				return nil
			}
			e := &Event{Index: btouint64(k)}
			e.Data = make([]byte, len(v))
			copy(e.Data, v)
			nextEv = e
			outCh = q.eventsChan
			return nil
		})
	}

	// advanceHead moves the buffer to the next record after nextEv.Index (or clears if none).
	advanceHead := func() {
		cur := uint64tob(nextEv.Index)
		nextEv = nil
		outCh = nil
		_ = q.db.View(func(tx *bbolt.Tx) error {
			c := tx.Bucket(bucketName).Cursor()
			// Seek to current key; then take the next.
			c.Seek(cur)
			if nk, nv := c.Next(); nk != nil {
				e := &Event{Index: btouint64(nk)}
				e.Data = make([]byte, len(nv))
				copy(e.Data, nv)
				nextEv = e
				outCh = q.eventsChan
			}
			return nil
		})
	}

	// Prime from disk if there are persisted items.
	_ = loadHead()

	for {
		select {
		case outCh <- nextEv:
			advanceHead()

		// Persist a new item. If no head is buffered, load it so the send path arms.
		case req := <-q.enqueueChan:
			if req.idx <= highestKey {
				req.respChan <- enqueueResp{err: nil}
				continue
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
			req.respChan <- enqueueResp{err: err}
			if err == nil && nextEv == nil {
				_ = loadHead()
			}

		// Delete all <= idx. If that wiped the current head, clear and reload.
		case req := <-q.deleteRangeChan:
			var deletedHead bool
			err := q.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(bucketName)
				c := b.Cursor()

				// Detect if current buffered head (if any) will be deleted.
				if nextEv != nil && nextEv.Index <= req.idx {
					deletedHead = true
				}

				for k, _ := c.First(); k != nil && btouint64(k) <= req.idx; k, _ = c.Next() {
					if derr := b.Delete(k); derr != nil {
						return derr
					}
				}
				return nil
			})
			req.respChan <- err

			if err == nil {
				if deletedHead {
					// Clear stale buffer and reload the (new) head.
					nextEv = nil
					outCh = nil
				}
				_ = loadHead()
			}

		case req := <-q.queryChan:
			var (
				isEmpty bool
				l       int
				err     error
			)
			err = q.db.View(func(tx *bbolt.Tx) error {
				st := tx.Bucket(bucketName).Stats()
				l = st.KeyN
				isEmpty = (l == 0)
				return nil
			})
			req.respChan <- queryResp{
				err:        err,
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

// Enqueue adds an item to the queue. Do not call Enqueue on a closed queue.
func (q *Queue) Enqueue(ev *Event) error {
	if ev == nil {
		return errors.New("event cannot be nil")
	}
	req := enqueueReq{idx: ev.Index, item: ev.Data, respChan: make(chan enqueueResp)}
	q.enqueueChan <- req
	resp := <-req.respChan
	return resp.err
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
	return resp.highestKey, resp.err
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

// Len returns the number of items currently in the queue.
func (q *Queue) Len() int {
	req := queryReq{respChan: make(chan queryResp)}
	q.queryChan <- req
	resp := <-req.respChan
	return resp.len
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
