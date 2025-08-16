package store

import (
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v8/command"
	"github.com/rqlite/rqlite/v8/command/proto"
)

// CDCCluster is a struct that wraps a Store and implements the functionality
// needed by the CDC service.
type CDCCluster struct {
	str            *Store
	hwm            atomic.Uint64
	prevHWM        atomic.Uint64
	hwmObserversMu sync.RWMutex
	hwmObservers   []chan<- uint64
}

// NewCDCCluster creates a new CDCCluster instance.
func NewCDCCluster(s *Store) *CDCCluster {
	c := &CDCCluster{
		str:          s,
		hwm:          atomic.Uint64{},
		hwmObservers: make([]chan<- uint64, 0),
	}

	s.raftTn.SetAppendEntriesTxHandler(c.appendEntriesTxHandler)
	s.raftTn.SetAppendEntriesRxHandler(c.appendEntriesRxHandler)

	return c
}

// SetHighWatermark sets the high watermark for the CDC service.
func (c *CDCCluster) SetHighWatermark(value uint64) error {
	c.hwm.Store(value)
	return nil
}

// RegisterLeaderChange registers a channel to receive notifications
// when the leader changes in the cluster.
func (c *CDCCluster) RegisterLeaderChange(ch chan<- bool) {
	c.str.RegisterLeaderChange(ch)
}

// RegisterHWMChange registers the given channel which will
// receive a notification when the high watermark changes.
func (c *CDCCluster) RegisterHWMChange(ch chan<- uint64) {
	c.hwmObserversMu.Lock()
	defer c.hwmObserversMu.Unlock()
	c.hwmObservers = append(c.hwmObservers, ch)
}

func (c *CDCCluster) appendEntriesTxHandler(req *raft.AppendEntriesRequest) (retErr error) {
	hwm := c.hwm.Load()
	if c.prevHWM.Load() == hwm {
		return nil
	}
	defer func() {
		if retErr == nil {
			c.prevHWM.Store(hwm)
		}
	}()

	ex := proto.AppendEntriesExtension{
		CdcHWM: hwm,
	}
	b, err := command.MarshalAppendEntriesExtension(&ex)
	if err != nil {
		return err
	}

	if len(req.Entries) > 0 {
		req.Entries[len(req.Entries)-1].Extensions = b
	} else {
		req.Entries = append(req.Entries, &raft.Log{
			Extensions: b,
		})
	}
	return nil
}

func (c *CDCCluster) appendEntriesRxHandler(req *raft.AppendEntriesRequest) error {
	if len(req.Entries) == 0 {
		return nil
	}

	// Only the last log will carry an extension.
	lastLog := req.Entries[len(req.Entries)-1]
	if lastLog.Extensions == nil {
		return nil
	}

	var ex proto.AppendEntriesExtension
	if err := command.UnmarshalAppendEntriesExtension(lastLog.Extensions, &ex); err != nil {
		return err
	}

	c.hwmObserversMu.RLock()
	for _, ch := range c.hwmObservers {
		select {
		case ch <- ex.CdcHWM:
		default:
			// Avoid blocking the AppendEntries handler.
		}
	}
	c.hwmObserversMu.RUnlock()
	return nil
}
