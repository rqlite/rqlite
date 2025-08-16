package store

import (
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// CDCCluster is a struct that wraps a Store and implements the functionality
// needed by the CDC service.
type CDCCluster struct {
	str            *Store
	hwm            atomic.Uint64
	hwmObserversMu sync.RWMutex
	hwmObservers   []chan<- uint64
}

// NewCDCCluster creates a new CDCCluster instance.
func NewCDCCluster(s *Store) *CDCCluster {
	appendEntriesTxHandler := func(req *raft.AppendEntriesRequest) error {
		return nil
	}
	appendEntriesRxHandler := func(req *raft.AppendEntriesRequest) error {
		return nil
	}

	s.raftTn.SetAppendEntriesTxHandler(appendEntriesTxHandler)
	s.raftTn.SetAppendEntriesRxHandler(appendEntriesRxHandler)

	return &CDCCluster{
		str:          s,
		hwm:          atomic.Uint64{},
		hwmObservers: make([]chan<- uint64, 0),
	}
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
