package cdc

import (
	"time"

	"github.com/rqlite/rqlite/v9/cluster"
	"github.com/rqlite/rqlite/v9/store"
)

// CDCCluster is a concrete implementation of the Cluster interface that bridges
// the CDC service with actual cluster components.
type CDCCluster struct {
	store  *store.Store
	clstr  *cluster.Service
	client *cluster.Client
}

// NewCDCCluster creates a new CDCCluster instance with the given store,
// cluster service, and cluster client.
func NewCDCCluster(str *store.Store, clstr *cluster.Service, client *cluster.Client) *CDCCluster {
	return &CDCCluster{
		store:  str,
		clstr:  clstr,
		client: client,
	}
}

// RegisterLeaderChange registers the given channel which will receive
// a signal when the node detects that the Leader changes.
func (c *CDCCluster) RegisterLeaderChange(ch chan<- bool) {
	c.store.RegisterLeaderChange(ch)
}

// RegisterSnapshotSync registers the given channel which will receive
// a channel when the node starts a snapshot. The snapshotting process
// will be blocked until the received channel is closed.
func (c *CDCCluster) RegisterSnapshotSync(ch chan<- chan struct{}) {
	c.store.RegisterSnapshotSync(ch)
}

// RegisterHWMUpdate registers a channel to receive highwater mark updates.
func (c *CDCCluster) RegisterHWMUpdate(ch chan<- uint64) {
	c.clstr.RegisterHWMUpdate(ch)
}

// BroadcastHighWatermark sets the high watermark across the voting cluster nodes.
func (c *CDCCluster) BroadcastHighWatermark(value uint64) error {
	servers, err := c.store.Nodes()
	if err != nil {
		return err
	}

	nodeAddrs := store.Servers(servers).Voters().Addrs()

	// For now, we'll use reasonable default parameters.
	// In a real implementation, these parameters might be configurable.
	const retries = 3
	const timeout = 5 * time.Second

	// Broadcast to all cluster nodes
	_, err = c.client.BroadcastHWM(value, retries, timeout, nodeAddrs...)
	return err
}
