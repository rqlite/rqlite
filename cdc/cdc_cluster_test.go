package cdc

import (
	"testing"

	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/store"
)

func Test_NewCDCCluster(t *testing.T) {
	// Create mock instances - we just need to verify the object is created correctly
	// The actual functionality is tested through integration with the existing systems
	var mockStore *store.Store = nil       // In practice, this would be a real store instance
	var mockCluster *cluster.Service = nil // In practice, this would be a real cluster service
	var mockClient *cluster.Client = nil   // In practice, this would be a real cluster client

	// Test that NewCDCCluster returns a non-nil object
	cdcCluster := NewCDCCluster(mockStore, mockCluster, mockClient)

	if cdcCluster == nil {
		t.Fatalf("NewCDCCluster returned nil")
	}

	// Verify that the fields are set correctly
	if cdcCluster.store != mockStore {
		t.Errorf("store field not set correctly")
	}
	if cdcCluster.clstr != mockCluster {
		t.Errorf("clstr field not set correctly")
	}
	if cdcCluster.client != mockClient {
		t.Errorf("client field not set correctly")
	}
}
