package rsync

import (
	"testing"
	"time"
)

func Test_MultiRSW(t *testing.T) {
	r := NewMultiRSW()

	// Test successful read lock
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	r.EndRead()

	// Test successful write lock
	if err := r.BeginWrite("owner1"); err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}
	r.EndWrite()

	// Test that a write blocks other writers and readers.
	err := r.BeginWrite("owner2")
	if err != nil {
		t.Fatalf("Failed to acquire write lock in goroutine: %v", err)
	}
	if err := r.BeginRead(); err == nil {
		t.Fatalf("Expected error when reading during active write, got none")
	}
	if err := r.BeginWrite("owner3"); err == nil {
		t.Fatalf("Expected error when writing during active write, got none")
	}
	r.EndWrite()

	// Test that a read blocks a writer.
	err = r.BeginRead()
	if err != nil {
		t.Fatalf("Failed to acquire read lock in goroutine: %v", err)
	}
	if err := r.BeginWrite("owner4"); err == nil {
		t.Fatalf("Expected error when writing during active read, got none")
	}
	r.EndRead()

	// Test that a reader doesn't block other readers.
	err = r.BeginRead()
	if err != nil {
		t.Fatalf("Failed to acquire read lock in goroutine: %v", err)
	}
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock in goroutine: %v", err)
	}
	r.EndRead()
	r.EndRead()
}

func Test_MultiRSW_Upgrade(t *testing.T) {
	r := NewMultiRSW()

	// Test successful read lock and upgrade
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	if err := r.UpgradeToWriter("owner11"); err != nil {
		t.Fatalf("Failed to upgrade to write lock: %v", err)
	}
	r.EndWrite()

	// Test that upgrades are blocked by other readers
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	if err := r.UpgradeToWriter("owner5"); err == nil {
		t.Fatalf("Expected error when upgrading with multiple readers, got none")
	}
	r.EndRead()
	r.EndRead()

	// Double-upgrade should fail
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	if err := r.UpgradeToWriter("owner6"); err != nil {
		t.Fatalf("Failed to upgrade to write lock: %v", err)
	}
	if err := r.UpgradeToWriter("owner7"); err == nil {
		t.Fatalf("Expected error when double-ugrading, got none")
	}
	r.EndWrite()

	// Test that upgrades are blocked by other writers
	if err := r.BeginWrite("owner8"); err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}
	if err := r.UpgradeToWriter("owner9"); err == nil {
		t.Fatalf("Expected error when upgrading with an active writer, got none")
	}
	r.EndWrite()
}

func Test_MultiRSW_BeginWriteBlocking_NoContention(t *testing.T) {
	r := NewMultiRSW()

	// Should acquire immediately when nothing is active.
	r.BeginWriteBlocking("owner1")
	r.EndWrite()
}

func Test_MultiRSW_BeginWriteBlocking_WaitsForReaders(t *testing.T) {
	r := NewMultiRSW()

	// Acquire two read locks.
	if err := r.BeginRead(); err != nil {
		t.Fatalf("BeginRead failed: %v", err)
	}
	if err := r.BeginRead(); err != nil {
		t.Fatalf("BeginRead failed: %v", err)
	}

	// BeginWriteBlocking should block until readers drain.
	acquired := make(chan struct{})
	go func() {
		r.BeginWriteBlocking("owner1")
		close(acquired)
	}()

	// Should not have acquired yet.
	select {
	case <-acquired:
		t.Fatal("Write acquired while readers active")
	case <-time.After(100 * time.Millisecond):
	}

	// Release one reader — still blocked.
	r.EndRead()
	select {
	case <-acquired:
		t.Fatal("Write acquired with 1 reader remaining")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the last reader — should acquire.
	r.EndRead()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("BeginWriteBlocking did not acquire after all readers exited")
	}
	r.EndWrite()
}

func Test_MultiRSW_BeginWriteBlocking_WaitsForWriter(t *testing.T) {
	r := NewMultiRSW()

	if err := r.BeginWrite("owner1"); err != nil {
		t.Fatalf("BeginWrite failed: %v", err)
	}

	acquired := make(chan struct{})
	go func() {
		r.BeginWriteBlocking("owner2")
		close(acquired)
	}()

	// Should not have acquired yet.
	select {
	case <-acquired:
		t.Fatal("Write acquired while another writer active")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the writer — blocking call should acquire.
	r.EndWrite()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("BeginWriteBlocking did not acquire after writer released")
	}
	r.EndWrite()
}

