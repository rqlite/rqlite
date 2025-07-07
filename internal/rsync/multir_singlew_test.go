package rsync

import (
	"testing"
)

func Test_MultiRSW(t *testing.T) {
	r := NewMultiRSW()

	// Test successful read lock
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	r.EndRead()

	// Test successful write lock
	if err := r.BeginWrite(); err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}
	r.EndWrite()

	// Test that a write blocks other writers and readers.
	err := r.BeginWrite()
	if err != nil {
		t.Fatalf("Failed to acquire write lock in goroutine: %v", err)
	}
	if err := r.BeginRead(); err == nil {
		t.Fatalf("Expected error when reading during active write, got none")
	}
	if err := r.BeginWrite(); err == nil {
		t.Fatalf("Expected error when writing during active write, got none")
	}
	r.EndWrite()

	// Test that a read blocks a writer.
	err = r.BeginRead()
	if err != nil {
		t.Fatalf("Failed to acquire read lock in goroutine: %v", err)
	}
	if err := r.BeginWrite(); err == nil {
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
	if err := r.UpgradeToWriter(); err != nil {
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
	if err := r.UpgradeToWriter(); err == nil {
		t.Fatalf("Expected error when upgrading with multiple readers, got none")
	}
	r.EndRead()
	r.EndRead()

	// Double-upgrade should fail
	if err := r.BeginRead(); err != nil {
		t.Fatalf("Failed to acquire read lock: %v", err)
	}
	if err := r.UpgradeToWriter(); err != nil {
		t.Fatalf("Failed to upgrade to write lock: %v", err)
	}
	if err := r.UpgradeToWriter(); err == nil {
		t.Fatalf("Expected error when double-ugrading, got none")
	}
	r.EndWrite()

	// Test that upgrades are blocked by other writers
	if err := r.BeginWrite(); err != nil {
		t.Fatalf("Failed to acquire write lock: %v", err)
	}
	if err := r.UpgradeToWriter(); err == nil {
		t.Fatalf("Expected error when upgrading with an active writer, got none")
	}
	r.EndWrite()
}
