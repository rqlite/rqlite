package cdc

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Test_NewQueue tests the creation of a new queue and ensures the DB file exists.
func Test_NewQueue(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	if q == nil {
		t.Fatal("NewQueue returned a nil queue")
	}
	if q.db == nil {
		t.Fatal("Queue has a nil db instance")
	}

	// Verify the database file was created by checking its path.
	dbPath := q.db.Path()
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("Database file was not created at %s", dbPath)
	}

	// Ensure the queue is empty
	e, err := q.Empty()
	if err != nil {
		t.Fatalf("Queue Empty check failed: %v", err)
	}
	if !e {
		t.Fatal("Newly created queue should be empty")
	}
}

// Test_EnqueueDequeue tests the basic Enqueue and Dequeue operations.
func Test_EnqueueDequeue(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue a single item then dequeue it.
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(idx1, item1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Ensure the queue is not empty
	e, err := q.Empty()
	if err != nil {
		t.Fatalf("Queue Empty check failed: %v", err)
	}
	if e {
		t.Fatal("Queue should not be empty")
	}

	// Should be no items to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
	}

	gotIdx, gotItem, err := q.Dequeue()
	if err != nil {
		t.Fatalf("First() failed after enqueue: %v", err)
	}
	if gotIdx != idx1 {
		t.Errorf("Expected first index to be %d, got %d", idx1, gotIdx)
	}
	if !bytes.Equal(gotItem, item1) {
		t.Errorf("Expected first item to be '%s', got '%s'", item1, gotItem)
	}

	// check highest key
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != idx1 {
		t.Errorf("Expected highest key to be %d, got %d", idx1, hi)
	}

	// Next enqueue two items, and dequeue each, ensuring we get them in the
	// expected order.
	item2 := []byte("second item")
	idx2 := uint64(20)
	if err := q.Enqueue(idx2, item2); err != nil {
		t.Fatalf("Enqueue failed for second item: %v", err)
	}

	item3 := []byte("third item")
	idx3 := uint64(30)
	if err := q.Enqueue(idx3, item3); err != nil {
		t.Fatalf("Enqueue failed for third item: %v", err)
	}

	// Should be more items to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext should be true after enqueueing an item")
	}

	// Dequeue the second item.
	gotIdx, gotItem, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed for second item: %v", err)
	}
	if gotIdx != idx2 {
		t.Fatalf("Expected second index to be %d, got %d", idx2, gotIdx)
	}
	if !bytes.Equal(gotItem, item2) {
		t.Fatalf("Expected second item to be '%s', got '%s'", item2, gotItem)
	}
	// Dequeue the third item.
	gotIdx, gotItem, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed for third item: %v", err)
	}
	if gotIdx != idx3 {
		t.Fatalf("Expected third index to be %d, got %d", idx3, gotIdx)
	}
	if !bytes.Equal(gotItem, item3) {
		t.Fatalf("Expected third item to be '%s', got '%s'", item3, gotItem)
	}

	// check highest key
	hi, err = q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != idx3 {
		t.Errorf("Expected highest key to be %d, got %d", idx1, hi)
	}

	// Should be no items to dequeue.
	if q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
	}
}

func Test_EnqueueHighest(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Enqueue a single item
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(idx1, item1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Now insert an "older" item, it shouldn't actually be inserted.
	item2 := []byte("older item")
	idx2 := uint64(5)
	if err := q.Enqueue(idx2, item2); err != nil {
		t.Fatalf("Enqueue of older item failed: %v", err)
	}

	// Check that the highest key is still idx1.
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after enqueue: %v", err)
	}
	if hi != idx1 {
		t.Errorf("Expected highest key to be %d, got %d", idx1, hi)
	}
}

func Test_DeleteRange(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	// Test that deleting on a empty queue is a no-op.
	if err := q.DeleteRange(1); err != nil {
		t.Fatalf("DeleteRange on empty queue should not error: %v", err)
	}

	// Enqueue a few items.
	items := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("one")},
		{2, []byte("two")},
		{3, []byte("three")},
	}
	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Delete a range of items.
	if err := q.DeleteRange(2); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	// Should still have another item to dequeue.
	if !q.HasNext() {
		t.Fatalf("HasNext failed after DeleteRange")
	}

	// Dequeue next item, should be 3.
	gotIdx, gotItem, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed after DeleteRange: %v", err)
	}
	if gotIdx != 3 {
		t.Fatalf("Expected index 3 after DeleteRange, got %d", gotIdx)
	}
	if !bytes.Equal(gotItem, []byte("three")) {
		t.Fatalf("Expected item 'three' after DeleteRange, got '%s'", gotItem)
	}

	// Should be no more items to dequeue.
	if q.HasNext() {
		t.Fatalf("HasNext should be false after dequeuing last item")
	}
}

func Test_QueueHighestKey(t *testing.T) {
	q, path, _ := newTestQueue(t)

	// Enqueue a few items.
	items := []struct {
		idx  uint64
		data []byte
	}{
		{1, []byte("one")},
		{2, []byte("two")},
		{3, []byte("three")},
	}
	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	// Check the highest key.
	hi, err := q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed: %v", err)
	}
	if hi != 3 {
		t.Fatalf("Expected highest key to be 3, got %d", hi)
	}

	// Delete all keys, highest key should still be 3.
	if err := q.DeleteRange(3); err != nil {
		t.Fatalf("DeleteRange failed: %v", err)
	}
	hi, err = q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after DeleteRange: %v", err)
	}
	if hi != 3 {
		t.Fatalf("Expected highest key to still be 3 after DeleteRange, got %d", hi)
	}

	// Close and reopen queue, highest key should still be 3.
	if err := q.Close(); err != nil {
		t.Fatalf("Failed to close queue: %v", err)
	}

	q, err = NewQueue(path)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	hi, err = q.HighestKey()
	if err != nil {
		t.Fatalf("HighestKey failed after reopening queue: %v", err)
	}
	if hi != 3 {
		t.Fatalf("Expected highest key to still be 3 after reopening, got %d", hi)
	}

	// Free up database file so it can be removed.
	q.Close()
}

// Test_DequeueBlocking tests that Dequeue blocks when the queue is empty
// and unblocks when an item is added.
func Test_DequeueBlocking(t *testing.T) {
	q, _, cleanup := newTestQueue(t)
	defer cleanup()

	item := []byte("unblock me")
	itemChan := make(chan []byte)

	// This goroutine will call Dequeue and block until an item is available.
	go func() {
		_, dequeuedItem, err := q.Dequeue()
		if err != nil {
			t.Errorf("Dequeue in goroutine failed: %v", err)
			close(itemChan)
			return
		}
		itemChan <- dequeuedItem
	}()

	// Give the goroutine a moment to start and call Dequeue, which should block.
	time.Sleep(100 * time.Millisecond)

	// Now, enqueue an item. This should unblock the waiting goroutine.
	if err := q.Enqueue(1, item); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Wait for the dequeued item from the channel, with a timeout.
	select {
	case receivedItem := <-itemChan:
		if !bytes.Equal(receivedItem, item) {
			t.Errorf("Expected dequeued item to be '%s', got '%s'", item, receivedItem)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for Dequeue to unblock")
	}
}

// Test_QueuePersistence ensures that items remain in the queue after closing and reopening it.
func Test_QueuePersistence(t *testing.T) {
	// This test cannot run in parallel because it relies on a specific file path.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist_test.db")

	item := []byte("survivor")
	idx := uint64(42)

	// 1. Create a queue, add an item, and close it.
	q1, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to create initial queue: %v", err)
	}
	if err := q1.Enqueue(idx, item); err != nil {
		t.Fatalf("Failed to enqueue item: %v", err)
	}
	if err := q1.Close(); err != nil {
		t.Fatalf("Failed to close initial queue: %v", err)
	}

	// 2. Reopen the queue from the same file.
	q2, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()

	// 3. Dequeue the item and verify it's the one we saved.
	_, dequeuedItem, err := q2.Dequeue()
	if err != nil {
		t.Fatalf("Failed to dequeue from reopened queue: %v", err)
	}
	if !bytes.Equal(dequeuedItem, item) {
		t.Errorf("Expected item '%s' after reopening, got '%s'", item, dequeuedItem)
	}
}

// newTestQueue is a helper function that creates a new Queue for testing.
// It creates a temporary file for the bbolt database and returns the Queue,
// the path to the database file, and a cleanup function to be called with defer.
func newTestQueue(t *testing.T) (*Queue, string, func()) {
	t.Helper()

	// Create a temporary file for the test database.
	// Using a subdirectory helps keep the project root clean.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	q, err := NewQueue(dbPath)
	if err != nil {
		t.Fatalf("Failed to create new queue: %v", err)
	}

	// The cleanup function closes the queue and removes the database file.
	cleanup := func() {
		q.Close()
	}

	return q, dbPath, cleanup
}
