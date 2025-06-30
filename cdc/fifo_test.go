package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// newTestQueue is a helper function that creates a new Queue for testing.
// It creates a temporary file for the bbolt database and returns the Queue,
// the path to the database file, and a cleanup function to be called with defer.
func newTestQueue(t *testing.T) (*Queue, func()) {
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

	return q, cleanup
}

// TestNewQueue tests the creation of a new queue and ensures the DB file exists.
func TestNewQueue(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
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
		t.Errorf("Database file was not created at %s", dbPath)
	}
}

// TestEnqueueAndFirst tests the basic Enqueue and First operations.
func TestEnqueueAndFirst(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
	defer cleanup()

	// 1. Test First on an empty queue
	_, _, err := q.First()
	if !errors.Is(err, ErrQueueEmpty) {
		t.Errorf("Expected ErrQueueEmpty for First() on empty queue, got %v", err)
	}

	// 2. Enqueue a single item and check First
	item1 := []byte("hello world")
	idx1 := uint64(10)
	if err := q.Enqueue(idx1, item1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	firstIdx, firstItem, err := q.First()
	if err != nil {
		t.Fatalf("First() failed after enqueue: %v", err)
	}
	if firstIdx != idx1 {
		t.Errorf("Expected first index to be %d, got %d", idx1, firstIdx)
	}
	if !bytes.Equal(firstItem, item1) {
		t.Errorf("Expected first item to be '%s', got '%s'", item1, firstItem)
	}

	// 3. Enqueue another item with a lower index (higher priority)
	item2 := []byte("first in line")
	idx2 := uint64(5)
	if err := q.Enqueue(idx2, item2); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	firstIdx, firstItem, err = q.First()
	if err != nil {
		t.Fatalf("First() failed after second enqueue: %v", err)
	}
	if firstIdx != idx2 {
		t.Errorf("Expected first index to be %d, got %d", idx2, firstIdx)
	}
	if !bytes.Equal(firstItem, item2) {
		t.Errorf("Expected first item to be '%s', got '%s'", item2, firstItem)
	}
}

// TestDequeueOrder verifies that items are dequeued in the correct index order.
func TestDequeueOrder(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
	defer cleanup()

	items := []struct {
		idx  uint64
		data []byte
	}{
		{20, []byte("item C")},
		{5, []byte("item A")},
		{10, []byte("item B")},
	}

	for _, item := range items {
		if err := q.Enqueue(item.idx, item.data); err != nil {
			t.Fatalf("Enqueue failed for index %d: %v", item.idx, err)
		}
	}

	expectedOrder := [][]byte{
		[]byte("item A"),
		[]byte("item B"),
		[]byte("item C"),
	}

	for i, expected := range expectedOrder {
		dequeued, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue failed at step %d: %v", i, err)
		}
		if !bytes.Equal(dequeued, expected) {
			t.Errorf("Step %d: Expected to dequeue '%s', got '%s'", i, expected, dequeued)
		}
	}

	// After dequeuing all items, the queue should be empty.
	_, _, err := q.First()
	if !errors.Is(err, ErrQueueEmpty) {
		t.Errorf("Expected queue to be empty after dequeuing all items, but First() returned err %v", err)
	}
}

// TestDelete tests the deletion of a specific item from the queue.
func TestDelete(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
	defer cleanup()

	// 1. Enqueue several items.
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

	// 2. Delete the middle item.
	idxToDelete := uint64(2)
	if err := q.Delete(idxToDelete); err != nil {
		t.Fatalf("Delete failed for index %d: %v", idxToDelete, err)
	}

	// 3. Dequeue the remaining items and verify the order.
	expectedOrder := [][]byte{
		[]byte("one"),
		[]byte("three"),
	}

	for i, expected := range expectedOrder {
		dequeued, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue failed at step %d after delete: %v", i, err)
		}
		if !bytes.Equal(dequeued, expected) {
			t.Errorf("Step %d: Expected '%s', got '%s'", i, expected, dequeued)
		}
	}

	// 4. Verify the queue is now empty.
	_, _, err := q.First()
	if !errors.Is(err, ErrQueueEmpty) {
		t.Errorf("Expected queue to be empty, but First() returned err %v", err)
	}

	// 5. Test deleting a non-existent item (should be a no-op and not error).
	err = q.Delete(999)
	if err != nil {
		t.Errorf("Deleting a non-existent item should not produce an error, got: %v", err)
	}
}

// TestDeleteRange tests deleting a range of items.
func TestDeleteRange(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
	defer cleanup()

	// 1. Populate the queue with 10 items, indices 1-10.
	for i := 1; i <= 10; i++ {
		item := []byte(fmt.Sprintf("item-%d", i))
		if err := q.Enqueue(uint64(i), item); err != nil {
			t.Fatalf("Failed to enqueue item %d", i)
		}
	}

	// 2. Delete a range from the middle (inclusive).
	// We will delete items 4, 5, 6, and 7.
	if err := q.DeleteRange(4, 7); err != nil {
		t.Fatalf("DeleteRange(4, 7) failed: %v", err)
	}

	// 3. Verify the remaining items are correct.
	expectedItems := [][]byte{
		[]byte("item-1"),
		[]byte("item-2"),
		[]byte("item-3"),
		[]byte("item-8"),
		[]byte("item-9"),
		[]byte("item-10"),
	}

	var remainingItems [][]byte
	for {
		item, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue failed unexpectedly: %v", err)
		}
		remainingItems = append(remainingItems, item)

		// Check if queue is empty to break loop.
		_, _, err = q.First()
		if errors.Is(err, ErrQueueEmpty) {
			break
		}
	}

	if !reflect.DeepEqual(remainingItems, expectedItems) {
		t.Errorf("Remaining items are not correct.\nGot:      %s\nExpected: %s", remainingItems, expectedItems)
	}

	// 4. Test edge cases.
	t.Run("DeleteInvalidRange", func(t *testing.T) {
		q, cleanup := newTestQueue(t)
		defer cleanup()
		q.Enqueue(1, []byte("a"))
		// upper < lower should be a no-op
		if err := q.DeleteRange(10, 1); err != nil {
			t.Errorf("DeleteRange with invalid range should not error, got %v", err)
		}
		// Queue should still contain the item
		_, _, err := q.First()
		if err != nil {
			t.Errorf("Queue should not be empty after invalid DeleteRange, got err: %v", err)
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		q, cleanup := newTestQueue(t)
		defer cleanup()
		for i := 1; i <= 5; i++ {
			q.Enqueue(uint64(i), []byte(fmt.Sprintf("item-%d", i)))
		}
		// Delete everything
		if err := q.DeleteRange(0, 100); err != nil {
			t.Errorf("DeleteRange covering all items failed: %v", err)
		}
		_, _, err := q.First()
		if !errors.Is(err, ErrQueueEmpty) {
			t.Errorf("Queue should be empty after deleting all items, got err: %v", err)
		}
	})
}

// TestDequeueBlocking tests that Dequeue blocks when the queue is empty
// and unblocks when an item is added.
func TestDequeueBlocking(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
	defer cleanup()

	item := []byte("unblock me")
	itemChan := make(chan []byte)

	// This goroutine will call Dequeue and block until an item is available.
	go func() {
		dequeuedItem, err := q.Dequeue()
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

// TestPersistence ensures that items remain in the queue after closing and reopening it.
func TestPersistence(t *testing.T) {
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
	dequeuedItem, err := q2.Dequeue()
	if err != nil {
		t.Fatalf("Failed to dequeue from reopened queue: %v", err)
	}
	if !bytes.Equal(dequeuedItem, item) {
		t.Errorf("Expected item '%s' after reopening, got '%s'", item, dequeuedItem)
	}
}

// TestConcurrency tests concurrent Enqueue and Dequeue operations.
func TestConcurrency(t *testing.T) {
	t.Parallel()
	q, cleanup := newTestQueue(t)
	defer cleanup()

	numItems := 100
	var wg sync.WaitGroup

	// Slice to store enqueued items for later verification.
	// We use a map to avoid duplicates and handle the non-deterministic
	// order of concurrent writes.
	enqueuedItems := make(map[string]bool)
	var enqueuedMutex sync.Mutex

	// Start goroutines to enqueue items concurrently.
	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			item := []byte(fmt.Sprintf("concurrent_item_%d", i))
			idx := uint64(i)
			if err := q.Enqueue(idx, item); err != nil {
				t.Errorf("Concurrent enqueue failed: %v", err)
				return
			}
			enqueuedMutex.Lock()
			enqueuedItems[string(item)] = true
			enqueuedMutex.Unlock()
		}(i)
	}

	// Slice to store dequeued items.
	dequeuedItems := make([][]byte, 0, numItems)
	var dequeuedMutex sync.Mutex

	// Start goroutines to dequeue items concurrently.
	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			item, err := q.Dequeue()
			if err != nil {
				t.Errorf("Concurrent dequeue failed: %v", err)
				return
			}
			dequeuedMutex.Lock()
			dequeuedItems = append(dequeuedItems, item)
			dequeuedMutex.Unlock()
		}()
	}

	wg.Wait()

	if len(dequeuedItems) != numItems {
		t.Fatalf("Expected %d dequeued items, but got %d", numItems, len(dequeuedItems))
	}

	// Verify that every enqueued item was dequeued exactly once.
	dequeuedMap := make(map[string]bool)
	for _, item := range dequeuedItems {
		dequeuedMap[string(item)] = true
	}

	// Convert map keys to slice for comparison
	var enqueuedKeys []string
	for k := range enqueuedItems {
		enqueuedKeys = append(enqueuedKeys, k)
	}

	var dequeuedKeys []string
	for k := range dequeuedMap {
		dequeuedKeys = append(dequeuedKeys, k)
	}

	sort.Strings(enqueuedKeys)
	sort.Strings(dequeuedKeys)

	if !reflect.DeepEqual(enqueuedKeys, dequeuedKeys) {
		t.Errorf("The set of enqueued items does not match the set of dequeued items")
	}
}
