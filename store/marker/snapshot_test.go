package marker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Test_Snapshot_Dir verifies that the Dir method returns the correct directory.
func Test_Snapshot_Dir(t *testing.T) {
	dir := "/tmp/snapshot_dir"
	sf := NewSnapshot(dir)
	if got := sf.Dir(); got != dir {
		t.Errorf("Dir() = %s; want %s", got, dir)
	}
}

// Test_Snapshot_MarkStartedAndIsStarted tests marking a snapshot as started and verifying it.
// Then test clearing it.
func Test_Snapshot_MarkStartedAndIsStarted(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Initially, IsStarted should return false.
	ok, _, _, err := sf.IsStarted()
	if err != nil {
		t.Fatalf("IsStarted() returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsStarted() = true; want false")
	}

	// Mark the snapshot as started.
	testTerm, testIndex := uint64(1), uint64(100)
	if err := sf.MarkStarted(testTerm, testIndex); err != nil {
		t.Fatalf("MarkStarted() returned error: %v", err)
	}

	// Now, IsStarted should return true with correct term and index.
	ok, term, index, err := sf.IsStarted()
	if err != nil {
		t.Fatalf("IsStarted() after MarkStarted returned error: %v", err)
	}
	if !ok {
		t.Fatalf("IsStarted() after MarkStarted = false; want true")
	}
	if term != testTerm || index != testIndex {
		t.Errorf("IsStarted() = (%d, %d); want (%d, %d)", term, index, testTerm, testIndex)
	}

	// Clear the started marker.
	if err := sf.ClearStarted(); err != nil {
		t.Fatalf("ClearStarted() returned error: %v", err)
	}

	// Now, IsStarted should return false.
	ok, _, _, err = sf.IsStarted()
	if err != nil {
		t.Fatalf("IsStarted() after ClearStarted returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsStarted() after ClearStarted = true; want false")
	}
}

// Test_Snapshot_MarkCheckpointedAndIsCheckpointed tests marking a snapshot as checkpointed
// and verifying it. Then test clearing it.
func Test_Snapshot_MarkCheckpointedAndIsCheckpointed(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Initially, IsCheckpointed should return false.
	ok, _, _, err := sf.IsCheckpointed()
	if err != nil {
		t.Fatalf("IsCheckpointed() returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsCheckpointed() = true; want false")
	}

	// Mark the snapshot as checkpointed.
	testTerm, testIndex := uint64(2), uint64(200)
	if err := sf.MarkCheckpointed(testTerm, testIndex); err != nil {
		t.Fatalf("MarkCheckpointed() returned error: %v", err)
	}

	// Now, IsCheckpointed should return true with correct term and index.
	ok, term, index, err := sf.IsCheckpointed()
	if err != nil {
		t.Fatalf("IsCheckpointed() after MarkCheckpointed returned error: %v", err)
	}
	if !ok {
		t.Fatalf("IsCheckpointed() after MarkCheckpointed = false; want true")
	}
	if term != testTerm || index != testIndex {
		t.Errorf("IsCheckpointed() = (%d, %d); want (%d, %d)", term, index, testTerm, testIndex)
	}

	// Clear the checkpointed marker.
	if err := sf.ClearCheckpointed(); err != nil {
		t.Fatalf("ClearCheckpointed() returned error: %v", err)
	}

	// Now, IsCheckpointed should return false.
	ok, _, _, err = sf.IsCheckpointed()
	if err != nil {
		t.Fatalf("IsCheckpointed() after ClearCheckpointed returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsCheckpointed() after ClearCheckpointed = true; want false")
	}
}

// Test_Snapshot_IsStarted_NoStartedFiles tests IsStarted when no started files are present.
func Test_Snapshot_IsStarted_NoStartedFiles(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Ensure IsStarted returns false when no SNAPSHOT_STARTED_ files exist.
	ok, _, _, err := sf.IsStarted()
	if err != nil {
		t.Fatalf("IsStarted() returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsStarted() = true; want false")
	}
}

// Test_Snapshot_IsCheckpointed_NoCheckpointedFiles tests IsCheckpointed when no checkpointed files are present.
func Test_Snapshot_IsCheckpointed_NoCheckpointedFiles(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Ensure IsCheckpointed returns false when no SNAPSHOT_CHECKPOINTED_ files exist.
	ok, _, _, err := sf.IsCheckpointed()
	if err != nil {
		t.Fatalf("IsCheckpointed() returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsCheckpointed() = true; want false")
	}
}

// Test_Snapshot_IsStarted_WithInvalidFiles ensures that invalid SNAPSHOT_STARTED_ files are handled gracefully.
func Test_Snapshot_IsStarted_WithInvalidFiles(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Create unrelated files.
	unrelatedFile := filepath.Join(tempDir, "unrelated.txt")
	if err := os.WriteFile(unrelatedFile, []byte("data"), 0644); err != nil {
		t.Fatalf("Failed to write unrelated file: %v", err)
	}

	// Create a SNAPSHOT_STARTED_ file with incorrect format.
	invalidStartedFile := filepath.Join(tempDir, "SNAPSHOT_STARTED_invalid")
	if err := os.WriteFile(invalidStartedFile, []byte("data"), 0644); err != nil {
		t.Fatalf("Failed to write invalid SNAPSHOT_STARTED_ file: %v", err)
	}

	// IsStarted should ignore the invalid file and return false.
	ok, _, _, err := sf.IsStarted()
	if err != nil {
		t.Fatalf("IsStarted() returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsStarted() = true; want false due to invalid file")
	}
}

// Test_Snapshot_IsCheckpointed_WithInvalidFiles ensures that invalid SNAPSHOT_CHECKPOINTED_ files are handled gracefully.
func Test_Snapshot_IsCheckpointed_WithInvalidFiles(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Create unrelated files.
	unrelatedFile := filepath.Join(tempDir, "unrelated.txt")
	if err := os.WriteFile(unrelatedFile, []byte("data"), 0644); err != nil {
		t.Fatalf("Failed to write unrelated file: %v", err)
	}

	// Create a SNAPSHOT_CHECKPOINTED_ file with incorrect format.
	invalidCheckpointedFile := filepath.Join(tempDir, "SNAPSHOT_CHECKPOINTED_invalid")
	if err := os.WriteFile(invalidCheckpointedFile, []byte("data"), 0644); err != nil {
		t.Fatalf("Failed to write invalid SNAPSHOT_CHECKPOINTED_ file: %v", err)
	}

	// IsCheckpointed should ignore the invalid file and return false.
	ok, _, _, err := sf.IsCheckpointed()
	if err != nil {
		t.Fatalf("IsCheckpointed() returned error: %v", err)
	}
	if ok {
		t.Fatalf("IsCheckpointed() = true; want false due to invalid file")
	}
}

// Test_Snapshot_MultipleStartedFiles ensures that multiple Checkpoint files
// results in an error.
func Test_Snapshot_MultipleStartedFiles(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	testCases := []struct {
		term  uint64
		index uint64
	}{
		{1, 100},
		{2, 200},
		{3, 300},
	}

	for _, tc := range testCases {
		filename := fmt.Sprintf("SNAPSHOT_MARK_STARTED_%d-%d", tc.term, tc.index)
		path := filepath.Join(tempDir, filename)
		if err := os.WriteFile(path, nil, 0644); err != nil {
			t.Fatalf("Failed to write SNAPSHOT_MARK_STARTED_ file %s: %v", filename, err)
		}
	}

	_, _, _, err := sf.IsStarted()
	if err == nil {
		t.Fatalf("IsStarted() expected to return error for multiple files, but got nil")
	}
}

// Test_Snapshot_MultipleCheckpointedFiles ensures that an error is returned when
// multiple SNAPSHOT_CHECKPOINTED_ files are present.
func Test_Snapshot_MultipleCheckpointedFiles(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Create multiple SNAPSHOT_CHECKPOINTED_ files.
	testCases := []struct {
		term  uint64
		index uint64
	}{
		{1, 100},
		{2, 200},
		{3, 300},
	}

	for _, tc := range testCases {
		filename := fmt.Sprintf("SNAPSHOT_MARK_CHECKPOINTED_%d-%d", tc.term, tc.index)
		path := filepath.Join(tempDir, filename)
		if err := os.WriteFile(path, nil, 0644); err != nil {
			t.Fatalf("Failed to write SNAPSHOT_MARK_CHECKPOINTED_ file %s: %v", filename, err)
		}
	}

	// IsCheckpointed should return the first valid SNAPSHOT_CHECKPOINTED_ file found.
	_, _, _, err := sf.IsCheckpointed()
	if err == nil {
		t.Fatalf("IsCheckpointed() expected to return error for multiple files, but got nil")
	}
}

// Test_Snapshot_ErrorHandling_SScanError tests behavior when file name parsing fails.
func Test_Snapshot_ErrorHandling_SScanError(t *testing.T) {
	tempDir := t.TempDir()
	sf := NewSnapshot(tempDir)

	// Create a SNAPSHOT_STARTED_ file with incomplete information.
	invalidStartedFile := filepath.Join(tempDir, "SNAPSHOT_MARK_STARTED_1")
	if err := os.WriteFile(invalidStartedFile, nil, 0644); err != nil {
		t.Fatalf("Failed to write invalid SNAPSHOT_STARTED_ file: %v", err)
	}

	_, _, _, err := sf.IsStarted()
	if err == nil {
		t.Fatalf("IsStarted() expected to return error due to invalid file name, but got nil")
	}

	// Similarly, create a SNAPSHOT_CHECKPOINTED_ file with incomplete information.
	invalidCheckpointedFile := filepath.Join(tempDir, "SNAPSHOT_MARK_CHECKPOINTED_2")
	if err := os.WriteFile(invalidCheckpointedFile, nil, 0644); err != nil {
		t.Fatalf("Failed to write invalid SNAPSHOT_MARK_CHECKPOINTED_ file: %v", err)
	}

	_, _, _, err = sf.IsCheckpointed()
	if err == nil {
		t.Fatalf("IsCheckpointed() expected to return error due to invalid file name, but got nil")
	}
}
