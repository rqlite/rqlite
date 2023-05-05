package backup

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSHA256SumString(t *testing.T) {
	tests := []struct {
		name     string
		input    SHA256Sum
		expected string
	}{
		{
			name:     "empty hash",
			input:    SHA256Sum([]byte{}),
			expected: "",
		},
		{
			name:     "non-empty hash",
			input:    SHA256Sum([]byte{0x12, 0x34, 0x56}),
			expected: "123456",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := tt.input.String()
			if output != tt.expected {
				t.Errorf("Expected: %s, got: %s", tt.expected, output)
			}
		})
	}
}

func TestSHA256SumEquals(t *testing.T) {
	tests := []struct {
		name     string
		input1   SHA256Sum
		input2   SHA256Sum
		expected bool
	}{
		{
			name:     "equal hashes",
			input1:   SHA256Sum([]byte{0x12, 0x34, 0x56}),
			input2:   SHA256Sum([]byte{0x12, 0x34, 0x56}),
			expected: true,
		},
		{
			name:     "unequal hashes",
			input1:   SHA256Sum([]byte{0x12, 0x34, 0x56}),
			input2:   SHA256Sum([]byte{0x12, 0x34, 0x57}),
			expected: false,
		},
		{
			name:     "unequal hashes with one being nil",
			input1:   SHA256Sum([]byte{0x12, 0x34, 0x56}),
			input2:   nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := tt.input1.Equals(tt.input2)
			if output != tt.expected {
				t.Errorf("Expected: %v, got: %v", tt.expected, output)
			}
		})
	}
}

func TestFileSha256(t *testing.T) {
	data := []byte("Test file content")
	tempFileName := mustWriteDataTempFile(data)
	defer os.Remove(tempFileName)

	// Calculate the SHA256 sum of the file contents using crypto/sha256 calls
	hasher := sha256.New()
	if _, err := io.Copy(hasher, bytes.NewReader(data)); err != nil {
		t.Fatalf("Error calculating hash with crypto/sha256: %v", err)
	}
	expectedHash := hasher.Sum(nil)

	// Call fileSha256 and check that it returns the same hash as the direct call
	hash, err := FileSHA256(tempFileName)
	if err != nil {
		t.Fatalf("Error calling fileSha256: %v", err)
	}

	if !hash.Equals(SHA256Sum(expectedHash)) {
		t.Errorf("Expected: %x, got: %s", expectedHash, hash)
	}
}

func mustWriteDataTempFile(data []byte) string {
	tempFile, err := ioutil.TempFile("", "uploader_test")
	if err != nil {
		panic("Error creating temp file: " + err.Error())
	}

	if _, err := tempFile.Write(data); err != nil {
		panic("Error writing to temp file: " + err.Error())
	}

	if err := tempFile.Close(); err != nil {
		panic("Error closing temp file: " + err.Error())
	}

	return tempFile.Name()
}
