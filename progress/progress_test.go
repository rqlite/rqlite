package progress

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
)

func TestCountingReader_Read(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		readBufferSize int
		wantCount      int64
	}{
		{
			name:           "Read all at once",
			input:          "Hello, world!",
			readBufferSize: 13, // Size of "Hello, world!"
			wantCount:      13,
		},
		{
			name:           "Read in small chunks",
			input:          "Hello, world!",
			readBufferSize: 2, // Read in chunks of 2 bytes
			wantCount:      13,
		},
		{
			name:           "Empty input",
			input:          "",
			readBufferSize: 10,
			wantCount:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			countingReader := NewCountingReader(reader)

			buf := make([]byte, tt.readBufferSize)
			var totalRead int64
			for {
				n, err := countingReader.Read(buf)
				totalRead += int64(n)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Read() error = %v", err)
				}
			}

			if totalRead != tt.wantCount {
				t.Fatalf("Total bytes read = %v, want %v", totalRead, tt.wantCount)
			}

			if got := countingReader.Count(); got != tt.wantCount {
				t.Fatalf("CountingReader.Count() = %v, want %v", got, tt.wantCount)
			}
		})
	}
}

func TestCountingReader_ConcurrentReads(t *testing.T) {
	input := "Concurrent reading test string"
	reader := strings.NewReader(input)
	countingReader := NewCountingReader(reader)

	var wg sync.WaitGroup
	readFunc := func() {
		defer wg.Done()
		buf := make([]byte, 5) // Read in chunks of 5 bytes
		for {
			if _, err := countingReader.Read(buf); err == io.EOF {
				break
			} else if err != nil {
				t.Errorf("Read() error = %v", err)
			}
		}
	}

	// Simulate concurrent reads
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go readFunc()
	}
	wg.Wait()

	wantCount := int64(len(input))
	if got := countingReader.Count(); got != wantCount {
		t.Fatalf("CountingReader.Count() after concurrent reads = %v, want %v", got, wantCount)
	}
}

func TestCountingWriter_Write(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantCount int64
	}{
		{
			name:      "Write string",
			input:     "Hello, world!",
			wantCount: 13, // Length of "Hello, world!"
		},
		{
			name:      "Write empty string",
			input:     "",
			wantCount: 0,
		},
		{
			name:      "Write long string",
			input:     "This is a longer test string",
			wantCount: 28, // Length of the string
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			countingWriter := NewCountingWriter(&buf)

			n, err := countingWriter.Write([]byte(tt.input))
			if err != nil {
				t.Fatalf("Write() error = %v", err)
			}
			if int64(n) != tt.wantCount {
				t.Errorf("Written bytes = %v, want %v", n, tt.wantCount)
			}

			if got := countingWriter.Count(); got != tt.wantCount {
				t.Errorf("CountingWriter.Count() = %v, want %v", got, tt.wantCount)
			}
		})
	}
}

func TestCountingWriter_ConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	countingWriter := NewCountingWriter(&buf)
	input := "Concurrent write test string"
	wantCount := int64(len(input) * 3) // 3 goroutines writing the same string

	var wg sync.WaitGroup
	writeFunc := func() {
		defer wg.Done()
		if _, err := countingWriter.Write([]byte(input)); err != nil {
			t.Errorf("Write() error = %v", err)
		}
	}

	// Perform concurrent writes
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go writeFunc()
	}
	wg.Wait()

	if got := countingWriter.Count(); got != wantCount {
		t.Errorf("CountingWriter.Count() after concurrent writes = %v, want %v", got, wantCount)
	}
}
