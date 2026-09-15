package progress

import (
	"bytes"
	"io"
	"strings"
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
