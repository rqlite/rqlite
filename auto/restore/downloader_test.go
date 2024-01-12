package restore

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestDownloader_Do(t *testing.T) {
	tests := []struct {
		name           string
		mockClientData []byte
		compress       bool
		expectError    error
	}{
		{
			name:           "Successful download",
			mockClientData: []byte("test data"),
			expectError:    nil,
		},
		{
			name:           "Successful download of compressed data",
			mockClientData: []byte("test data"),
			compress:       true,
			expectError:    nil,
		},
		{
			name:        "Download error",
			expectError: errors.New("download error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockStorageClient{
				data:  tt.mockClientData,
				error: tt.expectError,
			}
			if tt.compress {
				mockClient.Compress()
			}
			downloader := NewDownloader(mockClient)

			f := new(bytes.Buffer)
			err := downloader.Do(context.Background(), f, 5*time.Second)
			if tt.expectError != nil {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				if err.Error() != tt.expectError.Error() {
					t.Errorf("Expected error %v, but got %v", tt.expectError, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !bytes.Equal(tt.mockClientData, f.Bytes()) {
					t.Errorf("Expected output data %v, but got %v", tt.mockClientData, f.Bytes())
				}
			}
		})
	}
}

type mockStorageClient struct {
	data  []byte
	error error
}

func (m *mockStorageClient) Download(ctx context.Context, writer io.WriterAt) error {
	if m.error != nil {
		return m.error
	}
	n, err := writer.WriteAt(m.data, 0)
	if n != len(m.data) || err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	return nil
}

func (m *mockStorageClient) Compress() error {
	var compressedData bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedData)

	_, err := gzipWriter.Write(m.data)
	if err != nil {
		return err
	}

	err = gzipWriter.Close()
	if err != nil {
		return err
	}

	m.data = compressedData.Bytes()
	return nil
}

func (m *mockStorageClient) String() string {
	return "mockStorageClient"
}
