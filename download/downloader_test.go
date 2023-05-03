package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

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

func (m *mockStorageClient) String() string {
	return "mockStorageClient"
}

func TestDownloader_Do(t *testing.T) {
	tests := []struct {
		name               string
		mockClientData     []byte
		mockClientError    error
		expectedOutputData []byte
		expectError        bool
	}{
		{
			name:               "Successful Download",
			mockClientData:     []byte("test data"),
			expectedOutputData: []byte("test data"),
			expectError:        false,
		},
		{
			name:            "Download Error",
			mockClientError: errors.New("download error"),
			expectError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockStorageClient{
				data:  tt.mockClientData,
				error: tt.mockClientError,
			}
			downloader := download.NewDownloader(mockClient)

			outputBuffer := bytes.NewBuffer(make([]byte, 0, len(tt.expectedOutputData)))
			err := downloader.Do(context.Background(), outputBuffer)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !bytes.Equal(tt.expectedOutputData, outputBuffer.Bytes()) {
					t.Errorf("Expected output data %v, but got %v", tt.expectedOutputData, outputBuffer.Bytes())
				}
			}
		})
	}
}

func TestDownloader_Stats(t *testing.T) {
	mockClient := &mockStorageClient{
		data: []byte("test data"),
	}
	downloader := download.NewDownloader(mockClient)
	buffer := bytes.NewBuffer(make([]byte, 0, len(mockClient.data)))

	download.ResetStats()

	err := downloader.Do(context.Background(), buffer)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	numDownloadsOK := download.GetStats().Get(download.NumDownloadsOK)
	if numDownloadsOK != 1 {
		t.Errorf("Expected NumDownloadsOK to be 1, but got %d", numDownloadsOK)
	}

	numDownloadsFail := download.GetStats().Get(download.NumDownloadsFail)
	if numDownloadsFail != 0 {
		t.Errorf("Expected NumDownloadsFail to be 0, but got %d", numDownloadsFail)
	}

	numDownloadBytes := download.GetStats().Get(download.NumDownloadBytes)
	if numDownloadBytes != int64(len(mockClient.data)) {
		t.Errorf("Expected NumDownloadBytes to be %d, but got %d", len(mockClient.data), numDownloadBytes)
	}
}
