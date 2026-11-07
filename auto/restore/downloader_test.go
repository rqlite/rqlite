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

	"filippo.io/age"
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

func TestDownloader_Do_Encrypted(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	oldIdentity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	plaintext := bytes.Repeat([]byte("test data"), 16<<10)

	// Generate fixtures with age independently of the restore implementation.
	encrypt := func(data []byte, recipient age.Recipient) []byte {
		t.Helper()
		var ciphertext bytes.Buffer
		w, err := age.Encrypt(&ciphertext, recipient)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return ciphertext.Bytes()
	}
	ciphertext := encrypt(plaintext, identity.Recipient())
	oldCiphertext := encrypt(plaintext, oldIdentity.Recipient())
	compressed := &mockStorageClient{data: plaintext}
	if err := compressed.Compress(); err != nil {
		t.Fatal(err)
	}
	compressedCiphertext := encrypt(compressed.data, identity.Recipient())
	corrupted := bytes.Clone(ciphertext)
	corrupted[len(corrupted)-1] ^= 1
	corruptedCompressed := bytes.Clone(compressedCiphertext)
	corruptedCompressed[len(corruptedCompressed)-1] ^= 1
	identities := oldIdentity.String() + "\n" + identity.String()

	for _, tc := range []struct {
		name    string
		data    []byte
		keys    string
		wantErr bool
	}{
		{"Uncompressed", ciphertext, identity.String(), false},
		{"GzipCompressed", compressedCiphertext, identity.String(), false},
		{"RetainedOldIdentity", oldCiphertext, identities, false},
		{"CurrentIdentity", ciphertext, identities, false},
		{"WrongIdentity", ciphertext, oldIdentity.String(), true},
		{"Corrupted", corrupted, identity.String(), true},
		{"Truncated", ciphertext[:len(ciphertext)-1], identity.String(), true},
		{"CorruptedGzip", corruptedCompressed, identity.String(), true},
		{"TruncatedGzip", compressedCiphertext[:len(compressedCiphertext)-1], identity.String(), true},
		{"MissingIdentities", ciphertext, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDownloader(&mockStorageClient{data: tc.data})
			if tc.keys != "" {
				d.SetDecryptionKeys(tc.keys)
			}
			var output bytes.Buffer
			err := d.Do(context.Background(), &output, 5*time.Second)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected encrypted restore to fail")
				}
				if tc.keys == "" {
					if err.Error() != "backup is encrypted, but no decryption_keys configured" {
						t.Fatalf("unexpected missing-keys error: %v", err)
					}
					if output.Len() != 0 {
						t.Fatal("encrypted backup was written without decryption keys")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(output.Bytes(), plaintext) {
				t.Fatal("restored data does not match plaintext")
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
