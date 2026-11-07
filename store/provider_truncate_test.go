package store

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func Test_ProviderTruncatesExistingBackup(t *testing.T) {
	s, ln := mustNewStore(t)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Close(true)

	f, err := os.Create(filepath.Join(t.TempDir(), "backup.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := f.Truncate(1024 * 1024); err != nil {
		t.Fatal(err)
	}
	p := NewProvider(s, false, true)
	p.nRetries = 0
	if err := p.Provide(f); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	r, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("replacement backup contains stale trailing data: %v", err)
	}
}
