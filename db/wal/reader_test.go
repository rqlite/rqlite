package wal

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestReader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		buf := make([]byte, 4096)
		b, err := os.ReadFile("testdata/wal-reader/ok/wal")
		if err != nil {
			t.Fatal(err)
		}

		// Initialize reader with header info.
		r := NewReader(bytes.NewReader(b))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if got, want := r.PageSize(), uint32(4096); got != want {
			t.Fatalf("PageSize()=%d, want %d", got, want)
		} else if got, want := r.Offset(), int64(0); got != want {
			t.Fatalf("Offset()=%d, want %d", got, want)
		}

		// Read first frame.
		if pgno, commit, err := r.ReadFrame(buf); err != nil {
			t.Fatal(err)
		} else if got, want := pgno, uint32(1); got != want {
			t.Fatalf("pgno=%d, want %d", got, want)
		} else if got, want := commit, uint32(0); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		} else if !bytes.Equal(buf, b[56:4152]) {
			t.Fatal("page data mismatch")
		} else if got, want := r.Offset(), int64(32); got != want {
			t.Fatalf("Offset()=%d, want %d", got, want)
		}

		// Read second frame. End of transaction.
		if pgno, commit, err := r.ReadFrame(buf); err != nil {
			t.Fatal(err)
		} else if got, want := pgno, uint32(2); got != want {
			t.Fatalf("pgno=%d, want %d", got, want)
		} else if got, want := commit, uint32(2); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		} else if !bytes.Equal(buf, b[4176:8272]) {
			t.Fatal("page data mismatch")
		} else if got, want := r.Offset(), int64(4152); got != want {
			t.Fatalf("Offset()=%d, want %d", got, want)
		}

		// Read third frame.
		if pgno, commit, err := r.ReadFrame(buf); err != nil {
			t.Fatal(err)
		} else if got, want := pgno, uint32(2); got != want {
			t.Fatalf("pgno=%d, want %d", got, want)
		} else if got, want := commit, uint32(2); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		} else if !bytes.Equal(buf, b[8296:12392]) {
			t.Fatal("page data mismatch")
		} else if got, want := r.Offset(), int64(8272); got != want {
			t.Fatalf("Offset()=%d, want %d", got, want)
		}

		if _, _, err := r.ReadFrame(buf); err != io.EOF {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("SaltMismatch", func(t *testing.T) {
		buf := make([]byte, 4096)
		b, err := os.ReadFile("testdata/wal-reader/salt-mismatch/wal")
		if err != nil {
			t.Fatal(err)
		}

		// Initialize reader with header info.
		r := NewReader(bytes.NewReader(b))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if got, want := r.PageSize(), uint32(4096); got != want {
			t.Fatalf("PageSize()=%d, want %d", got, want)
		} else if got, want := r.Offset(), int64(0); got != want {
			t.Fatalf("Offset()=%d, want %d", got, want)
		}

		// Read first frame.
		if pgno, commit, err := r.ReadFrame(buf); err != nil {
			t.Fatal(err)
		} else if got, want := pgno, uint32(1); got != want {
			t.Fatalf("pgno=%d, want %d", got, want)
		} else if got, want := commit, uint32(0); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		} else if !bytes.Equal(buf, b[56:4152]) {
			t.Fatal("page data mismatch")
		}

		// Read second frame. Salt has been altered so it doesn't match header.
		if _, _, err := r.ReadFrame(buf); err != io.EOF {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("FrameChecksumMismatch", func(t *testing.T) {
		buf := make([]byte, 4096)
		b, err := os.ReadFile("testdata/wal-reader/frame-checksum-mismatch/wal")
		if err != nil {
			t.Fatal(err)
		}

		// Initialize reader with header info.
		r := NewReader(bytes.NewReader(b))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if got, want := r.PageSize(), uint32(4096); got != want {
			t.Fatalf("PageSize()=%d, want %d", got, want)
		} else if got, want := r.Offset(), int64(0); got != want {
			t.Fatalf("Offset()=%d, want %d", got, want)
		}

		// Read first frame.
		if pgno, commit, err := r.ReadFrame(buf); err != nil {
			t.Fatal(err)
		} else if got, want := pgno, uint32(1); got != want {
			t.Fatalf("pgno=%d, want %d", got, want)
		} else if got, want := commit, uint32(0); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		} else if !bytes.Equal(buf, b[56:4152]) {
			t.Fatal("page data mismatch")
		}

		// Read second frame. Checksum has been altered so it doesn't match.
		if _, _, err := r.ReadFrame(buf); err != io.EOF {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ZeroLength", func(t *testing.T) {
		r := NewReader(bytes.NewReader(nil))
		if err := r.ReadHeader(); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("PartialHeader", func(t *testing.T) {
		r := NewReader(bytes.NewReader(make([]byte, 10)))
		if err := r.ReadHeader(); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("BadMagic", func(t *testing.T) {
		r := NewReader(bytes.NewReader(make([]byte, 32)))
		if err := r.ReadHeader(); err == nil || err.Error() != `invalid wal header magic: 0` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("BadHeaderChecksum", func(t *testing.T) {
		data := []byte{
			0x37, 0x7f, 0x06, 0x83, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		r := NewReader(bytes.NewReader(data))
		if err := r.ReadHeader(); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("BadHeaderVersion", func(t *testing.T) {
		data := []byte{
			0x37, 0x7f, 0x06, 0x83, 0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			0x15, 0x7b, 0x20, 0x92, 0xbb, 0xf8, 0x34, 0x1d}
		r := NewReader(bytes.NewReader(data))
		if err := r.ReadHeader(); err == nil || err.Error() != `unsupported wal version: 1` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrBufferSize", func(t *testing.T) {
		b, err := os.ReadFile("testdata/wal-reader/ok/wal")
		if err != nil {
			t.Fatal(err)
		}

		// Initialize reader with header info.
		r := NewReader(bytes.NewReader(b))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.ReadFrame(make([]byte, 512)); err == nil || err.Error() != `WALReader.ReadFrame(): buffer size (512) must match page size (4096)` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrPartialFrameHeader", func(t *testing.T) {
		b, err := os.ReadFile("testdata/wal-reader/ok/wal")
		if err != nil {
			t.Fatal(err)
		}

		r := NewReader(bytes.NewReader(b[:40]))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if _, _, err := r.ReadFrame(make([]byte, 4096)); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrFrameHeaderOnly", func(t *testing.T) {
		b, err := os.ReadFile("testdata/wal-reader/ok/wal")
		if err != nil {
			t.Fatal(err)
		}

		r := NewReader(bytes.NewReader(b[:56]))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if _, _, err := r.ReadFrame(make([]byte, 4096)); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrPartialFrameData", func(t *testing.T) {
		b, err := os.ReadFile("testdata/wal-reader/ok/wal")
		if err != nil {
			t.Fatal(err)
		}

		r := NewReader(bytes.NewReader(b[:1000]))
		if err := r.ReadHeader(); err != nil {
			t.Fatal(err)
		} else if _, _, err := r.ReadFrame(make([]byte, 4096)); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}
