package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/internal/rsum"
)

// Restore reads a protobuf-framed snapshot stream and writes
// the resulting SQLite database to dstPath. If the stream contains
// WAL files, they are checkpointed into the database. It returns the
// number of bytes read from the stream.
func Restore(r io.Reader, dstPath string) (int64, error) {
	var totalRead int64

	// Read header length (4 bytes, big-endian).
	var hdrLenBuf [HeaderSizeLen]byte
	n, err := io.ReadFull(r, hdrLenBuf[:])
	totalRead += int64(n)
	if err != nil {
		return totalRead, fmt.Errorf("reading header length: %w", err)
	}
	hdrLen := binary.BigEndian.Uint32(hdrLenBuf[:])

	// Read and parse header.
	hdrBuf := make([]byte, hdrLen)
	n, err = io.ReadFull(r, hdrBuf)
	totalRead += int64(n)
	if err != nil {
		return totalRead, fmt.Errorf("reading header: %w", err)
	}
	hdr, err := UnmarshalSnapshotHeader(hdrBuf)
	if err != nil {
		return totalRead, fmt.Errorf("unmarshaling header: %w", err)
	}

	// The snapshot must be a full snapshot to extract a database.
	full := hdr.GetFull()
	if full == nil {
		return totalRead, fmt.Errorf("snapshot has no database")
	}

	// Extract DB file. Wrap the source in a CRC32Reader so we can verify
	// the bytes match the header's CRC32 without a second pass over disk.
	dbFile, err := os.Create(dstPath)
	if err != nil {
		return totalRead, err
	}

	dbCR := rsum.NewCRC32Reader(r)
	nr, err := io.CopyN(dbFile, dbCR, int64(full.DbHeader.SizeBytes))
	totalRead += nr
	if err != nil {
		dbFile.Close()
		return totalRead, fmt.Errorf("extracting database: %w", err)
	}
	if err := dbFile.Sync(); err != nil {
		dbFile.Close()
		return totalRead, fmt.Errorf("syncing database: %w", err)
	}
	if err := dbFile.Close(); err != nil {
		return totalRead, err
	}
	if got, want := dbCR.Sum32(), full.DbHeader.Crc32; got != want {
		return totalRead, fmt.Errorf("CRC32 mismatch for DB file: got %08x, expected %08x", got, want)
	}

	// Extract and checkpoint any WAL files. Each WAL is verified against
	// its header CRC32 immediately after read, before any are checkpointed
	// into the DB, so a corrupt WAL is never applied.
	if len(full.WalHeaders) > 0 {
		dir := filepath.Dir(dstPath)
		var walFiles []string
		for i, wh := range full.WalHeaders {
			walPath := filepath.Join(dir, fmt.Sprintf("restore-wal-%d.tmp", i))
			wf, err := os.Create(walPath)
			if err != nil {
				return totalRead, err
			}
			walCR := rsum.NewCRC32Reader(r)
			nr, err := io.CopyN(wf, walCR, int64(wh.SizeBytes))
			totalRead += nr
			if err != nil {
				wf.Close()
				return totalRead, fmt.Errorf("extracting WAL %d: %w", i, err)
			}
			if err := wf.Close(); err != nil {
				return totalRead, err
			}
			if got, want := walCR.Sum32(), wh.Crc32; got != want {
				return totalRead, fmt.Errorf("CRC32 mismatch for WAL file %d: got %08x, expected %08x", i, got, want)
			}
			walFiles = append(walFiles, walPath)
		}
		if err := db.ReplayWAL(dstPath, walFiles, false); err != nil {
			return totalRead, fmt.Errorf("checkpointing WALs: %w", err)
		}
		for _, wf := range walFiles {
			os.Remove(wf)
		}
	}

	return totalRead, nil
}
