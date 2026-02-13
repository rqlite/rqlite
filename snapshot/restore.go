package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rqlite/rqlite/v9/db"
)

// ExtractDatabase reads a protobuf-framed snapshot stream and writes
// the resulting SQLite database to dstPath. If the stream contains
// WAL files, they are checkpointed into the database. It returns the
// number of bytes read from the stream.
func ExtractDatabase(r io.Reader, dstPath string) (int64, error) {
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

	// Extract DB file.
	if hdr.DbHeader == nil {
		return totalRead, fmt.Errorf("snapshot has no database")
	}
	dbFile, err := os.Create(dstPath)
	if err != nil {
		return totalRead, err
	}

	nr, err := io.CopyN(dbFile, r, int64(hdr.DbHeader.SizeBytes))
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

	// Extract and checkpoint any WAL files.
	if len(hdr.WalHeaders) > 0 {
		dir := filepath.Dir(dstPath)
		var walFiles []string
		for i, wh := range hdr.WalHeaders {
			walPath := filepath.Join(dir, fmt.Sprintf("restore-wal-%d.tmp", i))
			wf, err := os.Create(walPath)
			if err != nil {
				return totalRead, err
			}
			nr, err := io.CopyN(wf, r, int64(wh.SizeBytes))
			totalRead += nr
			if err != nil {
				wf.Close()
				return totalRead, fmt.Errorf("extracting WAL %d: %w", i, err)
			}
			if err := wf.Close(); err != nil {
				return totalRead, err
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
