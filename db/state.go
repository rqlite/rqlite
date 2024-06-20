package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
)

const (
	// ModeReadOnly is the mode to open a database in read-only mode.
	ModeReadOnly = true
	// ModeReadWrite is the mode to open a database in read-write mode.
	ModeReadWrite = false
)

// MakeDSN returns a SQLite DSN for the given path, with the given options.
func MakeDSN(path string, readOnly, fkEnabled, walEnabled bool) string {
	opts := url.Values{}
	if readOnly {
		opts.Add("mode", "ro")
	}
	opts.Add("_fk", strconv.FormatBool(fkEnabled))
	opts.Add("_journal", "WAL")
	if !walEnabled {
		opts.Set("_journal", "DELETE")
	}
	opts.Add("_sync", "0")
	return fmt.Sprintf("file:%s?%s", path, opts.Encode())
}

// WALPath returns the path to the WAL file for the given database path.
func WALPath(dbPath string) string {
	return dbPath + "-wal"
}

// IsValidSQLiteFile checks that the supplied path looks like a SQLite file.
// A nonexistent file is considered invalid.
func IsValidSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 16)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsValidSQLiteData(b)
}

// IsValidSQLiteData checks that the supplied data looks like a SQLite data.
// See https://www.sqlite.org/fileformat.html.
func IsValidSQLiteData(b []byte) bool {
	return len(b) > 13 && string(b[0:13]) == "SQLite format"
}

// IsValidSQLiteWALFile checks that the supplied path looks like a SQLite
// WAL file. See https://www.sqlite.org/fileformat2.html#walformat. A
// nonexistent file is considered invalid.
func IsValidSQLiteWALFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 4)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsValidSQLiteWALData(b)
}

// IsValidSQLiteWALData checks that the supplied data looks like a SQLite
// WAL file.
func IsValidSQLiteWALData(b []byte) bool {
	if len(b) < 4 {
		return false
	}

	header1 := []byte{0x37, 0x7f, 0x06, 0x82}
	header2 := []byte{0x37, 0x7f, 0x06, 0x83}
	header := b[:4]
	return bytes.Equal(header, header1) || bytes.Equal(header, header2)
}

// IsWALModeEnabledSQLiteFile checks that the supplied path looks like a SQLite
// with WAL mode enabled.
func IsWALModeEnabledSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 20)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsWALModeEnabled(b)
}

// IsWALModeEnabled checks that the supplied data looks like a SQLite data
// with WAL mode enabled.
func IsWALModeEnabled(b []byte) bool {
	return len(b) >= 20 && b[18] == 2 && b[19] == 2
}

// IsDELETEModeEnabledSQLiteFile checks that the supplied path looks like a SQLite
// with DELETE mode enabled.
func IsDELETEModeEnabledSQLiteFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	b := make([]byte, 20)
	if _, err := f.Read(b); err != nil {
		return false
	}

	return IsDELETEModeEnabled(b)
}

// IsDELETEModeEnabled checks that the supplied data looks like a SQLite file
// with DELETE mode enabled.
func IsDELETEModeEnabled(b []byte) bool {
	return len(b) >= 20 && b[18] == 1 && b[19] == 1
}

// EnsureDeleteMode ensures the database at the given path is in DELETE mode.
func EnsureDeleteMode(path string) error {
	if IsDELETEModeEnabledSQLiteFile(path) {
		return nil
	}
	rwDSN := fmt.Sprintf("file:%s", path)
	conn, err := sql.Open(dbRegisterName, rwDSN)
	if err != nil {
		return fmt.Errorf("open: %s", err.Error())
	}
	defer conn.Close()
	_, err = conn.Exec("PRAGMA journal_mode=DELETE")
	return err
}

// EnsureWALMode ensures the database at the given path is in WAL mode.
func EnsureWALMode(path string) error {
	if IsWALModeEnabledSQLiteFile(path) {
		return nil
	}
	rwDSN := fmt.Sprintf("file:%s", path)
	conn, err := sql.Open(dbRegisterName, rwDSN)
	if err != nil {
		return fmt.Errorf("open: %s", err.Error())
	}
	defer conn.Close()
	_, err = conn.Exec("PRAGMA journal_mode=WAL")
	return err
}

// RemoveFiles removes the SQLite database file, and any associated WAL and SHM files.
func RemoveFiles(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(path + "-wal"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(path + "-shm"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// CheckIntegrity runs a PRAGMA integrity_check on the database at the given path.
// If full is true, a full integrity check is performed, otherwise a quick check.
func CheckIntegrity(path string, full bool) (bool, error) {
	db, err := Open(path, false, false)
	if err != nil {
		return false, err
	}
	defer db.Close()

	sql := "PRAGMA quick_check"
	if full {
		sql = "PRAGMA integrity_check"
	}

	rows, err := db.rwDB.Query(sql)
	if err != nil {
		return false, err
	}

	var result string
	if !rows.Next() {
		return false, nil
	}
	if err := rows.Scan(&result); err != nil {
		return false, err
	}
	return result == "ok", nil
}

// ReplayWAL replays the given WAL files into the database at the given path,
// in the order given by the slice. The supplied WAL files must be in the same
// directory as the database file and are deleted as a result of the replay operation.
// If deleteMode is true, the database file will be in DELETE mode after the replay
// operation, otherwise it will be in WAL mode. Finally, regardless of deleteMode,
// there will be no "true" WAL file after the replay operation.
func ReplayWAL(path string, wals []string, deleteMode bool) error {
	for _, wal := range wals {
		if filepath.Dir(wal) != filepath.Dir(path) {
			return ErrWALReplayDirectoryMismatch
		}
	}

	if !IsWALModeEnabledSQLiteFile(path) {
		return fmt.Errorf("database file %s is not a SQLite database in WAL mode", path)
	}

	for _, wal := range wals {
		if !IsValidSQLiteWALFile(wal) {
			return fmt.Errorf("invalid WAL file %s", wal)
		}
		if err := os.Rename(wal, path+"-wal"); err != nil {
			return fmt.Errorf("rename WAL %s: %s", wal, err.Error())
		}
		db, err := Open(path, false, true)
		if err != nil {
			return err
		}
		if err := db.Checkpoint(CheckpointTruncate); err != nil {
			return fmt.Errorf("checkpoint WAL %s: %s", wal, err.Error())
		}

		if err := db.Close(); err != nil {
			return err
		}
	}

	// Remove any WAL files by ensuring DELETE mode.
	if err := EnsureDeleteMode(path); err != nil {
		return err
	}

	// Might need to switch back to WAL mode to keep the contract.
	if !deleteMode {
		if err := EnsureWALMode(path); err != nil {
			return err
		}
	}

	// Ensure the database file is sync'ed to disk.
	fd, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	if err := fd.Sync(); err != nil {
		return err
	}
	return fd.Close()
}
