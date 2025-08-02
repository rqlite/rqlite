package db

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/internal/random"
)

const (
	// ModeReadOnly is the mode to open a database in read-only mode.
	ModeReadOnly = true
	// ModeReadWrite is the mode to open a database in read-write mode.
	ModeReadWrite = false
)

// SynchronousMode is SQLite synchronous mode.
type SynchronousMode int

const (
	SynchronousOff SynchronousMode = iota
	SynchronousNormal
	SynchronousFull
	SynchronousExtra
)

// String returns the string representation of the synchronous mode.
func (s SynchronousMode) String() string {
	switch s {
	case SynchronousOff:
		return "OFF"
	case SynchronousNormal:
		return "NORMAL"
	case SynchronousFull:
		return "FULL"
	case SynchronousExtra:
		return "EXTRA"
	default:
		panic("unknown synchronous mode")
	}
}

// SynchronousModeFromString returns the synchronous mode from the given string.
func SynchronousModeFromString(s string) (SynchronousMode, error) {
	switch strings.ToUpper(s) {
	case "OFF":
		return SynchronousOff, nil
	case "NORMAL":
		return SynchronousNormal, nil
	case "FULL":
		return SynchronousFull, nil
	case "EXTRA":
		return SynchronousExtra, nil
	default:
		return 0, fmt.Errorf("unknown synchronous mode %s", s)
	}
}

// SynchronousModeFromInt returns the synchronous mode from the given integer.
func SynchronousModeFromInt(i int) (SynchronousMode, error) {
	switch i {
	case 0:
		return SynchronousOff, nil
	case 1:
		return SynchronousNormal, nil
	case 2:
		return SynchronousFull, nil
	case 3:
		return SynchronousExtra, nil
	default:
		return 0, fmt.Errorf("unknown synchronous mode %d", i)
	}
}

// BreakingPragmas are PRAGMAs that, if executed, would break the database layer.
var BreakingPragmas = map[string]*regexp.Regexp{
	"PRAGMA journal_mode":       regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?journal_mode\s*=\s*`),
	"PRAGMA wal_autocheckpoint": regexp.MustCompile(`(?i)^\s*PRAGMA\s+wal_autocheckpoint\s*=\s*`),
	"PRAGMA wal_checkpoint":     regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?wal_checkpoint`),
	"PRAGMA synchronous":        regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?synchronous\s*=\s*`),
}

// IsBreakingPragma returns true if the given statement is a breaking PRAGMA.
func IsBreakingPragma(stmt string) bool {
	for _, re := range BreakingPragmas {
		if re.MatchString(stmt) {
			return true
		}
	}
	return false
}

// ParseHex parses the given string into a byte slice as per the SQLite specification:
//
//	BLOB literals are string literals containing hexadecimal data and preceded by a single
//	"x" or "X" character. Example: X'53514C697465'
func ParseHex(s string) ([]byte, error) {
	t := strings.TrimSpace(s)
	if len(t) < 3 || t[0] != 'X' && t[0] != 'x' {
		return nil, fmt.Errorf("invalid hex string %s", t)
	}
	t = t[1:]

	if t[0] != '\'' || t[len(t)-1] != '\'' {
		return nil, fmt.Errorf("invalid hex string %s", t)
	}

	b, err := hex.DecodeString(t[1 : len(t)-1])
	if err != nil {
		return nil, fmt.Errorf("invalid hex string %s: %s", t, err)
	}
	return b, nil
}

// ValidateExtension validates the given extension path can be loaded into a SQLite database.
func ValidateExtension(path string) error {
	name := random.String()
	sql.Register(name, &sqlite3.SQLiteDriver{})
	db, err := sql.Open(name, ":memory:")
	if err != nil {
		return err
	}
	defer db.Close()

	f := func(driverConn any) error {
		c := driverConn.(*sqlite3.SQLiteConn)
		return c.LoadExtension(path, "")
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	if err := conn.Raw(f); err != nil {
		return err
	}
	return nil
}

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

// IsValidSQLiteFileCompressed checks that the supplied path looks like a
// compressed SQLite file. A nonexistent file, invalid Gzip archive, or
// gzip archive that does not contain a valid SQLite file is considered
// invalid.
func IsValidSQLiteFileCompressed(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return false
	}
	defer gz.Close()

	b := make([]byte, 16)
	_, err = io.ReadFull(gz, b)
	if err != nil {
		return false
	}
	return IsValidSQLiteData(b)
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
	if _, err := io.ReadFull(f, b); err != nil {
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
func IsWALModeEnabledSQLiteFile(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	b := make([]byte, 20)
	if _, err := io.ReadFull(f, b); err != nil {
		return false, err
	}
	return IsWALModeEnabled(b), nil
}

// IsWALModeEnabled checks that the supplied data looks like a SQLite data
// with WAL mode enabled.
func IsWALModeEnabled(b []byte) bool {
	return len(b) >= 20 && b[18] == 2 && b[19] == 2
}

// IsDELETEModeEnabledSQLiteFile checks that the supplied path looks like a SQLite
// with DELETE mode enabled.
func IsDELETEModeEnabledSQLiteFile(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	b := make([]byte, 20)
	if _, err := io.ReadFull(f, b); err != nil {
		return false, err
	}
	return IsDELETEModeEnabled(b), nil
}

// IsDELETEModeEnabled checks that the supplied data looks like a SQLite file
// with DELETE mode enabled.
func IsDELETEModeEnabled(b []byte) bool {
	return len(b) >= 20 && b[18] == 1 && b[19] == 1
}

// EnsureDeleteMode ensures the database at the given path is in DELETE mode.
func EnsureDeleteMode(path string) error {
	db, err := Open(path, false, false)
	if err != nil {
		return err
	}
	return db.Close()
}

// EnsureWALMode ensures the database at the given path is in WAL mode.
func EnsureWALMode(path string) error {
	db, err := Open(path, false, true)
	if err != nil {
		return err
	}
	return db.Close()
}

// CheckpointRemove checkpoints any WAL files into the database file at the given
// given path. Checkpointing a database in DELETE mode is an error.
func CheckpointRemove(path string) error {
	d, err := IsDELETEModeEnabledSQLiteFile(path)
	if err != nil {
		return err
	}
	if d {
		return fmt.Errorf("cannot checkpoint database in DELETE mode")
	}

	drv := CheckpointDriver()
	db, err := OpenWithDriver(drv, path, false, true)
	if err != nil {
		return err
	}
	return db.Close()
}

// RemoveWALFiles removes the WAL and SHM files associated with the given path,
// leaving the database file untouched.
func RemoveWALFiles(path string) error {
	if err := os.Remove(path + "-wal"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(path + "-shm"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// RemoveFiles removes the SQLite database file, and any associated WAL and SHM files.
func RemoveFiles(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := RemoveWALFiles(path); err != nil {
		return err
	}
	return nil
}

// ReplayWAL replays the given WAL files into the database at the given path,
// in the order given by the slice. The supplied WAL files must be in the same
// directory as the database file and are deleted as a result of the replay operation.
// If deleteMode is true, the database file will be in DELETE mode after the replay
// operation, otherwise it will be in WAL mode. In either case no WAL-related files
// will be present.
func ReplayWAL(path string, wals []string, deleteMode bool) error {
	for _, wal := range wals {
		if filepath.Dir(wal) != filepath.Dir(path) {
			return ErrWALReplayDirectoryMismatch
		}
	}

	if !IsValidSQLiteFile(path) {
		return fmt.Errorf("invalid database file %s", path)
	}

	for _, wal := range wals {
		if !IsValidSQLiteWALFile(wal) {
			return fmt.Errorf("invalid WAL file %s", wal)
		}
		if err := os.Rename(wal, path+"-wal"); err != nil {
			return fmt.Errorf("rename WAL %s: %s", wal, err.Error())
		}

		if err := CheckpointRemove(path); err != nil {
			return fmt.Errorf("checkpoint WAL %s: %s", wal, err.Error())
		}
	}

	if deleteMode {
		db, err := Open(path, false, false)
		if err != nil {
			return err
		}
		if err := db.Close(); err != nil {
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

func DumpTablesReq(tables ...string) *command.Request {
	if len(tables) == 0 {
		return &command.Request{
			Statements: []*command.Statement{
				{
					Sql: `SELECT "name", "type", "sql" FROM sqlite_master WHERE "sql" NOT NULL AND type='table' ORDER BY name`,
				},
			},
		}
	}

	var sb strings.Builder
	sb.WriteString(`SELECT "name", "type", "sql" FROM sqlite_master WHERE "sql" NOT NULL AND type='table' AND name IN (`)
	for i := range tables {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
	}
	sb.WriteString(") ORDER BY name")

	parameters := make([]*command.Parameter, len(tables))
	for i, table := range tables {
		parameters[i] = &command.Parameter{
			Value: &command.Parameter_S{
				S: table,
			},
		}
	}

	return &command.Request{
		Statements: []*command.Statement{
			{
				Sql:        sb.String(),
				Parameters: parameters,
			},
		},
	}
}
