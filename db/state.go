package db

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mattn/go-sqlite3"
	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/random"
)

const (
	// ModeReadOnly is the mode to open a database in read-only mode.
	ModeReadOnly = true
	// ModeReadWrite is the mode to open a database in read-write mode.
	ModeReadWrite = false
)

var (
	// ErrWALReplayDirectoryMismatch is returned when the WAL file(s) are not in the same
	// directory as the database file.
	ErrWALReplayDirectoryMismatch = errors.New("WAL file(s) not in same directory as database file")

	// ErrWALAlreadyExists is returned when attempting to replay WAL files but a WAL file
	// already exists alongside the database file.
	ErrWALAlreadyExists = errors.New("cannot replay WAL files: existing WAL file present")

	// ErrWALStillExists is returned when a WAL file still exists after checkpointing and
	// closing the database.
	ErrWALStillExists = errors.New("WAL file still exists after checkpointing and closing the database")
)

// SQLiteError is a representation of the SQLite-level detailed error.
type SQLiteError struct {
	Code         int32
	ExtendedCode int32
	SystemErrno  int32
}

// ReadOnlyError returns true if the error is a read-only error.
func (e *SQLiteError) ReadOnlyError() bool {
	return e.Code == int32(sqlite3.ErrReadonly)
}

// NewSQLiteErrorFromError extracts a full structured SQLite error from an error,
// returning nil if the error is not a SQLite error.
func NewSQLiteErrorFromError(err error) *SQLiteError {
	var sqErr sqlite3.Error
	if errors.As(err, &sqErr) {
		return &SQLiteError{
			Code:         int32(sqErr.Code),
			ExtendedCode: int32(sqErr.ExtendedCode),
			SystemErrno:  int32(sqErr.SystemErrno),
		}
	}
	return nil
}

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

// breakingPragmasAnyForm lists pragma names that are breaking in any form:
// bare name, =value, or (arg).
var breakingPragmasAnyForm = []string{
	"wal_checkpoint",
}

// breakingPragmasAssignment lists pragma names that are breaking only when
// used in an assignment form (name = value or name(value)).
var breakingPragmasAssignment = []string{
	"journal_mode",
	"wal_autocheckpoint",
	"synchronous",
	"query_only",
}

// IsBreakingPragma reports whether any SQL statement contains a PRAGMA that
// would break the database layer. It recognizes both assignment forms, quoted
// identifiers, and comments, while skipping quoted SQL text.
func IsBreakingPragma(stmt string) bool {
	start := true
	for len(stmt) > 0 {
		var token string
		token, stmt = nextPragmaToken(stmt)
		if token == ";" {
			start = true
			continue
		}
		// Some PRAGMAs take effect during preparation, including when the
		// statement is prefixed with EXPLAIN or EXPLAIN QUERY PLAN.
		if start && strings.EqualFold(token, "EXPLAIN") {
			token, stmt = nextPragmaToken(stmt)
			if strings.EqualFold(token, "QUERY") {
				token, stmt = nextPragmaToken(stmt)
				if strings.EqualFold(token, "PLAN") {
					token, stmt = nextPragmaToken(stmt)
				}
			}
		}
		if start && strings.EqualFold(token, "PRAGMA") && isBreakingPragmaBody(stmt) {
			return true
		}
		start = false
	}
	return false
}

func isBreakingPragmaBody(stmt string) bool {
	name, rest := nextPragmaToken(stmt)
	next, rest := nextPragmaToken(rest)
	if next == "." {
		name, rest = nextPragmaToken(rest)
		next, _ = nextPragmaToken(rest)
	}
	if len(name) > 1 {
		switch name[0] {
		case '\'', '"', '`', '[':
			name = name[1 : len(name)-1]
		}
	}
	for _, p := range breakingPragmasAnyForm {
		if strings.EqualFold(name, p) {
			return true
		}
	}
	for _, p := range breakingPragmasAssignment {
		if strings.EqualFold(name, p) {
			return next == "=" || next == "("
		}
	}
	return false
}

// nextPragmaToken scans only the SQL tokens needed to recognize PRAGMAs and
// statement boundaries. Quoted tokens are returned intact, so semicolons and
// comment markers inside strings or identifiers cannot become SQL syntax.
func nextPragmaToken(s string) (token, rest string) {
	for len(s) > 0 {
		if isASCIISpace(s[0]) {
			s = s[1:]
			continue
		}
		if strings.HasPrefix(s, "\ufeff") {
			s = s[3:]
			continue
		}
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
			} else {
				return "", ""
			}
			continue
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s[2:], "*/"); i >= 0 {
				s = s[i+4:]
			} else {
				return "", ""
			}
			continue
		}
		break
	}
	if len(s) == 0 {
		return "", ""
	}

	switch s[0] {
	case '\'', '"', '`', '[':
		quote := s[0]
		if quote == '[' {
			quote = ']'
		}
		for i := 1; i < len(s); i++ {
			if s[i] != quote {
				continue
			}
			if quote != ']' && i+1 < len(s) && s[i+1] == quote {
				i++
				continue
			}
			return s[:i+1], s[i+1:]
		}
		return s, ""
	}
	i := 0
	for i < len(s) {
		b := s[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '$' || b >= 0x80 {
			i++
			continue
		}
		break
	}
	if i == 0 {
		i = 1
	}
	return s[:i], s[i:]
}

func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\v' || b == '\f'
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
	name := path + "-" + random.String()
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
	defer conn.Close()
	if err := conn.Raw(f); err != nil {
		return err
	}
	return nil
}

// MakeDSN returns a SQLite DSN for the given path, with the given options.
// The returned DSN always sets Synchronous=OFF.
func MakeDSN(path string, readOnly, fkEnabled, walEnabled bool) string {
	opts := url.Values{}
	if readOnly {
		opts.Add("mode", "ro")
		opts.Add("_query_only", "true")
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
	if _, err := io.ReadFull(f, b); err != nil {
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

	b := make([]byte, 8)
	if _, err := io.ReadFull(f, b); err != nil {
		return false
	}
	return IsValidSQLiteWALData(b)
}

// IsValidSQLiteWALData checks that the supplied data looks like a SQLite
// WAL file.
func IsValidSQLiteWALData(b []byte) bool {
	if len(b) < 8 {
		return false
	}

	// Check magic number.
	magic := binary.BigEndian.Uint32(b[:4])
	if magic != 0x377f0682 && magic != 0x377f0683 {
		return false
	}

	// Verify version is correct.
	return binary.BigEndian.Uint32(b[4:8]) == 3007000
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

// CheckpointRemove checkpoints any WAL file into the database file at the given
// given path. The function confirms that the WAL file has been removed before returning.
//
// Checkpointing a database in DELETE mode is an error.
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
	defer db.Close()

	// Ensure changes are flushed to disk.
	if err := db.SetSynchronousMode(SynchronousFull); err != nil {
		return fmt.Errorf("failed to set synchronous mode to FULL: %w", err)
	}

	// Explicitly checkpoint the WAL to be absolutely sure.
	meta, err := db.Checkpoint(CheckpointTruncate)
	if err != nil {
		return fmt.Errorf("failed to checkpoint WAL: %w", err)
	}
	if !meta.Success() {
		return fmt.Errorf("checkpoint not successful: %v", meta)
	}

	// Now close the database which checkpoint and removes the WAL and SHM files.
	if err := db.Close(); err != nil {
		return fmt.Errorf("failed to close database after checkpoint: %w", err)
	}

	// Confirm that the WAL file is gone.
	if fsutil.FileExists(path + "-wal") {
		return ErrWALStillExists
	}
	return nil
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

const stashedFilesSuffix = ".stash"

var errSQLiteFilesRollback = errors.New("SQLite file rollback failed")

// StashFiles moves the SQLite database file and any associated WAL and SHM files
// aside to dbPath + ".stash" (with "-wal" and "-shm" appended for the sidecars).
// Missing files are ignored. An existing stash is never overwritten.
//
// The caller must close the database and prevent concurrent access to the files.
// The renames are not atomic as a group or guaranteed durable across a crash. If
// a rename fails, earlier renames are rolled back; rollback errors are returned
// along with the original error, and files may then remain at either location.
func StashFiles(dbPath string) error {
	return moveSQLiteFiles(dbPath, dbPath+stashedFilesSuffix, os.Rename)
}

// PopFiles restores files moved aside by StashFiles. If there are no stashed
// files, it returns without modifying anything. Existing database, WAL or SHM
// files are never overwritten; callers must remove them explicitly if desired.
// The same concurrency and error guarantees as StashFiles apply.
func PopFiles(dbPath string) error {
	return moveSQLiteFiles(dbPath+stashedFilesSuffix, dbPath, os.Rename)
}

// RemoveStashedFiles removes the files moved aside by StashFiles, leaving the
// database and its WAL and SHM files untouched. Missing stash files are ignored.
func RemoveStashedFiles(dbPath string) error {
	return RemoveFiles(dbPath + stashedFilesSuffix)
}

func moveSQLiteFiles(from, to string, rename func(string, string) error) error {
	suffixes := []string{"", "-wal", "-shm"}
	var present []string
	for _, suffix := range suffixes {
		info, err := os.Lstat(from + suffix)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("not a regular SQLite file: %s", from+suffix)
		}
		present = append(present, suffix)
	}
	if len(present) == 0 {
		return nil
	}

	// Check every destination, even if its source is absent, to avoid combining
	// a database with WAL or SHM files from a different database.
	for _, suffix := range suffixes {
		if _, err := os.Lstat(to + suffix); err == nil {
			return fmt.Errorf("SQLite file already exists: %s: %w", to+suffix, os.ErrExist)
		} else if !os.IsNotExist(err) {
			return err
		}
	}

	for i, suffix := range present {
		if err := rename(from+suffix, to+suffix); err != nil {
			retErr := fmt.Errorf("move SQLite file %s: %w", from+suffix, err)
			for j := i - 1; j >= 0; j-- {
				s := present[j]
				if err := rename(to+s, from+s); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("%w for %s: %w", errSQLiteFilesRollback, from+s, err))
				}
			}
			return retErr
		}
	}
	return nil
}

// ReplayWAL replays the given WAL files into the database at the given path,
// in the order given by the slice. The supplied WAL files must be in the same
// directory as the database file and are deleted as a result of the replay operation.
// If deleteMode is true, the database file will be in DELETE mode after the replay
// operation, otherwise it will be in WAL mode. In either case no WAL-related files
// will be present.
//
// If any WAL file is already present alongside the database file, an error is returned.
func ReplayWAL(path string, wals []string, deleteMode bool) error {
	for _, wal := range wals {
		if filepath.Dir(wal) != filepath.Dir(path) {
			return ErrWALReplayDirectoryMismatch
		}
	}

	if fsutil.FileExists(path + "-wal") {
		return ErrWALAlreadyExists
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
		fd.Close() // Best effort.
		return err
	}
	return fd.Close()
}

// DumpTablesReq returns a command.Request that will dump the schema of the
// given tables, or all tables if none are specified. This form protects
// against SQL injection by using parameterized queries.
//
// The returne object does not have the Transaction flag set.
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
