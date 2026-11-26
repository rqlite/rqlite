package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

// TestRawHandlesResolveAgainstDriver guards the reflection in rawHandles against
// a breaking modernc.org/sqlite upgrade.
//
// rawHandles recovers the unexported "db" (uintptr) and "tls" (*libc.TLS)
// fields of the driver connection by name. A modernc release that renames or
// retypes either field still compiles, but makes rawHandles return an error,
// which makes buildConnectHook fail, which makes every connection Open fail at
// run time: a node then cannot start or serve, yet nothing is caught at build
// time. This test opens a connection exactly as the package does and asserts
// the handles resolve, turning that latent run-time failure into a red CI lane
// on any breaking bump of modernc.org/sqlite.
func TestRawHandlesResolveAgainstDriver(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "guard.db")
	sqlDB := sql.OpenDB(ConnConfig{}.connector(dsn))
	defer sqlDB.Close()

	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		// buildConnectHook calls rawHandles, so a broken reflection surfaces
		// here as a failed Open.
		t.Fatalf("open connection (rawHandles may have failed inside the connect hook): %v", err)
	}
	defer conn.Close()

	var raw rawConn
	if err := conn.Raw(func(driverConn any) error {
		r, err := rawHandles(driverConn)
		if err != nil {
			return err
		}
		raw = r
		return nil
	}); err != nil {
		t.Fatalf("rawHandles failed against the live driver connection "+
			"(modernc.org/sqlite may have renamed or retyped its db/tls fields): %v", err)
	}

	if raw.db == 0 {
		t.Error("rawHandles returned a zero db handle")
	}
	if raw.tls == nil {
		t.Error("rawHandles returned a nil tls")
	}
}
