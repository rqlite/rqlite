package db

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func Test_OpenURICharactersInPath(t *testing.T) {
	for _, filename := range []string{"question?.db", "fragment#.db", "percent%20.db"} {
		t.Run(filename, func(t *testing.T) {
			if runtime.GOOS == "windows" && filename == "question?.db" {
				t.Skip("question marks are not valid Windows filenames")
			}
			path := filepath.Join(t.TempDir(), filename)
			database, err := Open(path, false, true)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			mustExecute(database, "CREATE TABLE foo (value TEXT)")
			mustExecute(database, "INSERT INTO foo VALUES ('stored')")
			rows, err := database.QueryStringStmt("SELECT value FROM foo")
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].Error != "" || len(rows[0].Values) != 1 {
				t.Fatalf("query failed: %v", rows)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("database not created at requested path: %v", err)
			}
			if _, err := os.Stat(path + "-wal"); err != nil {
				t.Fatalf("WAL not created at requested path: %v", err)
			}
		})
	}
}
