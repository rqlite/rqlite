// Benchmark suite for CheckpointManager and alternative Checkpointer implementations.
//
// Test data (DB and WAL files) must be supplied externally via custom flags:
//
//	-bench-db-path  Path to the SQLite database file
//	-bench-wal-path Path to the WAL file
//
// If either flag is omitted the benchmarks are skipped.
//
// Usage:
//
//	go test -bench=BenchmarkCheckpoint -benchmem ./db \
//	  -bench-db-path /path/to/test.db \
//	  -bench-wal-path /path/to/test.db-wal
//
// Controlling the number of iterations:
//
//	# Run exactly 5 iterations (note the 'x' suffix)
//	go test -bench=BenchmarkCheckpoint ./db -benchtime=5x \
//	  -bench-db-path /path/to/test.db \
//	  -bench-wal-path /path/to/test.db-wal
//
//	# Run for at least 30 seconds, auto-tuning iteration count
//	go test -bench=BenchmarkCheckpoint ./db -benchtime=30s \
//	  -bench-db-path /path/to/test.db \
//	  -bench-wal-path /path/to/test.db-wal
//
// To benchmark an alternative Checkpointer implementation, add an entry to
// the factories map in each benchmark function.
package db

import (
	"flag"
	"io"
	"path/filepath"
	"testing"
	"time"
)

var (
	benchDBPath  = flag.String("bench-db-path", "", "path to SQLite database file for checkpoint benchmarks")
	benchWALPath = flag.String("bench-wal-path", "", "path to WAL file for checkpoint benchmarks")
)

// Checkpointer is the interface that checkpoint manager implementations must
// satisfy in order to be benchmarked.
type Checkpointer interface {
	Checkpoint(w io.Writer, timeout time.Duration) (int64, error)
	Close() error
}

// checkpointerFactory creates a Checkpointer for the given DB.
type checkpointerFactory func(db *DB) (Checkpointer, error)

func BenchmarkCheckpoint(b *testing.B) {
	if *benchDBPath == "" || *benchWALPath == "" {
		b.Skip("supply -bench-db-path and -bench-wal-path to run checkpoint benchmarks")
	}

	factories := map[string]checkpointerFactory{
		"CheckpointManager": func(db *DB) (Checkpointer, error) {
			return NewCheckpointManager(db)
		},
	}

	for name, factory := range factories {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				dbPath := filepath.Join(dir, "bench.db")
				walPath := dbPath + "-wal"
				mustCopyFile(dbPath, *benchDBPath)
				mustCopyFile(walPath, *benchWALPath)

				db, err := Open(dbPath, false, true)
				if err != nil {
					b.Fatalf("failed to open database: %s", err)
				}

				cp, err := factory(db)
				if err != nil {
					db.Close()
					b.Fatalf("failed to create checkpointer: %s", err)
				}

				b.StartTimer()
				_, err = cp.Checkpoint(io.Discard, 30*time.Second)
				if err != nil {
					b.Fatalf("checkpoint failed: %s", err)
				}
				b.StopTimer()

				cp.Close()
				db.Close()
			}
		})
	}
}

func BenchmarkCheckpointNoWriter(b *testing.B) {
	if *benchDBPath == "" || *benchWALPath == "" {
		b.Skip("supply -bench-db-path and -bench-wal-path to run checkpoint benchmarks")
	}

	factories := map[string]checkpointerFactory{
		"CheckpointManager": func(db *DB) (Checkpointer, error) {
			return NewCheckpointManager(db)
		},
	}

	for name, factory := range factories {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := b.TempDir()
				dbPath := filepath.Join(dir, "bench.db")
				walPath := dbPath + "-wal"
				mustCopyFile(dbPath, *benchDBPath)
				mustCopyFile(walPath, *benchWALPath)

				db, err := Open(dbPath, false, true)
				if err != nil {
					b.Fatalf("failed to open database: %s", err)
				}

				cp, err := factory(db)
				if err != nil {
					db.Close()
					b.Fatalf("failed to create checkpointer: %s", err)
				}

				b.StartTimer()
				_, err = cp.Checkpoint(nil, 30*time.Second)
				if err != nil {
					b.Fatalf("checkpoint failed: %s", err)
				}
				b.StopTimer()

				cp.Close()
				db.Close()
			}
		})
	}
}
