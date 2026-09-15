package db

import (
	"regexp"
	"testing"
)

// breakingPragmasRegex is the original regex-based implementation, kept here
// for benchmark comparison against the scanner-based IsBreakingPragma.
var breakingPragmasRegex = map[string]*regexp.Regexp{
	"PRAGMA journal_mode":       regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?journal_mode\s*=\s*`),
	"PRAGMA wal_autocheckpoint": regexp.MustCompile(`(?i)^\s*PRAGMA\s+wal_autocheckpoint\s*=\s*`),
	"PRAGMA wal_checkpoint":     regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?wal_checkpoint`),
	"PRAGMA synchronous":        regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?synchronous\s*=\s*`),
	"PRAGMA query_only":         regexp.MustCompile(`(?i)^\s*PRAGMA\s+(\w+\.)?query_only\s*=\s*`),
}

func isBreakingPragmaRegex(stmt string) bool {
	for _, re := range breakingPragmasRegex {
		if re.MatchString(stmt) {
			return true
		}
	}
	return false
}

var benchmarkStatements = []string{
	"PRAGMA main.journal_mode=WAL",                      // breaking, assignment form
	"PRAGMA query_only = true",                          // breaking, assignment form
	"PRAGMA wal_checkpoint(TRUNCATE)",                   // breaking, any form
	"PRAGMA journal_mode",                               // allowed, query form
	"PRAGMA cache_size = 10000",                         // allowed pragma
	"INSERT INTO foo VALUES(1)",                         // not a pragma
	"SELECT * FROM foo WHERE x='PRAGMA wal_checkpoint'", // keyword mid-string
}

// BenchmarkIsBreakingPragma measures the scanner-based implementation.
func BenchmarkIsBreakingPragma(b *testing.B) {
	for b.Loop() {
		for _, s := range benchmarkStatements {
			_ = IsBreakingPragma(s)
		}
	}
}

// BenchmarkIsBreakingPragmaRegex measures the original regex-based
// implementation on the same input, for comparison.
func BenchmarkIsBreakingPragmaRegex(b *testing.B) {
	for b.Loop() {
		for _, s := range benchmarkStatements {
			_ = isBreakingPragmaRegex(s)
		}
	}
}

// BenchmarkIsBreakingPragmaNonPragma measures the dominant production
// workload, where virtually all statements are not PRAGMAs at all.
func BenchmarkIsBreakingPragmaNonPragma(b *testing.B) {
	stmts := []string{
		"INSERT INTO foo VALUES(1, 'abc')",
		"SELECT * FROM foo WHERE id = 42",
		"UPDATE bar SET x = 1 WHERE y = 2",
	}
	for b.Loop() {
		for _, s := range stmts {
			_ = IsBreakingPragma(s)
		}
	}
}
