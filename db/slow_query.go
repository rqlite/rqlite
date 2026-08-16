package db

import "time"

const defaultSlowQueryThreshold = 10 * time.Second

// SetSlowQueryThreshold sets the duration after which a SQL statement is logged
// as slow. A non-positive duration disables slow-query logging.
func (db *DB) SetSlowQueryThreshold(d time.Duration) {
	db.slowQueryThreshold = d
}

// logSlowQuery logs sql when the elapsed time since start meets or exceeds the
// configured slow-query threshold.
func (db *DB) logSlowQuery(start time.Time, sql string) {
	if db.slowQueryThreshold <= 0 {
		return
	}

	elapsed := time.Since(start)
	if elapsed >= db.slowQueryThreshold {
		db.logger.Printf("slow query: duration=%s sql=%s", elapsed, sql)
	}
}
