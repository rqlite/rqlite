package store

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	DSN    string // Any custom DSN
	Memory bool   // Whether the database is in-memory only.
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig(dsn string, memory bool) *DBConfig {
	return &DBConfig{DSN: dsn, Memory: memory}
}
