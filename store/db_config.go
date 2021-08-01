package store

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	Memory bool // Whether the database is in-memory only.
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig(memory bool) *DBConfig {
	return &DBConfig{Memory: memory}
}
