package store

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	// SQLite on-disk path
	OnDiskPath string `json:"on_disk_path,omitempty"`

	// Enforce Foreign Key constraints
	FKConstraints bool `json:"fk_constraints"`

	// Disable WAL mode if running in on-disk mode
	DisableWAL bool `json:"disable_wal"`
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig() *DBConfig {
	return &DBConfig{}
}
