package store

// DBConfig represents the configuration of the underlying SQLite database.
type DBConfig struct {
	// Enforce Foreign Key constraints
	FKConstraints bool `json:"fk_constraints"`
}

// NewDBConfig returns a new DB config instance.
func NewDBConfig() *DBConfig {
	return &DBConfig{}
}
