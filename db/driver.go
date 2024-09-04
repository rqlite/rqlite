package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/rqlite/go-sqlite3"
)

const (
	defaultDriverName = "rqlite-sqlite3"
)

var registerOnce sync.Once

var connectHookFn = func(conn *sqlite3.SQLiteConn) error {
	if err := conn.DBConfigNoCkptOnClose(); err != nil {
		return fmt.Errorf("cannot disable checkpoint on close: %w", err)
	}
	return nil
}

// Driver is a Database driver.
type Driver struct {
	name       string
	extensions []string
}

// DefaultDriver returns the default driver. It registers the SQLite3 driver
// with the default driver name. It can be called multiple times, but only
// registers the SQLite3 driver once.
func DefaultDriver() *Driver {
	registerOnce.Do(func() {
		sql.Register(defaultDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: connectHookFn,
		})
	})
	return &Driver{
		name: defaultDriverName,
	}
}

// NewDriver returns a new driver with the given name and extensions. It
// registers the SQLite3 driver with the given name. If a driver with the
// given name already exists, a panic will occur.
func NewDriver(name string, extensions []string) *Driver {
	sql.Register(name, &sqlite3.SQLiteDriver{
		Extensions:  extensions,
		ConnectHook: connectHookFn,
	})
	return &Driver{
		name:       name,
		extensions: extensions,
	}
}

// Name returns the driver name.
func (d *Driver) Name() string {
	return d.name
}

// Extensions returns the paths of the loaded driver extensions.
func (d *Driver) Extensions() []string {
	return d.extensions
}

// ExtensionNames returns the names of the loaded driver extensions.
func (d *Driver) ExtensionNames() []string {
	names := make([]string, 0)
	for _, ext := range d.extensions {
		names = append(names, filepath.Base(ext))
	}
	sort.Strings(names)
	return names
}
