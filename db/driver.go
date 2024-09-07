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
	chkDriverName     = "rqlite-sqlite3-chk"
)

// CnkOnCloseMode represents the checkpoint on close mode.
type CnkOnCloseMode int

const (
	// CnkOnCloseModeEnabled enables checkpoint on close.
	CnkOnCloseModeEnabled CnkOnCloseMode = iota

	// CnkOnCloseModeDisabled disables checkpoint on close.
	CnkOnCloseModeDisabled
)

// Driver is a Database driver.
type Driver struct {
	name       string
	extensions []string
	chkOnClose CnkOnCloseMode
}

var defRegisterOnce sync.Once

// DefaultDriver returns the default driver. It registers the SQLite3 driver
// with the default driver name. It can be called multiple times, but only
// registers the SQLite3 driver once. This driver disables checkpoint on close
// for any database in WAL mode.
func DefaultDriver() *Driver {
	defRegisterOnce.Do(func() {
		sql.Register(defaultDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: makeConnectHookFn(CnkOnCloseModeDisabled),
		})
	})
	return &Driver{
		name:       defaultDriverName,
		chkOnClose: CnkOnCloseModeDisabled,
	}
}

var chkRegisterOnce sync.Once

// CheckpointDriver returns the checkpoint driver. It registers the SQLite3
// driver with the checkpoint driver name. It can be called multiple times,
// but only registers the SQLite3 driver once. This driver enables checkpoint
// on close for any database in WAL mode.
func CheckpointDriver() *Driver {
	chkRegisterOnce.Do(func() {
		sql.Register(chkDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: makeConnectHookFn(CnkOnCloseModeEnabled),
		})
	})
	return &Driver{
		name:       chkDriverName,
		chkOnClose: CnkOnCloseModeEnabled,
	}
}

// NewDriver returns a new driver with the given name and extensions. It
// registers the SQLite3 driver with the given name. extensions is a list of
// paths to SQLite3 extension shared objects. chkpt is the checkpoint-on-close
// mode the Driver will use.
//
// If a driver with the given name already exists, a panic will occur.
func NewDriver(name string, extensions []string, chkpt CnkOnCloseMode) *Driver {
	sql.Register(name, &sqlite3.SQLiteDriver{
		Extensions:  extensions,
		ConnectHook: makeConnectHookFn(chkpt),
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

// CheckpointOnCloseMode returns the checkpoint on close mode.
func (d *Driver) CheckpointOnCloseMode() CnkOnCloseMode {
	return d.chkOnClose
}

func makeConnectHookFn(chkpt CnkOnCloseMode) func(conn *sqlite3.SQLiteConn) error {
	return func(conn *sqlite3.SQLiteConn) error {
		if chkpt == CnkOnCloseModeDisabled {
			if err := conn.DBConfigNoCkptOnClose(); err != nil {
				return fmt.Errorf("cannot disable checkpoint on close: %w", err)
			}
		}
		return nil
	}
}
