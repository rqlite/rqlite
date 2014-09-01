package server

import (
	"io/ioutil"
	"os"

	log "code.google.com/p/log4go"
)

type DbStateMachine struct {
	dbpath string
}

// NewDbStateMachine returns a StateMachine for capturing and restoring
// the state of an sqlite database.
func NewDbStateMachine(path string) *DbStateMachine {
	d := &DbStateMachine{
		dbpath: path,
	}
	log.Trace("New DB state machine created with path: %s", path)
	return d
}

// Save captures the state of the database. The caller must ensure that
// no transaction is taking place during this call.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (d *DbStateMachine) Save() ([]byte, error) {
	log.Trace("Capturing database state from path: %s", d.dbpath)
	b, err := ioutil.ReadFile(d.dbpath)
	if err != nil {
		log.Error("Failed to save state: ", err.Error())
		return nil, err
	}
	log.Trace("Database state successfully saved to %s", d.dbpath)
	return b, nil
}

// Recovery restores the state of the database using the given data.
func (d *DbStateMachine) Recovery(b []byte) error {
	log.Trace("Restoring database state to path: %s", d.dbpath)
	err := ioutil.WriteFile(d.dbpath, b, os.ModePerm)
	if err != nil {
		log.Error("Failed to recover state: ", err.Error())
		return err
	}
	log.Trace("Database restored successfully to %s", d.dbpath)
	return nil
}
