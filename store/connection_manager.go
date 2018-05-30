package store

import (
	"log"
	"os"
	"sync"
	"time"
)

type ConnectionFactory func() (*Connection, error)

// ConnectionManager manages a set of connections.
type ConnectionManager struct {
	factory ConnectionFactory

	idleTimeout time.Duration
	txTimeout   time.Duration

	connsMu sync.RWMutex
	conns   map[uint64]*Connection

	logger *log.Logger
}

func NewConnectionManager(f ConnectionFactory, ct, tt time.Duration) *ConnectionManager {
	cm := ConnectionManager{
		factory:     f,
		idleTimeout: ct,
		txTimeout:   tt,
		conns:       make(map[uint64]*Connection, 16),
		logger:      log.New(os.Stderr, "[connection_manager] ", log.LstdFlags),
	}
	cm.run()
	return &cm
}

func (cm *ConnectionManager) New() (*Connection, error) {
	c, err := cm.factory()
	if err != nil {
		return nil, err
	}

	cm.connsMu.Lock()
	defer cm.connsMu.Unlock()
	cm.conns[c.ID()] = c
	return c, nil
}

func (cm *ConnectionManager) Get(id uint64) (*Connection, bool) {
	cm.connsMu.RLock()
	defer cm.connsMu.RUnlock()
	c, ok := cm.conns[id]
	return c, ok
}

func (cm *ConnectionManager) Close(id uint64) error {
	cm.connsMu.Lock()
	defer cm.connsMu.Unlock()
	return nil
}

func (cm *ConnectionManager) run() {
	ticker := time.NewTicker(time.Second) // XXX CONFIGURABLE OR CONST
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			func() {
				cm.connsMu.Lock()
				defer cm.connsMu.Unlock()
				for id, c := range cm.conns {
					if time.Since(c.LastUsedAt) > cm.idleTimeout {
						if err := c.Close(); err != nil {
							cm.logger.Printf("failed to close %s: %s", c, err.Error())
						}
						delete(cm.conns, id)
					}
				}
			}()
		}
	}
}
