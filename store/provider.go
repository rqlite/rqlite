package store

import (
	"os"
	"time"

	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/db"
)

// Provider implements the uploader Provider interface, allowing the
// Store to be used as a DataProvider for an uploader.
type Provider struct {
	str    *Store
	vacuum bool

	nRetries      int
	retryInterval time.Duration
}

// NewProvider returns a new instance of Provider.
func NewProvider(s *Store, v bool) *Provider {
	return &Provider{
		str:           s,
		vacuum:        v,
		nRetries:      10,
		retryInterval: 500 * time.Millisecond,
	}
}

// Provider writes the SQLite database to the given path, ensuring the database
// is in DELETE mode. If path exists, it will be overwritten.
func (p *Provider) Provide(path string) error {
	fd, err := os.Create(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	br := &proto.BackupRequest{
		Format: proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Vacuum: p.vacuum,
	}
	nRetries := 0
	for {
		err := p.str.Backup(br, fd)
		if err == nil {
			break
		}
		time.Sleep(p.retryInterval)
		nRetries++
		if nRetries > p.nRetries {
			return err
		}
	}

	// Switch database to DELETE mode, to keep existing behaviour.
	if err := fd.Close(); err != nil {
		return err
	}
	if db.EnsureDeleteMode(path) != nil {
		return err
	}
	stats.Add(numProvides, 1)
	return nil
}
