package store

import (
	"context"
	"io"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// Provider implements the uploader Provider interface, allowing the
// Store to be used as a DataProvider for an uploader.
type Provider struct {
	str      *Store
	vacuum   bool
	compress bool
}

// NewProvider returns a new instance of Provider. If v is true, the
// SQLite database will be VACUUMed before being provided. If c is
// true, the SQLite database will be compressed before being provided.
func NewProvider(s *Store, v, c bool) *Provider {
	return &Provider{
		str:      s,
		vacuum:   v,
		compress: c,
	}
}

// LastIndex returns the cluster-wide index the data managed by the DataProvider was
// last modified by.
func (p *Provider) LastIndex() (uint64, error) {
	stats.Add(numProviderChecks, 1)
	return p.str.DBAppliedIndex(), nil
}

// Provide writes the SQLite database to w. A backup can fail transiently, for
// example while the Store is snapshotting, and a failed attempt may leave a
// partial backup in w. The caller decides whether to retry, and must do so
// with a fresh writer.
func (p *Provider) Provide(w io.Writer) (retErr error) {
	stats.Add(numProviderProvides, 1)
	defer func() {
		if retErr != nil {
			stats.Add(numProviderProvidesFail, 1)
		}
	}()

	br := &proto.BackupRequest{
		Format:   proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Vacuum:   p.vacuum,
		Compress: p.compress,
	}
	_, err := p.str.Backup(context.Background(), br, w)
	return err
}
