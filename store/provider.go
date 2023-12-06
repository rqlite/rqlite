package store

// Provider implements the uploader Provider interface, allowing the
// Store to be used as a DataProvider for an uploader.
type Provider struct {
	str    *Store
	vacuum bool
}

// NewProvider returns a new instance of Provider.
func NewProvider(s *Store, v bool) *Provider {
	return &Provider{
		str:    s,
		vacuum: v,
	}
}

// Provider writes the SQLite database to the given path.
func (p *Provider) Provide(path string) error {
	if err := p.str.db.Backup(path, p.vacuum); err != nil {
		return err
	}
	stats.Add(numProvides, 1)
	return nil
}
