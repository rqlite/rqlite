package store

// ColumnNamesProvider is a wrapper around the Store which implements sql.ColumnNamesProvider.
// It ensures that the column names are always returned by the current database,
// ensuring that even if it's swapped, we access the latest database.
type ColumnNamesProvider struct {
	s *Store
}

// ColumnNames returns the column names for the given table.
func (c *ColumnNamesProvider) ColumnNames(table string) ([]string, error) {
	return c.s.db.ColumnNames(table)
}
