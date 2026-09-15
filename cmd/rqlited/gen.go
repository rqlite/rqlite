//go:generate flagforge -o config_flags.go flags.toml

// The configuration guide at https://rqlite.io/docs/guides/config/ is generated
// from flags.toml as well, grouped by the section key on each flag. It is not a
// go:generate directive because the output lands in a different repository,
// which may be checked out anywhere. To regenerate it:
//
//	flagforge -f html -header config_doc_header.md \
//	    -o $RQLITE_IO/content/en/docs/Guides/config/_index.md flags.toml
//
// config_doc_header.md holds that page's front matter and introduction, which
// aren't derived from flags.toml and would otherwise be lost each time the page
// is regenerated.

package main
