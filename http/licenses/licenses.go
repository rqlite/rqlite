package licenses

import _ "embed"

// Text contains the license information for rqlite and its dependencies.
//
//go:embed licenses.txt
var Text string
