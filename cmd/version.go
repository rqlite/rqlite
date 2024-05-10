package cmd

// These variables are populated via the Go linker.
// Make sure the build process (linker flags) are updated, as well as go.mod.
var (
	// Version of rqlite.
	Version = "8"

	// Commit this code was built at.
	Commit = "unknown"

	// Branch the code was built from.
	Branch = "unknown"

	// Buildtime is the timestamp when the build took place.
	Buildtime = "unknown"

	// CompilerCommand is the compiler used to build the binary.
	CompilerCommand = "unknown"
)
