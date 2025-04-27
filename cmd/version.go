package cmd

import "time"

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

	// Buildyear is the year when the build took place.
	Buildyear = "unknown"

	// CompilerCommand is the compiler used to build the binary.
	CompilerCommand = "unknown"
)

// BuildyearOrNow returns the build year if known, or the current year if not.
func BuildyearOrNow() string {
	if Buildyear == "unknown" {
		return time.Now().Format("2006")
	}
	return Buildyear
}
