package proto

// SQLiteError represents a structured SQLite error with full error details.
// This struct is manually defined pending protoc regeneration of the
// corresponding SQLiteError protobuf message in command.proto.
// It flows through Go structs only (db layer → JSON encoding layer)
// and is not yet part of protobuf wire serialization.
type SQLiteError struct {
	Code         int32
	ExtendedCode int32
	SystemErrno  int32
	Message      string
}
