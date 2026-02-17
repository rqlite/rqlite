package command

import (
	"strings"

	"github.com/rqlite/rqlite/v10/command/proto"
)

// LevelToString converts a proto.ConsistencyLevel to a string.
func LevelToString(l proto.ConsistencyLevel) string {
	switch l {
	case proto.ConsistencyLevel_NONE:
		return "none"
	case proto.ConsistencyLevel_WEAK:
		return "weak"
	case proto.ConsistencyLevel_STRONG:
		return "strong"
	case proto.ConsistencyLevel_AUTO:
		return "auto"
	case proto.ConsistencyLevel_LINEARIZABLE:
		return "linearizable"
	default:
		return "unknown"
	}
}

// LevelFromString converts a string to a proto.ConsistencyLevel.
func LevelFromString(s string) proto.ConsistencyLevel {
	switch strings.ToLower(s) {
	case "none":
		return proto.ConsistencyLevel_NONE
	case "weak":
		return proto.ConsistencyLevel_WEAK
	case "strong":
		return proto.ConsistencyLevel_STRONG
	case "auto":
		return proto.ConsistencyLevel_AUTO
	case "linearizable":
		return proto.ConsistencyLevel_LINEARIZABLE
	default:
		return proto.ConsistencyLevel_WEAK
	}
}

// BackupFormatToString converts a proto.BackupRequest_Format to a string.
func BackupFormatToString(f proto.BackupRequest_Format) string {
	switch f {
	case proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY:
		return "binary"
	case proto.BackupRequest_BACKUP_REQUEST_FORMAT_SQL:
		return "sql"
	case proto.BackupRequest_BACKUP_REQUEST_FORMAT_DELETE:
		return "delete"
	default:
		return "unknown"
	}
}

// BackupFormatFromString converts a string to a proto.BackupRequest_Format.
func BackupFormatFromString(s string) proto.BackupRequest_Format {
	switch strings.ToLower(s) {
	case "binary":
		return proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY
	case "sql":
		return proto.BackupRequest_BACKUP_REQUEST_FORMAT_SQL
	case "delete":
		return proto.BackupRequest_BACKUP_REQUEST_FORMAT_DELETE
	default:
		return proto.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY
	}
}
