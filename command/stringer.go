package command

import "github.com/rqlite/rqlite/v9/command/proto"

// LevelToString converts a proto.QueryRequest_Level to a string.
func LevelToString(l proto.QueryRequest_Level) string {
	switch l {
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE:
		return "none"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK:
		return "weak"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG:
		return "strong"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_AUTO:
		return "auto"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_LINEARIZABLE:
		return "linearizable"
	default:
		return "unknown"
	}
}

// LevelFromString converts a string to a proto.QueryRequest_Level.
func LevelFromString(s string) proto.QueryRequest_Level {
	switch s {
	case "none":
		return proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	case "weak":
		return proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	case "strong":
		return proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	case "auto":
		return proto.QueryRequest_QUERY_REQUEST_LEVEL_AUTO
	case "linearizable":
		return proto.QueryRequest_QUERY_REQUEST_LEVEL_LINEARIZABLE
	default:
		return proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
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
