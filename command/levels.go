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
