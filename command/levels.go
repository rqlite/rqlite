package command

import "github.com/rqlite/rqlite/v9/command/proto"

// ConsistencyLevelToString converts a proto QueryRequest_Level to a string.
func ConsistencyLevelToString(level proto.QueryRequest_Level) string {
	switch level {
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_NONE:
		return "none"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_WEAK:
		return "weak"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_STRONG:
		return "strong"
	case proto.QueryRequest_QUERY_REQUEST_LEVEL_LINEARIZABLE:
		return "linearizable"
	default:
		return "weak"
	}
}
