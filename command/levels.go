package command

import "github.com/rqlite/rqlite/v9/command/proto"

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
	switch s {
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
