package http

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rqlite/rqlite/command"
)

// QueryParams represents the query parameters passed in an HTTP request.
type QueryParams map[string]string

// NewQueryParams returns a new QueryParams from the given HTTP request.
func NewQueryParams(r *http.Request) (QueryParams, error) {
	qp := make(QueryParams)
	values, err := url.ParseQuery(strings.ToLower(r.URL.RawQuery))
	if err != nil {
		return nil, err
	}
	for k, v := range values {
		qp[k] = v[0]
	}

	for _, k := range []string{"timeout", "freshness"} {
		t, ok := qp[k]
		if ok {
			_, err := time.ParseDuration(t)
			if err != nil {
				return nil, err
			}
		}
	}
	sz, ok := qp["chunk_kb"]
	if ok {
		_, err := strconv.Atoi(sz)
		if err != nil {
			return nil, err
		}
	}
	return qp, nil
}

// Timings returns true if the query parameters indicate timings should be returned.
func (qp QueryParams) Timings() bool {
	return qp.HasKey("timings")
}

// Tx returns true if the query parameters indicate the query should be executed in a transaction.
func (qp QueryParams) Tx() bool {
	return qp.HasKey("transaction")
}

// Query returns true if the query parameters request queued operation
func (qp QueryParams) Queue() bool {
	return qp.HasKey("queue")
}

// Pretty returns true if the query parameters indicate pretty-printing should be returned.
func (qp QueryParams) Pretty() bool {
	return qp.HasKey("pretty")
}

// Bypass returns true if the query parameters indicate bypass mode.
func (qp QueryParams) Bypass() bool {
	return qp.HasKey("bypass")
}

// Wait returns true if the query parameters indicate the query should wait.
func (qp QueryParams) Wait() bool {
	return qp.HasKey("wait")
}

// ChunkKB returns the requested chunk size.
func (qp QueryParams) ChunkKB(defSz int) int {
	s, ok := qp["chunk_kb"]
	if !ok {
		return defSz
	}
	sz, _ := strconv.Atoi(s)
	return sz * 1024
}

// Associative returns true if the query parameters request associative results.
func (qp QueryParams) Associative() bool {
	return qp.HasKey("associative")
}

// NoRewrite returns true if the query parameters request no rewriting of queries.
func (qp QueryParams) NoRewriteRandom() bool {
	return qp.HasKey("norwrandom")
}

// NonVoters returns true if the query parameters request non-voters to be included in results.
func (qp QueryParams) NonVoters() bool {
	return qp.HasKey("nonvoters")
}

// NoLeader returns true if the query parameters request no leader mode.
func (qp QueryParams) NoLeader() bool {
	return qp.HasKey("noleader")
}

// Redirect returns true if the query parameters request redirect mode.
func (qp QueryParams) Redirect() bool {
	return qp.HasKey("redirect")
}

// Vacuum returns true if the query parameters request vacuum mode.
func (qp QueryParams) Vacuum() bool {
	return qp.HasKey("vacuum")
}

// Key returns the value of the key named "key".
func (qp QueryParams) Key() string {
	return qp["key"]
}

// Level returns the requested consistency level.
func (qp QueryParams) Level() command.QueryRequest_Level {
	lvl := qp["level"]
	switch strings.ToLower(lvl) {
	case "none":
		return command.QueryRequest_QUERY_REQUEST_LEVEL_NONE
	case "weak":
		return command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	case "strong":
		return command.QueryRequest_QUERY_REQUEST_LEVEL_STRONG
	default:
		return command.QueryRequest_QUERY_REQUEST_LEVEL_WEAK
	}
}

// BackupFormat returns the requested backup format.
func (qp QueryParams) BackupFormat() command.BackupRequest_Format {
	f := qp["fmt"]
	switch f {
	case "sql":
		return command.BackupRequest_BACKUP_REQUEST_FORMAT_SQL
	default:
		return command.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY
	}
}

// Query returns the requested query.
func (qp QueryParams) Query() string {
	return qp["q"]
}

// Freshness returns the requested freshness duration.
func (qp QueryParams) Freshness() time.Duration {
	f := qp["freshness"]
	d, _ := time.ParseDuration(f)
	return d
}

// Timeout returns the requested timeout duration.
func (qp QueryParams) Timeout(def time.Duration) time.Duration {
	t, ok := qp["timeout"]
	if !ok {
		return def
	}
	d, _ := time.ParseDuration(t)
	return d
}

// Version returns the requested version.
func (qp QueryParams) Version() string {
	return qp["version"]
}

// HasKey returns true if the given key is present in the query parameters.
func (qp QueryParams) HasKey(k string) bool {
	_, ok := qp[k]
	return ok
}
