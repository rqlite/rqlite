package snapshot

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SnapshotNamer generates names for Snapshots in the Store.
type SnapshotNamer struct {
	nowFn func() time.Time
}

// NewSnapshotNamer returns a SnapshotName. If nowFn is nil, it
// uses time.Now.
func NewSnapshotNamer(nowFn func() time.Time) *SnapshotNamer {
	if nowFn == nil {
		return &SnapshotNamer{time.Now}
	}
	return &SnapshotNamer{nowFn}
}

// MakeName returns a name for the Snapshot, for the given the term and index.
func (sn *SnapshotNamer) MakeName(term, index uint64) string {
	now := sn.nowFn()
	msec := now.UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// ParseSnapshotName splits a name into its three fields. It requires a
// non-negative timestamp, since a negative one introduces a fourth field.
func ParseSnapshotName(name string) (term, index uint64, msec int64, retErr error) {
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		return 0, 0, 0, fmt.Errorf("name does not have 3 parts")
	}
	term, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("name has bad term field: %s", err)
	}
	index, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("name has bad index field: %s", err)
	}
	msec, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("name has bad timestamp field: %s", err)
	}
	return term, index, msec, nil
}
