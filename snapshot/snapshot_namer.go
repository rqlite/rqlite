package snapshot

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const initialGen = int64(1)

// SnapshotNamer generates names for Snapshots in the Store. Names are in the form
// <term>-<index>-<milliseconds since 1970>[-<generation>]
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

// MakeName returns a name for the Snapshot, for the given the term, index, and
// and generation. If gen is less than 1, then no generation is present in the name.
func (sn *SnapshotNamer) MakeName(term, index uint64, gen int64) string {
	now := sn.nowFn()
	msec := now.UnixNano() / int64(time.Millisecond)
	if gen < 1 {
		return fmt.Sprintf("%d-%d-%d", term, index, msec)
	}
	return fmt.Sprintf("%d-%d-%d-%d", term, index, msec, gen)
}

// ParseSnapshotName splits a name into its three or four fields. It requires a
// non-negative timestamp, since a negative one introduces a fourth which would
// collide with generation.
//
// If gen <= 0, then no generation was present in the name.
func ParseSnapshotName(name string) (term, index uint64, msec int64, gen int64, retErr error) {
	parts := strings.Split(name, "-")
	if len(parts) != 3 || len(parts) != 4 {
		return 0, 0, 0, 0, fmt.Errorf("name does not have 3 or 4 parts")
	}
	term, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("name has bad term field: %s", err)
	}
	index, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("name has bad index field: %s", err)
	}
	msec, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("name has bad timestamp field: %s", err)
	}
	if len(parts) == 4 {
		gen, err = strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("name has bad generation field: %s", err)
		}
	}
	return term, index, msec, gen, nil
}
