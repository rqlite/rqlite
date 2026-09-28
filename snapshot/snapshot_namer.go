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

// NewSnapshotNamer returns a SnapshotNamer. If nowFn is nil, it
// uses time.Now.
func NewSnapshotNamer(nowFn func() time.Time) *SnapshotNamer {
	if nowFn == nil {
		return &SnapshotNamer{time.Now}
	}
	return &SnapshotNamer{nowFn}
}

// MakeName returns a name with three fields: term, index, and milliseconds.
//
// The name is generated so that it sorts as newer than every snapshot in the
// set sharing the given term and index: the millisecond field is the larger of
// the current wall-clock time and one more than the millisecond field of the
// newest such snapshot. Ordering therefore stays correct even if the system
// clock moved backwards since those snapshots were created.
func (sn *SnapshotNamer) MakeName(set SnapshotSet, term, index uint64) string {
	// Keep timestamps non-negative so the name always has exactly three fields.
	minMsec := int64(0)
	if newest, ok := set.WithTermIndex(term, index).Newest(); ok {
		// A snapshot whose ID has no parsable timestamp cannot take part in
		// the floor, so it is simply ignored.
		if _, _, msec, err := ParseSnapshotName(newest.id); err == nil {
			minMsec = msec + 1
		}
	}
	return sn.makeName(term, index, minMsec)
}

// makeName returns a name for the Snapshot, as MakeName does, but the
// millisecond field is raised to minMsec if the current time is lower.
func (sn *SnapshotNamer) makeName(term, index uint64, minMsec int64) string {
	now := sn.nowFn()
	msec := now.UnixNano() / int64(time.Millisecond)
	if msec < minMsec {
		msec = minMsec
	}
	return fmt.Sprintf("%d-%d-%d", term, index, msec)
}

// ParseSnapshotName splits a name into exactly three fields: term, index, and
// milliseconds. All fields must be non-negative.
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
