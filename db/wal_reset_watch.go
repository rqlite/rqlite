package db

// walResetWatch records state carried forward from a checkpoint attempt
// that moved all pages from the WAL to the database but failed to truncate
// the WAL itself. In that situation SQLite may, on the next write, either
// append to the existing WAL or reset it from the start — and the only way
// to tell which happened is to compare the WAL header's salt against the
// salt observed at the time of the partial-checkpoint.
//
// While armed the watch carries the salt to compare against and the frame
// index to resume reading from if the WAL has not been reset. The zero
// value is the unarmed state; all methods are safe on the zero value.
type walResetWatch struct {
	armed          bool
	salt           Salt
	resumeFrameIdx int64
}

// arm starts the watch, recording the salt observed and the frame index
// from which the next checkpoint should resume if the WAL has not been
// reset in the meantime.
func (w *walResetWatch) arm(s Salt, resumeFrameIdx int64) {
	*w = walResetWatch{armed: true, salt: s, resumeFrameIdx: resumeFrameIdx}
}

// disarm clears the watch.
func (w *walResetWatch) disarm() {
	*w = walResetWatch{}
}

// check returns the frame index at which the next checkpoint should begin
// reading the WAL, and whether a WAL reset was detected since the watch
// was armed. When the watch is not armed it returns (0, false). On reset
// detection the watch is disarmed: the prior resume frame index refers to
// a WAL state that no longer exists.
func (w *walResetWatch) check(current Salt) (frameIdx int64, walReset bool) {
	if !w.armed {
		return 0, false
	}
	if w.salt.Equal(current) {
		return w.resumeFrameIdx, false
	}
	w.disarm()
	return 0, true
}
