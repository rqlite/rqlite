package snapshot

import (
	"testing"
	"time"
)

func Test_Observer(t *testing.T) {
	ch := make(chan ReapObservation, 2)
	obs := NewObserver(ch, nil)

	set := newObserverSet()
	set.register(obs)

	set.notify(ReapObservation{SnapshotsReaped: 3, WALsReaped: 1, Duration: 42 * time.Millisecond})
	set.notify(ReapObservation{SnapshotsReaped: 5, WALsReaped: 2, Duration: 100 * time.Millisecond})

	if obs.GetNumObserved() != 2 {
		t.Fatalf("expected 2 observed, got %d", obs.GetNumObserved())
	}
	if obs.GetNumDropped() != 0 {
		t.Fatalf("expected 0 dropped, got %d", obs.GetNumDropped())
	}

	o := <-ch
	if o.SnapshotsReaped != 3 || o.WALsReaped != 1 || o.Duration != 42*time.Millisecond {
		t.Fatalf("unexpected first observation: %+v", o)
	}
	o = <-ch
	if o.SnapshotsReaped != 5 || o.WALsReaped != 2 || o.Duration != 100*time.Millisecond {
		t.Fatalf("unexpected second observation: %+v", o)
	}
}

func Test_ObserverDropsWhenFull(t *testing.T) {
	ch := make(chan ReapObservation, 1)
	obs := NewObserver(ch, nil)

	set := newObserverSet()
	set.register(obs)

	set.notify(ReapObservation{SnapshotsReaped: 1, WALsReaped: 0, Duration: 10 * time.Millisecond})
	set.notify(ReapObservation{SnapshotsReaped: 2, WALsReaped: 0, Duration: 20 * time.Millisecond})

	if obs.GetNumObserved() != 1 {
		t.Fatalf("expected 1 observed, got %d", obs.GetNumObserved())
	}
	if obs.GetNumDropped() != 1 {
		t.Fatalf("expected 1 dropped, got %d", obs.GetNumDropped())
	}
}

func Test_ObserverWithFilter(t *testing.T) {
	ch := make(chan ReapObservation, 10)
	filter := func(o *ReapObservation) bool {
		return o.SnapshotsReaped > 0
	}
	obs := NewObserver(ch, filter)

	set := newObserverSet()
	set.register(obs)

	// This should be filtered out (0 snapshots reaped).
	set.notify(ReapObservation{SnapshotsReaped: 0, WALsReaped: 5, Duration: 10 * time.Millisecond})
	// This should pass the filter.
	set.notify(ReapObservation{SnapshotsReaped: 2, WALsReaped: 1, Duration: 50 * time.Millisecond})

	if obs.GetNumObserved() != 1 {
		t.Fatalf("expected 1 observed, got %d", obs.GetNumObserved())
	}
	if obs.GetNumDropped() != 0 {
		t.Fatalf("expected 0 dropped, got %d", obs.GetNumDropped())
	}

	o := <-ch
	if o.SnapshotsReaped != 2 {
		t.Fatalf("unexpected observation: %+v", o)
	}
}

func Test_ObserverDeregister(t *testing.T) {
	ch := make(chan ReapObservation, 10)
	obs := NewObserver(ch, nil)

	set := newObserverSet()
	set.register(obs)
	set.deregister(obs)

	set.notify(ReapObservation{SnapshotsReaped: 1, WALsReaped: 0, Duration: 10 * time.Millisecond})

	if obs.GetNumObserved() != 0 {
		t.Fatalf("expected 0 observed after deregister, got %d", obs.GetNumObserved())
	}
	if len(ch) != 0 {
		t.Fatalf("expected empty channel after deregister")
	}
}

func Test_ObserverMultipleObservers(t *testing.T) {
	ch1 := make(chan ReapObservation, 10)
	ch2 := make(chan ReapObservation, 10)
	obs1 := NewObserver(ch1, nil)
	obs2 := NewObserver(ch2, nil)

	set := newObserverSet()
	set.register(obs1)
	set.register(obs2)

	set.notify(ReapObservation{SnapshotsReaped: 4, WALsReaped: 2, Duration: 77 * time.Millisecond})

	if obs1.GetNumObserved() != 1 {
		t.Fatalf("expected 1 observed for obs1, got %d", obs1.GetNumObserved())
	}
	if obs2.GetNumObserved() != 1 {
		t.Fatalf("expected 1 observed for obs2, got %d", obs2.GetNumObserved())
	}

	o1 := <-ch1
	o2 := <-ch2
	if o1.SnapshotsReaped != 4 || o2.SnapshotsReaped != 4 {
		t.Fatalf("unexpected observations: %+v, %+v", o1, o2)
	}
}
