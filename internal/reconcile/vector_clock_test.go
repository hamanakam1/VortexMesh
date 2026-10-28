package reconcile

import (
	"testing"
	"time"
)

func TestVectorClockRelations(t *testing.T) {
	if Compare(VectorClock{"a": 2}, VectorClock{"a": 1}) != LocalAhead { t.Fatal("expected local ahead") }
	if Compare(VectorClock{"a": 1}, VectorClock{"a": 2}) != RemoteAhead { t.Fatal("expected remote ahead") }
	if Compare(VectorClock{"a": 2}, VectorClock{"b": 2}) != Concurrent { t.Fatal("expected concurrent") }
	if Plan(VectorClock{"a": 1}, VectorClock{"a": 1}) != Noop { t.Fatal("expected noop") }
}

func TestMergeTakesMaximumVersion(t *testing.T) {
	got := Merge(VectorClock{"a": 1, "b": 4}, VectorClock{"a": 3, "c": 2})
	if got["a"] != 3 || got["b"] != 4 || got["c"] != 2 { t.Fatalf("unexpected merge: %#v", got) }
}

func TestHeartbeatStateMachine(t *testing.T) {
	m := NewMachine(); if m.State() != Disconnected { t.Fatal("expected disconnected") }
	m.StartConnect(); if m.State() != Connecting { t.Fatal("expected connecting") }
	now := time.Unix(100, 0); m.Heartbeat(now); if m.State() != Healthy { t.Fatal("expected healthy") }
	m.Check(now.Add(11*time.Second), 10*time.Second); if m.State() != Degraded { t.Fatal("expected degraded") }
	m.Disconnect(); if m.State() != Disconnected { t.Fatal("expected disconnected") }
}
