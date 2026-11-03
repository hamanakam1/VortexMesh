package reconcile

import (
	"sync"
	"testing"
	"time"
)

func TestVectorClockRelations(t *testing.T) {
	tests := []struct {
		name          string
		local, remote VectorClock
		want          Relation
	}{
		{"local ahead", VectorClock{"a": 2}, VectorClock{"a": 1}, LocalAhead},
		{"remote ahead", VectorClock{"a": 1}, VectorClock{"a": 2}, RemoteAhead},
		{"concurrent", VectorClock{"a": 2}, VectorClock{"b": 2}, Concurrent},
		{"equal", VectorClock{"a": 1, "b": 2}, VectorClock{"a": 1, "b": 2}, Equal},
		{"empty", VectorClock{}, VectorClock{}, Equal},
		{"local ahead on shared and new node", VectorClock{"a": 3, "b": 1}, VectorClock{"a": 2}, LocalAhead},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Compare(tc.local, tc.remote); got != tc.want {
				t.Fatalf("Compare = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPlan(t *testing.T) {
	tests := []struct {
		local, remote VectorClock
		want          Action
	}{
		{VectorClock{"a": 2}, VectorClock{"a": 1}, SendLocal},
		{VectorClock{"a": 1}, VectorClock{"a": 2}, RequestRemote},
		{VectorClock{"a": 2}, VectorClock{"b": 2}, MergeState},
		{VectorClock{"a": 1}, VectorClock{"a": 1}, Noop},
	}
	for _, tc := range tests {
		if got := Plan(tc.local, tc.remote); got != tc.want {
			t.Fatalf("Plan(%v, %v) = %v, want %v", tc.local, tc.remote, got, tc.want)
		}
	}
}

func TestMergeTakesMaximumVersion(t *testing.T) {
	local := VectorClock{"a": 1, "b": 4}
	remote := VectorClock{"a": 3, "c": 2}

	got := Merge(local, remote)
	if got["a"] != 3 || got["b"] != 4 || got["c"] != 2 {
		t.Fatalf("unexpected merge: %#v", got)
	}
	if local["a"] != 1 {
		t.Fatal("Merge mutated the local clock")
	}
}

func TestHeartbeatStateMachine(t *testing.T) {
	m := NewMachine()
	if m.State() != Disconnected {
		t.Fatalf("state = %v, want disconnected", m.State())
	}

	m.StartConnect()
	if m.State() != Connecting {
		t.Fatalf("state = %v, want connecting", m.State())
	}

	now := time.Unix(100, 0)
	m.Heartbeat(now)
	if m.State() != Healthy {
		t.Fatalf("state = %v, want healthy", m.State())
	}

	m.Check(now.Add(11*time.Second), 10*time.Second)
	if m.State() != Degraded {
		t.Fatalf("state = %v, want degraded", m.State())
	}

	m.Disconnect()
	if m.State() != Disconnected {
		t.Fatalf("state = %v, want disconnected", m.State())
	}
}

func TestMachineIsConcurrencySafe(t *testing.T) {
	m := NewMachine()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.StartConnect()
			m.Heartbeat(time.Now())
			m.Check(time.Now(), time.Second)
			_ = m.State()
		}()
	}
	wg.Wait()
}
