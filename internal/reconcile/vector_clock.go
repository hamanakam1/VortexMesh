// Package reconcile decides what to do when two nodes meet again after a
// partition: send local state, request remote state, merge, or do nothing.
package reconcile

import (
	"sync"
	"time"
)

// ConnectionState describes the health of a link to a peer.
type ConnectionState string

const (
	Disconnected ConnectionState = "disconnected"
	Connecting   ConnectionState = "connecting"
	Healthy      ConnectionState = "healthy"
	Degraded     ConnectionState = "degraded"
)

// VectorClock maps a node ID to the highest version that node has seen.
type VectorClock map[string]uint64

// Relation is the causal relationship between two vector clocks.
type Relation int

const (
	Equal Relation = iota
	LocalAhead
	RemoteAhead
	Concurrent
)

// Compare reports how local relates to remote.
//
// Equal means identical clocks, LocalAhead means local has seen everything
// remote has plus more, RemoteAhead is the mirror image, and Concurrent means
// both sides have unique writes so a merge is required.
func Compare(local, remote VectorClock) Relation {
	var localAhead, remoteAhead bool
	seen := make(map[string]struct{}, len(local)+len(remote))
	for node := range local {
		seen[node] = struct{}{}
	}
	for node := range remote {
		seen[node] = struct{}{}
	}
	for node := range seen {
		if local[node] > remote[node] {
			localAhead = true
		}
		if remote[node] > local[node] {
			remoteAhead = true
		}
	}
	switch {
	case localAhead && remoteAhead:
		return Concurrent
	case localAhead:
		return LocalAhead
	case remoteAhead:
		return RemoteAhead
	default:
		return Equal
	}
}

// Merge returns the component-wise maximum of two clocks. It never mutates
// its inputs, so callers can keep the originals for auditing.
func Merge(local, remote VectorClock) VectorClock {
	merged := make(VectorClock, len(local)+len(remote))
	for node, version := range local {
		merged[node] = version
	}
	for node, version := range remote {
		if version > merged[node] {
			merged[node] = version
		}
	}
	return merged
}

// Action is the reconciliation step selected by Plan.
type Action string

const (
	Noop          Action = "noop"
	SendLocal     Action = "send_local"
	RequestRemote Action = "request_remote"
	MergeState    Action = "merge_state"
)

// Plan maps a clock comparison onto the cheapest action that converges.
func Plan(local, remote VectorClock) Action {
	switch Compare(local, remote) {
	case LocalAhead:
		return SendLocal
	case RemoteAhead:
		return RequestRemote
	case Concurrent:
		return MergeState
	default:
		return Noop
	}
}

// Machine tracks link health from heartbeats. It is safe for concurrent use
// because a heartbeat reader and a supervisor typically share it.
type Machine struct {
	mu            sync.Mutex
	state         ConnectionState
	lastHeartbeat time.Time
}

// NewMachine returns a machine in the Disconnected state.
func NewMachine() *Machine { return &Machine{state: Disconnected} }

// State returns the current connection state.
func (m *Machine) State() ConnectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// StartConnect moves a disconnected machine into Connecting. Other states are
// left untouched so a late duplicate call cannot downgrade a healthy link.
func (m *Machine) StartConnect() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == Disconnected {
		m.state = Connecting
	}
}

// Heartbeat records a heartbeat at now and marks the link Healthy.
func (m *Machine) Heartbeat(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastHeartbeat = now
	m.state = Healthy
}

// Check degrades the link when the last heartbeat is older than timeout.
func (m *Machine) Check(now time.Time, timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastHeartbeat.IsZero() || now.Sub(m.lastHeartbeat) > timeout {
		m.state = Degraded
	}
}

// Disconnect resets the machine.
func (m *Machine) Disconnect() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = Disconnected
}
