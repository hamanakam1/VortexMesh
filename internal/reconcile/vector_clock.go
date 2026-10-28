package reconcile

import "time"

type ConnectionState string

const (
	Disconnected ConnectionState = "disconnected"
	Connecting ConnectionState = "connecting"
	Healthy ConnectionState = "healthy"
	Degraded ConnectionState = "degraded"
)

type VectorClock map[string]uint64

type Relation int

const (
	Equal Relation = iota
	LocalAhead
	RemoteAhead
	Concurrent
)

func Compare(local, remote VectorClock) Relation {
	localAhead, remoteAhead := false, false
	seen := make(map[string]struct{}, len(local)+len(remote))
	for node := range local { seen[node] = struct{}{} }
	for node := range remote { seen[node] = struct{}{} }
	for node := range seen { if local[node] > remote[node] { localAhead = true }; if remote[node] > local[node] { remoteAhead = true } }
	switch { case localAhead && remoteAhead: return Concurrent; case localAhead: return LocalAhead; case remoteAhead: return RemoteAhead; default: return Equal }
}

func Merge(local, remote VectorClock) VectorClock {
	merged := make(VectorClock, len(local)+len(remote))
	for node, version := range local { merged[node] = version }
	for node, version := range remote { if version > merged[node] { merged[node] = version } }
	return merged
}

type Action string

const (
	Noop Action = "noop"
	SendLocal Action = "send_local"
	RequestRemote Action = "request_remote"
	MergeState Action = "merge_state"
)

func Plan(local, remote VectorClock) Action { switch Compare(local, remote) { case LocalAhead: return SendLocal; case RemoteAhead: return RequestRemote; case Concurrent: return MergeState; default: return Noop } }

type Machine struct { state ConnectionState; lastHeartbeat time.Time }

func NewMachine() *Machine { return &Machine{state: Disconnected} }
func (m *Machine) State() ConnectionState { return m.state }
func (m *Machine) StartConnect() { if m.state == Disconnected { m.state = Connecting } }
func (m *Machine) Heartbeat(now time.Time) { m.lastHeartbeat = now; m.state = Healthy }
func (m *Machine) Check(now time.Time, timeout time.Duration) { if m.lastHeartbeat.IsZero() || now.Sub(m.lastHeartbeat) > timeout { m.state = Degraded } }
func (m *Machine) Disconnect() { m.state = Disconnected }
