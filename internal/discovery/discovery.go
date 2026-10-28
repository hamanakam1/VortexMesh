package discovery

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type Peer struct {
	ID string
	Address string
	Port int
	Protocol string
	LastSeen time.Time
}

func (p Peer) Addr() string { return net.JoinHostPort(p.Address, fmt.Sprint(p.Port)) }

type Service interface {
	Advertise(context.Context, Peer) error
	Peers(context.Context) ([]Peer, error)
	Close() error
}

// Registry is a deterministic discovery implementation for tests and single-host
// development. A production adapter can map these operations to mDNS or gossip.
type Registry struct {
	mu sync.RWMutex
	peers map[string]Peer
	ttl time.Duration
}

func NewRegistry(ttl time.Duration) *Registry { if ttl <= 0 { ttl = time.Minute }; return &Registry{peers: make(map[string]Peer), ttl: ttl} }

func (r *Registry) Advertise(ctx context.Context, peer Peer) error {
	if peer.ID == "" || peer.Address == "" || peer.Port < 1 || peer.Port > 65535 { return fmt.Errorf("peer id, address, and valid port are required") }
	select { case <-ctx.Done(): return ctx.Err(); default: }
	peer.LastSeen = time.Now().UTC()
	r.mu.Lock(); r.peers[peer.ID] = peer; r.mu.Unlock()
	return nil
}

func (r *Registry) Peers(ctx context.Context) ([]Peer, error) {
	select { case <-ctx.Done(): return nil, ctx.Err(); default: }
	now := time.Now()
	r.mu.Lock(); defer r.mu.Unlock()
	peers := make([]Peer, 0, len(r.peers))
	for id, peer := range r.peers { if now.Sub(peer.LastSeen) > r.ttl { delete(r.peers, id); continue }; peers = append(peers, peer) }
	return peers, nil
}

func (r *Registry) Close() error { return nil }
