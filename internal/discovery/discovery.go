// Package discovery finds peers and expires stale advertisements.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

// Peer is one advertised mesh node.
type Peer struct {
	ID       string
	Address  string
	Port     int
	Protocol string
	LastSeen time.Time
}

// Addr returns the dialable host:port for the peer.
func (p Peer) Addr() string { return net.JoinHostPort(p.Address, fmt.Sprint(p.Port)) }

// Validate reports whether the peer has the fields needed to dial it.
func (p Peer) Validate() error {
	if p.ID == "" {
		return errors.New("peer id is required")
	}
	if p.Address == "" {
		return errors.New("peer address is required")
	}
	if p.Port < 1 || p.Port > 65535 {
		return fmt.Errorf("peer port %d is out of range", p.Port)
	}
	return nil
}

// Service is the discovery contract. A production adapter can back it with
// mDNS or gossip without changing callers.
type Service interface {
	Advertise(ctx context.Context, peer Peer) error
	Peers(ctx context.Context) ([]Peer, error)
	Close() error
}

// Registry is a deterministic in-memory discovery implementation for tests and
// single-host development.
//
// Determinism matters: Peers returns advertisements sorted by ID so callers
// and tests do not depend on Go's randomised map iteration order.
type Registry struct {
	mu    sync.Mutex
	peers map[string]Peer
	ttl   time.Duration
}

// NewRegistry returns a registry that expires advertisements older than ttl.
func NewRegistry(ttl time.Duration) *Registry {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &Registry{peers: make(map[string]Peer), ttl: ttl}
}

// Advertise records or refreshes a peer. The peer's LastSeen is always set by
// the registry so a caller cannot keep a dead peer alive with a stale clock.
func (r *Registry) Advertise(ctx context.Context, peer Peer) error {
	if err := peer.Validate(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	peer.LastSeen = time.Now().UTC()
	r.mu.Lock()
	r.peers[peer.ID] = peer
	r.mu.Unlock()
	return nil
}

// Peers returns live advertisements sorted by peer ID, dropping expired ones.
func (r *Registry) Peers(ctx context.Context) ([]Peer, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	now := time.Now()
	r.mu.Lock()
	peers := make([]Peer, 0, len(r.peers))
	for id, peer := range r.peers {
		if now.Sub(peer.LastSeen) > r.ttl {
			delete(r.peers, id)
			continue
		}
		peers = append(peers, peer)
	}
	r.mu.Unlock()

	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	return peers, nil
}

// Close releases registry resources. The in-memory registry has none.
func (r *Registry) Close() error { return nil }
