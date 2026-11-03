package discovery

import (
	"context"
	"testing"
	"time"
)

func TestRegistryAdvertiseAndExpire(t *testing.T) {
	r := NewRegistry(20 * time.Millisecond)
	defer func() { _ = r.Close() }()

	peer := Peer{ID: "edge-a", Address: "127.0.0.1", Port: 4242, Protocol: "vortexmesh/1"}
	if err := r.Advertise(context.Background(), peer); err != nil {
		t.Fatalf("Advertise: %v", err)
	}

	peers, err := r.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 1 || peers[0].Addr() != "127.0.0.1:4242" {
		t.Fatalf("unexpected peers: %+v", peers)
	}

	time.Sleep(30 * time.Millisecond)
	peers, err = r.Peers(context.Background())
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected expired peer, got %+v", peers)
	}
}

// TestPeersAreSortedByID guards the determinism the registry promises.
func TestPeersAreSortedByID(t *testing.T) {
	r := NewRegistry(time.Minute)
	defer func() { _ = r.Close() }()

	for _, id := range []string{"c", "a", "b"} {
		if err := r.Advertise(context.Background(), Peer{ID: id, Address: "127.0.0.1", Port: 1}); err != nil {
			t.Fatalf("Advertise %s: %v", id, err)
		}
	}

	for attempt := 0; attempt < 20; attempt++ {
		peers, err := r.Peers(context.Background())
		if err != nil {
			t.Fatalf("Peers: %v", err)
		}
		if len(peers) != 3 {
			t.Fatalf("len = %d, want 3", len(peers))
		}
		for i := 1; i < len(peers); i++ {
			if peers[i-1].ID > peers[i].ID {
				t.Fatalf("peers not sorted: %q before %q", peers[i-1].ID, peers[i].ID)
			}
		}
	}
}

func TestRegistryRejectsInvalidPeers(t *testing.T) {
	r := NewRegistry(time.Minute)
	defer func() { _ = r.Close() }()

	tests := map[string]Peer{
		"missing id":      {Address: "127.0.0.1", Port: 1},
		"missing address": {ID: "a", Port: 1},
		"port too low":    {ID: "a", Address: "127.0.0.1", Port: 0},
		"port too high":   {ID: "a", Address: "127.0.0.1", Port: 70000},
	}
	for name, peer := range tests {
		t.Run(name, func(t *testing.T) {
			if err := r.Advertise(context.Background(), peer); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestRegistryHonorsCancellation(t *testing.T) {
	r := NewRegistry(time.Minute)
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Advertise(ctx, Peer{ID: "edge-a", Address: "localhost", Port: 1}); err == nil {
		t.Fatal("expected cancellation from Advertise")
	}
	if _, err := r.Peers(ctx); err == nil {
		t.Fatal("expected cancellation from Peers")
	}
}
