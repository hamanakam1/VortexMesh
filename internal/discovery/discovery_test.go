package discovery

import (
	"context"
	"testing"
	"time"
)

func TestRegistryAdvertiseAndExpire(t *testing.T) {
	r := NewRegistry(20 * time.Millisecond); defer r.Close()
	peer := Peer{ID: "edge-a", Address: "127.0.0.1", Port: 4242, Protocol: "vortexmesh/1"}
	if err := r.Advertise(context.Background(), peer); err != nil { t.Fatal(err) }
	peers, err := r.Peers(context.Background()); if err != nil { t.Fatal(err) }
	if len(peers) != 1 || peers[0].Addr() != "127.0.0.1:4242" { t.Fatalf("unexpected peers: %+v", peers) }
	time.Sleep(30 * time.Millisecond)
	peers, err = r.Peers(context.Background()); if err != nil { t.Fatal(err) }; if len(peers) != 0 { t.Fatalf("expected expired peer: %+v", peers) }
}

func TestRegistryHonorsCancellation(t *testing.T) {
	r := NewRegistry(time.Minute); defer r.Close(); ctx, cancel := context.WithCancel(context.Background()); cancel()
	if err := r.Advertise(ctx, Peer{ID: "edge-a", Address: "localhost", Port: 1}); err == nil { t.Fatal("expected cancellation") }
	if _, err := r.Peers(ctx); err == nil { t.Fatal("expected cancellation") }
}
