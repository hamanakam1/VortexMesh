package broker

import (
	"context"
	"testing"
	"time"
	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func TestRouterWorkerPoolAndMetrics(t *testing.T) {
	b := New(4); ch, cancel := b.Subscribe("telemetry"); defer cancel()
	r := NewRouter(b, 2, 2); defer r.Close()
	msg, err := envelope.New("1", envelope.Telemetry, "telemetry", "node-a", map[string]bool{"online": true})
	if err != nil { t.Fatal(err) }
	if err := r.Publish(context.Background(), msg); err != nil { t.Fatal(err) }
	select { case <-ch: case <-time.After(time.Second): t.Fatal("timed out waiting for routed message") }
	deadline := time.Now().Add(time.Second)
	for r.Metrics().Delivered == 0 && time.Now().Before(deadline) { time.Sleep(time.Millisecond) }
	if got := r.Metrics(); got.Accepted != 1 || got.Delivered != 1 || got.Dropped != 0 { t.Fatalf("unexpected metrics: %+v", got) }
}

func TestRouterBackpressureHonorsCancellation(t *testing.T) {
	b := New(1); r := NewRouter(b, 1, 1); defer r.Close()
	// Block the worker on a subscriber with a full buffer.
	_, cancel := b.Subscribe("telemetry"); defer cancel()
	msg, _ := envelope.New("1", envelope.Telemetry, "telemetry", "node-a", map[string]bool{"online": true})
	_ = r.Publish(context.Background(), msg)
	_ = r.Publish(context.Background(), msg)
	ctx, cancelCtx := context.WithCancel(context.Background()); cancelCtx()
	if err := r.Publish(ctx, msg); err == nil { t.Fatal("expected cancellation") }
}
