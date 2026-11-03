package broker

import (
	"context"
	"testing"
	"time"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func TestRouterWorkerPoolAndMetrics(t *testing.T) {
	b := New(4)
	defer b.Close()

	ch, cancel := b.Subscribe("telemetry")
	defer cancel()

	r := NewRouter(b, 2, 2)
	defer r.Close()

	if err := r.Publish(context.Background(), mustEnvelope(t, "1", "telemetry")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for routed message")
	}

	deadline := time.Now().Add(time.Second)
	for r.Metrics().Delivered == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := r.Metrics(); got.Accepted != 1 || got.Delivered != 1 || got.Dropped != 0 {
		t.Fatalf("unexpected metrics: %+v", got)
	}
}

func TestRouterBackpressureHonorsCancellation(t *testing.T) {
	b := New(1)
	defer b.Close()

	// A subscriber that never drains fills its buffer (1), parks the single
	// worker on the second message (1), and leaves the third in the router
	// queue (1).
	_, cancel := b.Subscribe("telemetry")
	defer cancel()

	r := NewRouter(b, 1, 1)
	defer r.Close()

	msg := mustEnvelope(t, "1", "telemetry")
	for i := 0; i < 3; i++ {
		if err := r.Publish(context.Background(), msg); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()
	if err := r.Publish(ctx, msg); err == nil {
		t.Fatal("expected cancellation error when the queue is full")
	}
	if got := r.Metrics().Dropped; got == 0 {
		t.Fatal("expected the cancelled publish to be counted as dropped")
	}
}

func TestRouterRejectsInvalidEnvelope(t *testing.T) {
	b := New(1)
	defer b.Close()

	r := NewRouter(b, 1, 1)
	defer r.Close()

	if err := r.Publish(context.Background(), envelope.Envelope{}); err == nil {
		t.Fatal("expected invalid envelope to be rejected")
	}
	if got := r.Metrics().Dropped; got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}

func TestRouterPublishAfterClose(t *testing.T) {
	b := New(1)
	defer b.Close()

	r := NewRouter(b, 1, 1)
	r.Close()
	r.Close() // idempotent

	if err := r.Publish(context.Background(), mustEnvelope(t, "1", "telemetry")); err != ErrRouterClosed {
		t.Fatalf("err = %v, want %v", err, ErrRouterClosed)
	}
}

func TestRouterCloseUnblocksPublish(t *testing.T) {
	b := New(1)
	defer b.Close()

	_, cancel := b.Subscribe("telemetry")
	defer cancel()

	// Fill subscriber buffer (1), park the worker (1), and fill the queue (1)
	// so the next Publish is guaranteed to block.
	r := NewRouter(b, 1, 1)
	msg := mustEnvelope(t, "1", "telemetry")
	for i := 0; i < 3; i++ {
		if err := r.Publish(context.Background(), msg); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Publish(context.Background(), msg)
	}()

	time.Sleep(10 * time.Millisecond)
	r.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock a blocked Publish")
	}
}
