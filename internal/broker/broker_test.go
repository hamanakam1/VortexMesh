package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func mustEnvelope(t *testing.T, id, topic string) envelope.Envelope {
	t.Helper()
	e, err := envelope.New(id, envelope.Telemetry, topic, "node-a", map[string]bool{"online": true})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	return e
}

func TestPublishSubscribe(t *testing.T) {
	b := New(1)
	defer b.Close()

	ch, cancel := b.Subscribe("telemetry")
	defer cancel()

	if err := b.Publish(context.Background(), mustEnvelope(t, "1", "telemetry")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case got := <-ch:
		if got.ID != "1" {
			t.Fatalf("got %q, want %q", got.ID, "1")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPublishRejectsInvalidEnvelope(t *testing.T) {
	b := New(1)
	defer b.Close()

	if err := b.Publish(context.Background(), envelope.Envelope{}); err == nil {
		t.Fatal("expected invalid envelope to be rejected")
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	b := New(1)
	ch, cancel := b.Subscribe("telemetry")
	cancel()
	cancel() // idempotent

	if _, ok := <-ch; ok {
		t.Fatal("expected channel to be closed after unsubscribe")
	}
	if n := b.Subscribers("telemetry"); n != 0 {
		t.Fatalf("subscribers = %d, want 0", n)
	}
}

func TestPublishAfterCloseReturnsError(t *testing.T) {
	b := New(1)
	b.Close()
	b.Close() // idempotent

	if err := b.Publish(context.Background(), mustEnvelope(t, "1", "telemetry")); err != ErrClosed {
		t.Fatalf("err = %v, want %v", err, ErrClosed)
	}
}

// TestConcurrentPublishAndUnsubscribe exercises the send/close race that would
// otherwise panic with "send on closed channel". Run with -race.
func TestConcurrentPublishAndUnsubscribe(t *testing.T) {
	b := New(1)
	defer b.Close()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := b.Subscribe("telemetry")
			go func() {
				for range ch {
				}
			}()
			ctx, cancelCtx := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancelCtx()
			_ = b.Publish(ctx, mustEnvelope(t, "race", "telemetry"))
			cancel()
		}()
	}
	wg.Wait()
}

func TestPublishHonorsCancellationOnFullBuffer(t *testing.T) {
	b := New(1)
	defer b.Close()

	_, cancel := b.Subscribe("telemetry")
	defer cancel()

	if err := b.Publish(context.Background(), mustEnvelope(t, "1", "telemetry")); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	ctx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()
	if err := b.Publish(ctx, mustEnvelope(t, "2", "telemetry")); err == nil {
		t.Fatal("expected cancellation error on full buffer")
	}
}

func TestSubscribersCountsPerTopic(t *testing.T) {
	b := New(1)
	defer b.Close()

	_, cancelA := b.Subscribe("a")
	defer cancelA()
	_, cancelB := b.Subscribe("b")
	defer cancelB()

	if n := b.Subscribers("a"); n != 1 {
		t.Fatalf("subscribers(a) = %d, want 1", n)
	}
	if n := b.Subscribers("missing"); n != 0 {
		t.Fatalf("subscribers(missing) = %d, want 0", n)
	}
}
