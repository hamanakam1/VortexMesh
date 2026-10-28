package broker

import (
	"context"
	"testing"
	"time"
	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func TestPublishSubscribe(t *testing.T) {
	b := New(1); ch, closeSub := b.Subscribe("telemetry"); defer closeSub()
	e, _ := envelope.New("1", envelope.Telemetry, "telemetry", "node-a", map[string]bool{"online": true})
	if err := b.Publish(context.Background(), e); err != nil { t.Fatal(err) }
	select { case got := <-ch: if got.ID != "1" { t.Fatalf("got %s", got.ID) }; case <-time.After(time.Second): t.Fatal("timed out") }
}
