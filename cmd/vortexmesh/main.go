// Command vortexmesh runs a self-contained demo of the mesh pipeline: one
// validated envelope travels through the bounded router and reaches a
// subscriber, and the process prints the envelope plus router metrics.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hamanakam1/vortexmesh/internal/broker"
	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

const banner = `
__     __         _            __  __           _
\ \   / /__  _ __| |_ _____  _|  \/  | ___  ___| |__
 \ \ / / _ \| '__| __/ _ \ \/ / |\/| |/ _ \/ __| '_ \
  \ V / (_) | |  | ||  __/>  <| |  | |  __/\__ \ | | |
   \_/ \___/|_|   \__\___/_/\_\_|  |_|\___||___/_| |_|
     edge-to-cloud event mesh · validated envelopes · bounded routing
`

func main() {
	topic := flag.String("topic", "telemetry", "topic to publish the demo envelope to")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Stdout, *topic); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, out io.Writer, topic string) error {
	fmt.Fprint(out, banner)

	b := broker.New(16)
	defer b.Close()

	sub, unsubscribe := b.Subscribe(topic)
	defer unsubscribe()

	router := broker.NewRouter(b, 2, 8)
	defer router.Close()

	msg, err := envelope.New(
		"demo-1",
		envelope.Telemetry,
		topic,
		"local-node",
		map[string]any{"status": "online", "phase": "demo"},
	)
	if err != nil {
		return fmt.Errorf("build envelope: %w", err)
	}
	if err := router.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publish envelope: %w", err)
	}

	var received envelope.Envelope
	select {
	case received = <-sub:
	case <-ctx.Done():
		return ctx.Err()
	}

	encoded, err := json.MarshalIndent(received, "", "  ")
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}
	fmt.Fprintln(out, string(encoded))

	// The worker increments Delivered immediately after the broker returns,
	// so allow the counter to settle before reporting it.
	deadline := time.Now().Add(time.Second)
	for router.Metrics().Delivered == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	metrics := router.Metrics()
	fmt.Fprintf(out, "router: accepted=%d delivered=%d dropped=%d\n",
		metrics.Accepted, metrics.Delivered, metrics.Dropped)
	return nil
}
