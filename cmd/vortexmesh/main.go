package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"github.com/hamanakam1/vortexmesh/internal/broker"
	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

func main() {
	topic := flag.String("topic", "telemetry", "topic to demonstrate")
	flag.Parse()
	b := broker.New(16)
	ch, cancel := b.Subscribe(*topic); defer cancel()
	e, err := envelope.New("demo-1", envelope.Telemetry, *topic, "local-node", map[string]any{"status": "online"})
	if err != nil { log.Fatal(err) }
	if err := b.Publish(context.Background(), e); err != nil { log.Fatal(err) }
	out, _ := json.MarshalIndent(<-ch, "", "  "); fmt.Println(string(out))
}
