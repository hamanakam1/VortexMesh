package broker

import (
	"context"
	"sync"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

type Broker struct {
	mu sync.RWMutex
	subs map[string]map[chan envelope.Envelope]struct{}
	buffer int
}

func New(buffer int) *Broker { if buffer < 1 { buffer = 1 }; return &Broker{subs: make(map[string]map[chan envelope.Envelope]struct{}), buffer: buffer} }

func (b *Broker) Subscribe(topic string) (<-chan envelope.Envelope, func()) {
	c := make(chan envelope.Envelope, b.buffer)
	b.mu.Lock(); if b.subs[topic] == nil { b.subs[topic] = make(map[chan envelope.Envelope]struct{}) }; b.subs[topic][c] = struct{}{}; b.mu.Unlock()
	return c, func() { b.mu.Lock(); if _, ok := b.subs[topic][c]; ok { delete(b.subs[topic], c); close(c) }; b.mu.Unlock() }
}

func (b *Broker) Publish(ctx context.Context, e envelope.Envelope) error {
	if err := e.Validate(); err != nil { return err }
	b.mu.RLock(); targets := make([]chan envelope.Envelope, 0, len(b.subs[e.Topic])); for c := range b.subs[e.Topic] { targets = append(targets, c) }; b.mu.RUnlock()
	for _, c := range targets { select { case c <- e: case <-ctx.Done(): return ctx.Err() } }
	return nil
}
