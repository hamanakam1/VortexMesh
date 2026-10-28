package broker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

type Metrics struct {
	Accepted uint64
	Delivered uint64
	Dropped uint64
}

type Router struct {
	broker *Broker
	queue chan envelope.Envelope
	workers sync.WaitGroup
	accepted atomic.Uint64
	delivered atomic.Uint64
	dropped atomic.Uint64
	closeOnce sync.Once
}

// NewRouter starts a bounded worker pool. Publish only accepts work into the
// bounded queue; workers perform fan-out through the broker.
func NewRouter(b *Broker, workers, queueSize int) *Router {
	if workers < 1 { workers = 1 }
	if queueSize < 1 { queueSize = 1 }
	r := &Router{broker: b, queue: make(chan envelope.Envelope, queueSize)}
	r.workers.Add(workers)
	for i := 0; i < workers; i++ { go r.worker() }
	return r
}

func (r *Router) worker() {
	defer r.workers.Done()
	for msg := range r.queue {
		if r.broker.Publish(context.Background(), msg) == nil { r.delivered.Add(1) } else { r.dropped.Add(1) }
	}
}

func (r *Router) Publish(ctx context.Context, msg envelope.Envelope) error {
	if err := msg.Validate(); err != nil { return err }
	select { case r.queue <- msg: r.accepted.Add(1); return nil; case <-ctx.Done(): r.dropped.Add(1); return ctx.Err() }
}

func (r *Router) Metrics() Metrics { return Metrics{Accepted: r.accepted.Load(), Delivered: r.delivered.Load(), Dropped: r.dropped.Load()} }

func (r *Router) Close() { r.closeOnce.Do(func() { close(r.queue); r.workers.Wait() }) }
