// Package broker provides an in-process, bounded pub/sub fan-out used by the
// mesh before messages reach the network or the write-ahead log.
package broker

import (
	"context"
	"errors"
	"sync"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

// ErrClosed is returned by Publish once the broker has been closed.
var ErrClosed = errors.New("broker: closed")

// Broker fans validated envelopes out to per-topic subscribers.
//
// Delivery is best effort with a bounded per-subscriber buffer. A slow
// subscriber applies backpressure to the publisher instead of silently
// growing memory, which is the behaviour an intermittent edge link needs.
type Broker struct {
	mu     sync.RWMutex
	subs   map[string]map[*subscription]struct{}
	buffer int
	closed bool
}

// subscription owns one channel and the synchronisation that makes closing it
// safe while a publisher is mid-send.
type subscription struct {
	ch   chan envelope.Envelope
	done chan struct{}

	once   sync.Once
	mu     sync.RWMutex
	closed bool
}

func newSubscription(buffer int) *subscription {
	return &subscription{
		ch:   make(chan envelope.Envelope, buffer),
		done: make(chan struct{}),
	}
}

// send delivers e unless the subscription is closed or ctx is cancelled.
//
// The read lock is held across the send so close(s.ch) cannot race with it,
// and s.done is closed *before* close takes the write lock so a send parked
// on a full buffer wakes up immediately when the subscriber unsubscribes.
func (s *subscription) send(ctx context.Context, e envelope.Envelope) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return false
	}
	select {
	case s.ch <- e:
		return true
	case <-s.done:
		return false
	case <-ctx.Done():
		return false
	}
}

func (s *subscription) close() {
	s.once.Do(func() {
		close(s.done)
		s.mu.Lock()
		s.closed = true
		close(s.ch)
		s.mu.Unlock()
	})
}

// New returns a broker whose subscribers each buffer up to buffer messages.
func New(buffer int) *Broker {
	if buffer < 1 {
		buffer = 1
	}
	return &Broker{
		subs:   make(map[string]map[*subscription]struct{}),
		buffer: buffer,
	}
}

// Subscribe returns a channel for topic plus an idempotent unsubscribe func.
// The channel is closed by unsubscribe, so consumers may range over it.
func (b *Broker) Subscribe(topic string) (<-chan envelope.Envelope, func()) {
	sub := newSubscription(b.buffer)

	b.mu.Lock()
	if b.subs[topic] == nil {
		b.subs[topic] = make(map[*subscription]struct{})
	}
	b.subs[topic][sub] = struct{}{}
	b.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs[topic], sub)
			if len(b.subs[topic]) == 0 {
				delete(b.subs, topic)
			}
			b.mu.Unlock()
			sub.close()
		})
	}
	return sub.ch, cancel
}

// Publish validates e and delivers it to every subscriber of e.Topic.
//
// It returns ctx.Err() if a subscriber buffer is full and the context is
// cancelled first, which lets the router shed load rather than stall.
func (b *Broker) Publish(ctx context.Context, e envelope.Envelope) error {
	if err := e.Validate(); err != nil {
		return err
	}

	b.mu.RLock()
	closed := b.closed
	targets := make([]*subscription, 0, len(b.subs[e.Topic]))
	for sub := range b.subs[e.Topic] {
		targets = append(targets, sub)
	}
	b.mu.RUnlock()

	if closed {
		return ErrClosed
	}
	for _, sub := range targets {
		if !sub.send(ctx, e) {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Subscribers reports how many live subscriptions a topic currently has.
func (b *Broker) Subscribers(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[topic])
}

// Close stops the broker from accepting new messages and closes every
// subscriber channel. It is safe to call more than once.
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	subs := make([]*subscription, 0, len(b.subs))
	for _, topic := range b.subs {
		for sub := range topic {
			subs = append(subs, sub)
		}
	}
	b.subs = make(map[string]map[*subscription]struct{})
	b.mu.Unlock()

	for _, sub := range subs {
		sub.close()
	}
}
