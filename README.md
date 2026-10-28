# VortexMesh

VortexMesh is a learning-oriented distributed edge-to-cloud event mesh in Go. It is designed for local agent swarms, central services, and intermittent links such as satellite backhaul.

## Current implementation

The first vertical slice covers the protocol envelope and an in-process concurrent pub/sub broker. Phase 2 now adds a QUIC transport boundary:

- versioned JSON envelopes for task requests, telemetry heartbeats, and state sync;
- strict validation before serialization or publication;
- topic subscriptions with buffered channels;
- context-aware publishing for backpressure and cancellation;
- tests that demonstrate the basic message flow.
- QUIC listener lifecycle with context cancellation;
- TLS 1.3 with required, verified client certificates;
- bounded length-prefixed stream framing to prevent oversized messages;
- transport handler callbacks that receive only validated envelopes.
- bounded worker-pool routing with accepted, delivered, and dropped metrics;
- cancellation-aware ingress when the queue is full.

Run it with:

```sh
go test ./...
go run ./cmd/vortexmesh -topic telemetry
```

## Learning path

Each phase will add one production concern while keeping the previous interfaces testable:

1. Envelope schema and validation (implemented)
2. QUIC transport with mTLS (transport boundary implemented; dependency download and live certificate acceptance remain to be exercised)
3. Concurrent pub/sub routing (implemented with bounded worker pool and metrics)
4. SQLite-backed offline WAL
5. Heartbeat and vector-clock reconciliation
6. mDNS/gossip peer discovery
7. RBAC, topic ACLs, and token verification
8. Partition, loss, and latency chaos tests
9. Scratch container and systemd service
10. Operator CLI for peers, queues, bandwidth, and sync

The later phases are intentionally not represented as fake production behavior yet.
