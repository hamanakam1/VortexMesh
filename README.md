# VortexMesh

```text
 __      __           __                  __  ___                    __
/  \    /  \___  ____/  |_  ____  ______/  |/  /___  ____  ____  ____/  |_
\   \/\/   /  _ \/ __ \   __\/ __ \/  ___/   |  / __ \/ __ \/ __ \/ __ \   __\
 \        (  <_> ) ___/|  | \  ___/ /__/ /    |/ /_/ / / / / /_/ / /_/ /|  |
  \__/\__/ \____/\___  >__|  \___  >___  >____  \____/_/ /_/\____/\____/ |__|
                     \/         \/         \/
```

> A small, inspectable event mesh for agents and edge nodes that must keep
> working across slow, intermittent, or disconnected links.

VortexMesh is a learning-oriented distributed edge-to-cloud event mesh in Go. It is designed for local agent swarms, central services, and intermittent links such as satellite backhaul.

## Goals and delivery status

The repository is intentionally built as a learning path: every phase should
add one production concern while keeping the previous boundary testable. The
status below is the contract for the project; an unchecked item is a goal, not
an implied feature.

| Phase | Goal | Status |
| --- | --- | --- |
| 1 | Versioned envelopes and strict validation | **Implemented** |
| 2 | QUIC transport with TLS 1.3 mutual authentication | **Implemented boundary; live certificate exercise pending** |
| 3 | Concurrent pub/sub with bounded routing and metrics | **Implemented** |
| 4 | SQLite-backed offline write-ahead log | Planned |
| 5 | Heartbeats and vector-clock reconciliation | Planned |
| 6 | mDNS/gossip peer discovery | Planned |
| 7 | RBAC, topic ACLs, and token verification | Planned |
| 8 | Partition, loss, and latency chaos tests | Planned |
| 9 | Scratch container and systemd service | Planned |
| 10 | Operator CLI for peers, queues, bandwidth, and sync | Planned |

The project does not claim the planned phases as production behavior. Each
future phase should ship with focused tests, an explicit failure mode, and a
small runnable example before its status changes.

## Architecture

```text
             validated envelope
 agent ──────────────────────────────────────┐
                                             v
                                        +----------+
 edge peer ── QUIC + mTLS ───────────> |  Router  | ──> subscribers
                                        +----------+
                                             |
                         future: WAL / sync / discovery / ACL
```

The current vertical slice is deliberately local: the broker and router are
usable without a network, while the QUIC server provides the authenticated
transport boundary for the next phases.

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
- SQLite WAL persistence with ordered pending reads and idempotent message IDs;
- explicit delivery acknowledgment for replay after network recovery.
- heartbeat-driven connection states and deterministic vector-clock reconciliation plans.

Run it with:

```sh
go test ./...
go run ./cmd/vortexmesh -topic telemetry
```

The demo prints the VortexMesh banner and one validated telemetry envelope.

## Learning path

Each phase will add one production concern while keeping the previous interfaces testable:

1. Envelope schema and validation (implemented)
2. QUIC transport with mTLS (transport boundary implemented; dependency download and live certificate acceptance remain to be exercised)
3. Concurrent pub/sub routing (implemented with bounded worker pool and metrics)
4. SQLite-backed offline WAL (implemented in `internal/wal`)
5. Heartbeat and vector-clock reconciliation (implemented in `internal/reconcile`)
6. mDNS/gossip peer discovery
7. RBAC, topic ACLs, and token verification
8. Partition, loss, and latency chaos tests
9. Scratch container and systemd service
10. Operator CLI for peers, queues, bandwidth, and sync

The later phases are intentionally not represented as fake production behavior yet.
