# Architecture

VortexMesh is organised as a stack of independent boundaries. Each one can be
tested without a network, and each one refuses invalid input rather than
trusting its caller.

## Layering rules

```
cmd/vortexmesh
      │
      ├── internal/broker ──────┐
      ├── internal/wal ─────────┤
      ├── internal/transport ───┤
      ├── internal/reconcile    │
      └── internal/discovery    │
                                ▼
                        internal/envelope
```

`internal/envelope` imports nothing from the project. Every other package may
import it. There are no cycles, and no package reaches "up" into `cmd`.

## Envelope

`Envelope` is the only type that crosses a boundary. It carries a schema
version, a kind, a topic, a source node, a UTC timestamp, and an opaque JSON
payload.

Validation is enforced in three places that cannot be bypassed:

1. `New` validates before returning, so a constructor cannot produce garbage.
2. `Decode` validates anything arriving from the network or from SQLite.
3. `MarshalJSON` validates before serialising, so a bug elsewhere cannot leak
   a malformed message onto the wire or into the log.

## Broker

The broker is a topic-keyed fan-out over per-subscriber buffered channels.

The interesting part is shutdown, not delivery. Two races are easy to get wrong:

- **Send on a closed channel.** `Publish` snapshots subscribers under a read
  lock, then sends *without* holding the broker lock. If `unsubscribe` closed
  the channel in that window, the process panics. Each subscription therefore
  owns an `RWMutex`: `send` holds the read lock across the send, `close` takes
  the write lock. The channel can only close once no send is in flight.
- **Deadlock on unsubscribe.** Because `close` waits for an in-flight send, a
  publisher blocked on a full subscriber buffer would prevent `close` from ever
  acquiring the write lock. Each subscription therefore carries a `done`
  channel that `close` closes *before* taking the write lock, waking any
  blocked `send` immediately.

`TestConcurrentPublishAndUnsubscribe` reproduces the first race under `-race`.

## Router

The router puts a bounded queue in front of the broker and drains it with a
fixed worker pool. Producers never block on a slow subscriber for longer than
their own context allows.

Counters are exact and additive:

| Counter | Incremented when |
| --- | --- |
| `Accepted` | the envelope entered the queue |
| `Delivered` | the broker fanned it out successfully |
| `Dropped` | validation failed, the caller's context expired, or the broker rejected it |

Shutdown ordering matters. `Close` cancels the root context **first**, then
takes the write lock. If it took the lock first, a `Publish` blocked on a full
queue would hold the read lock and deadlock the shutdown. Envelopes still in
the queue at shutdown are abandoned and are not counted as dropped, because
they were never delivered and never rejected.

## Write-ahead log

`wal.Store` is a single-connection SQLite database. One connection is
deliberate: SQLite serialises writers anyway, and a single connection keeps
sequence numbers and replay order unambiguous.

`Append` is idempotent on the message ID. The insert uses
`ON CONFLICT (id) DO NOTHING`; when nothing was written, the existing sequence
is looked up and returned. A sender that never saw its acknowledgment can
therefore retry safely.

`Pending` returns undelivered entries ordered by the autoincrement sequence,
which is the replay order after a reconnect. `MarkDelivered` is the
acknowledgment step.

## Reconciliation

A vector clock maps node ID to the highest version that node has observed.
`Compare` returns one of four relations, and `Plan` maps them to the cheapest
converging action:

| Relation | Action |
| --- | --- |
| `LocalAhead` | `send_local` |
| `RemoteAhead` | `request_remote` |
| `Concurrent` | `merge_state` |
| `Equal` | `noop` |

`Merge` is a component-wise maximum and never mutates its inputs, so the
originals remain available for auditing.

`Machine` tracks link health from heartbeats (`disconnected → connecting →
healthy → degraded`). It is mutex-guarded because a heartbeat reader and a
supervisor typically share one instance.

## Transport

QUIC with TLS 1.3 and `RequireAndVerifyClientCert`. One bidirectional stream
carries exactly one envelope as a 4-byte big-endian length prefix followed by
JSON.

The reader checks the length **before** allocating, rejecting zero-length and
oversized frames. Without that check, a hostile peer could request a
multi-gigabyte allocation with four bytes.

`TestEndToEndMutualTLS` generates a throwaway CA, a server certificate for
`127.0.0.1`, and a client certificate, then performs a real handshake over
loopback UDP. `TestServerRejectsUntrustedClientCertificate` proves the negative
case with a certificate from an unrelated CA.

## Discovery

`discovery.Service` is the contract a real mDNS or gossip adapter would
implement. `Registry` is the in-memory implementation for tests and single-host
development.

It returns peers sorted by ID. That sounds cosmetic but is not: Go randomises
map iteration, and an unsorted `Peers` would make reconciliation order and
tests non-deterministic.

## What is not wired together

There is no supervisor process that ties these boundaries into a running
daemon. A production loop would look like:

```
transport.Handler ──▶ wal.Append ──▶ router.Publish ──▶ broker ──▶ subscribers
        ▲                                        │
        └────────── wal.Pending / MarkDelivered ◀─┘   (after reconnect)
```

That loop, plus authorization, is the work remaining in phases 7–10. The
building blocks are implemented and individually tested; the orchestration is
not.
