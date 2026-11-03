# Wire protocol

Version 1 of the VortexMesh protocol. Everything here is exercised by
`internal/transport` tests.

## Transport

- **QUIC** (RFC 9000), one bidirectional stream per envelope.
- **TLS 1.3** with mutual authentication. The server requires and verifies a
  client certificate (`tls.RequireAndVerifyClientCert`).
- **ALPN** identifier: `vortexmesh/1`.
- **Idle timeout:** 30 seconds, with a keep-alive every 10 seconds. A silent
  peer is assumed to be on a broken link rather than idle.

## Framing

Every stream carries exactly one message:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        payload length (u32)                   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        payload (JSON, UTF-8)                  |
|                              ...                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- Length is unsigned 32-bit **big-endian**, and counts payload bytes only.
- `0 < length <= 4 MiB` (4194304). Anything else is rejected before a buffer is
  allocated.
- The payload is a JSON object matching the envelope schema below.

A stream is closed by the sender after one message. There is no request/response
correlation in v1; the receiver acknowledges at the WAL layer, not on the wire.

## Envelope schema

```json
{
  "id": "m-1",
  "version": 1,
  "kind": "task.request",
  "topic": "agents/a1",
  "source": "node-a",
  "created_at": "2026-10-30T04:06:23.089231Z",
  "payload": { "action": "sync" }
}
```

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `id` | string | yes | Unique per message; the WAL's idempotency key |
| `version` | integer | yes | Must be `1` |
| `kind` | string | yes | `task.request`, `telemetry.heartbeat`, or `state.sync` |
| `topic` | string | yes | Routing key; subscribers match exactly |
| `source` | string | yes | Originating node ID |
| `created_at` | RFC 3339 timestamp | yes | Set in UTC by `envelope.New` |
| `payload` | JSON value | yes | Opaque to the mesh; must be valid JSON |

Unknown fields are tolerated on decode so a future minor version can add
metadata without breaking older readers. Unknown `kind` values and unknown
`version` values are rejected — those are semantic changes, not metadata.

Trailing bytes after the JSON object are rejected, so two concatenated messages
cannot be silently interpreted as one.

## Error handling

The protocol has no error frames in v1. A receiver that rejects a frame simply
closes the stream, and the sender's acknowledgment never arrives. That is
deliberate: on an intermittent link, a timeout is a more reliable signal than an
error message that may itself be lost. The WAL's idempotent `Append` makes the
retry safe.

`transport.Server.OnError` surfaces protocol errors to the caller for logging.
Handler errors are reported there rather than being discarded.

## Compatibility

`vortexmesh/1` will not change incompatibly. A breaking change to the envelope
or framing means `vortexmesh/2`, advertised as an additional ALPN identifier so
old and new peers can negotiate.
