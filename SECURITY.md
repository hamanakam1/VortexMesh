# Security policy

## Reporting a vulnerability

Please **do not** open a public issue for a security problem. Use GitHub's
private reporting flow:

<https://github.com/hamanakam1/VortexMesh/security/advisories/new>

Include the affected version or commit, a description of the impact, and a
minimal reproduction if you have one. Expect an initial response within a few
days. This is a personal project, not a funded product, so please be patient.

## Supported versions

Only the `main` branch is supported. There are no tagged releases yet.

## Current security posture

Be aware of what this project does and does not do before deploying it anywhere
that matters:

| Area | Status |
| --- | --- |
| Transport encryption | TLS 1.3 with mutual certificate authentication |
| Peer identity | Any certificate signed by the configured client CA |
| Authorization | **Not implemented** — a valid client can publish to any topic |
| Replay protection | Partial — the WAL deduplicates by message ID, but there is no per-connection nonce or timestamp window |
| Rate limiting | **Not implemented** — bounded queues drop rather than throttle |
| Secret storage | Out of scope; certificates are supplied by the caller |

The mesh is not hardened for hostile networks. Treat it as a reference
implementation until phase 7 (authorization) lands.

## Known limitations

- `transport.MaxFrameSize` bounds a single frame at 4 MiB, but there is no
  limit on concurrent streams per connection beyond quic-go's defaults.
- Envelope IDs are caller-supplied. A malicious peer could pre-empt an ID and
  cause a legitimate message to be treated as a duplicate. Fixing this requires
  signed envelopes, which are not implemented.
- The demo binary listens on nothing; it is in-process only.

## Dependencies

The dependency surface is intentionally small: `quic-go` for transport and
`modernc.org/sqlite` for the WAL. Dependabot is not configured; run
`go list -m -u all` periodically to check for updates.
