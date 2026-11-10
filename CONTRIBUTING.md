# Contributing

Thanks for taking a look. This is a small, deliberately readable codebase, and
the bar for a change is "can a reader follow it without extra context?"

## Before you open a pull request

```sh
make check      # tidy, gofmt check, vet, build, test -race
```

That is exactly what CI runs on Go 1.26 and the latest stable release. If
`make check` passes locally, CI should pass too.

## Ground rules

**Keep the boundaries honest.** `internal/envelope` must not import anything
else from this repository. Packages may import `envelope`; they must not import
`cmd`.

**Every new behaviour needs a test that would fail without it.** The existing
suite is organised that way — for example `TestConcurrentPublishAndUnsubscribe`
exists specifically to catch a send-on-closed-channel panic, and
`TestRouterCloseUnblocksPublish` exists to catch a shutdown deadlock.

**Do not claim a phase is done until it has a test.** The README status table is
the project contract. If you add a feature, add its row and its evidence in the
same pull request.

**Bounded by default.** Any new queue, buffer, or read path needs a limit and a
defined behaviour when the limit is hit. "It grows until memory runs out" is a
bug.

**Errors carry context.** Wrap with `fmt.Errorf("...: %w", err)`. Do not discard
errors silently; if a path genuinely ignores one, `_ =` makes that explicit.

## Style

- `gofmt` is mandatory; CI fails otherwise.
- Prefer small interfaces at the point of use.
- Exported identifiers get doc comments starting with their name.
- Keep `context.Context` as the first parameter and never store it in a struct
  (the router stores a derived root context intentionally, and it is documented
  as such).
- Tests use table-driven subtests where the cases are homogeneous.

## Commit messages

Conventional-ish prefixes keep the history scannable:

```
feat: add topic ACL enforcement
fix: prevent send on closed channel in broker
docs: document the framing format
test: cover oversized frame rejection
```

## Reporting bugs

Open an issue with:

1. Go version (`go version`) and OS.
2. The command that failed.
3. What you expected and what happened.
4. If it is a race or panic, the output of `go test -race ./...`.

Security issues go to [SECURITY.md](SECURITY.md) instead, not the public issue
tracker.
