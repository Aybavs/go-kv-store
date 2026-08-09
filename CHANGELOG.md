# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-08-09

Extended commands, and the testing that makes them worth trusting.

### Added

- `MGET key [key ...]` — one element per key in request order, a null bulk
  string where the key is absent. Read-only, so it never touches the log
- `INCR key` and `DECR key`, replying with the value after the change
- A seeded, bounded command-sequence generator (`internal/cmdgen`) with two
  consumers: the conformance suite compares each sequence against real Redis
  step by step, and the engine replays the same sequence out of the append-only
  file and compares the recovered state against the live one after every step
- `store.ExpiresAt`, reporting a key's deadline as the absolute instant it is
- Benchmarks for the new commands, and re-measured end-to-end figures
- Crash-durability tests that kill a real server process at random moments and
  restart it against the file left on disk, asserting that every acknowledged
  write survives and that the recovered keys form a contiguous prefix
- ADR-0003, recording why fatal conditions are broadcast by closing a channel
  rather than delivered as a value. The decision dates from v0.1.0 and had gone
  unwritten

### Counter semantics, measured against Redis rather than remembered

**`INCR` and `DECR` preserve an existing expiry exactly**, in memory and in the
log: the record is `SET key <result> PXAT <the same absolute deadline>`. This is
the case ADR-0004 was written about — getting the in-memory TTL right while
logging a `SET` with no expiry passes every test until a crash, and then
recovery brings the key back without its expiry.

**The incrementable-value grammar is narrower than Go's `strconv.ParseInt`.**
Redis rejects `+5`, `07`, `00` and `-0`; the standard library accepts all four.
The parser is written to the measured grammar, and each of the four has a
conformance scenario.

**Overflow is its own error**, `ERR increment or decrement would overflow`,
rather than a variant of the not-an-integer error. It is a new class in the
conformance normaliser; without that, both servers collapse to `other` and every
overflow scenario passes without comparing anything.

### Fixed

- `INCR` read the clock on every call, including in a store where no key carries
  a deadline. Reading it only when it can matter — the same rule the read path
  has followed since v0.2 — took the path from 151 ns to 102 ns

### Notes

- `MSET` remains a non-goal. It cannot be expressed as one canonical append-only
  record, and one complete record is one recovery atomicity unit
- 103 handwritten conformance scenarios, up from 59


## [0.3.0] - 2026-08-09

Persistence. Data survives a restart when `--appendonly` is on; it is off by
default, and with it off nothing about the server changes.

### Added

- Append-only file recording **canonical effects**, not client commands.
  `EXPIRE` becomes a `SET` carrying the value the key holds, `PERSIST` a `SET`
  with no expiry. See ADR-0004 for the counterexample that rules command logging
  out
- `--appendonly`, `--appendfilename`, `--appendfsync everysec|always`
- Startup recovery, with three distinct outcomes: a complete log is replayed, a
  torn tail is truncated to the last complete record, and structural corruption
  refuses to start and reports the byte offset
- Persistence finalisation during shutdown, between draining and stopped: the
  writer is drained and synced before the process exits
- ADR-0004 and ADR-0005

### Durability

`always` acknowledges after `fsync`. `everysec` acknowledges after `write()`
succeeds and syncs about once a second.

**`everysec` is not Redis's `everysec`.** Redis acknowledges before the write;
we acknowledge only once `write()` has succeeded. Same name, stronger guarantee.
Stated in the flag's own help text, not only in the docs.

### Known limitation

No checksums. Structural validation catches torn tails and invalid records, but
a flipped bit that still forms a valid record is undetectable. Listed as a future
format improvement rather than shipped for completeness.

## [0.2.0] - 2026-08-08

Key expiration.

### Added

- `SET key value [EX seconds | PX milliseconds]`, `EXPIRE`, `TTL` and `PERSIST`
- Lazy expiration: a key is absent the moment its deadline passes, decided on
  the read path with no write lock and no deletion
- A bounded active expiration worker that reclaims memory on a timer. Work per
  cycle is bounded; reclamation is eventual and best-effort
- 29 further differential conformance scenarios, plus two timing tests that
  compare the expiry transition itself against Redis

### Changed

- A `SET` with no expiry option now clears any TTL the key already had, which is
  Redis's rule
- `DEL` no longer counts a key whose deadline has passed. It was already absent
  to the client, so reporting it removed would have leaked reclamation timing
- `engine.Set` takes a TTL argument. The signature changed rather than gaining a
  default-preserving variant, so no call site could keep the old behaviour by
  accident

### Performance

- Reads consult a second map to find a deadline, costing `StoreGetHit` 7.7 → 11.0 ns
- The engine skips reading the clock entirely when no key carries a deadline.
  `time.Now` measures 53.7 ns on the reference machine against roughly 4 ns for
  the map work around it, so reading it unconditionally had made `EngineGet`
  69.9 ns — over five times its v0.1.0 cost. It is now 17.1 ns

### Notes on Redis compatibility

Three behaviours were measured against Redis 7 rather than assumed, and the
assumption would have been wrong in each case:

- `TTL` rounds to nearest, `(ms + 500) / 1000`, not up
- `EXPIRE` with a non-positive value deletes the key and replies `1`
- Repeating the same `SET` option is accepted, and the last one wins

## [0.1.2] - 2026-08-08

The first release with downloadable binaries. No change to server behaviour:
every difference from 0.1.1 is in tooling, documentation or comments.

### Added

- Prebuilt binaries for linux and darwin on amd64 and arm64, with a
  `SHA256SUMS` file, built by a release workflow that a tag push triggers. It
  re-runs vet and the race suite first, because a tag can be pushed at any
  commit, including one CI never saw
- `examples/go-client`, a working client that exercises every command using an
  ordinary RESP2 library. Its own module, so the dependency stays out of the
  server
- `docker-compose.yml` and a `make conformance` target that brings the reference
  Redis up, runs the differential suite and takes it down again

### Changed

- Comment volume cut by a third. Two doc comments had accumulated on one test,
  and several others repeated rationale that `docs/` already carried, where a
  second copy only drifts. Verified to be a comment-only change: every file
  reparsed without comments is byte-identical to the previous release

## [0.1.1] - 2026-08-08

### Fixed

- **Unbounded memory growth from a single connection.** `-max-array-elements`
  and `-max-bulk-length` each bounded one dimension of a request and multiplied
  without anything bounding their product, so the defaults permitted a 64 GiB
  frame. Measured before the fix: one connection sending a 300 MiB request it
  never completed drove resident memory from 5 MB to 1.09 GB. A new
  `-max-command-bytes` limit (default 128 MiB) bounds the total, checked against
  each declared length before any payload is read
- A decode buffer grown past 1 MiB by one large request is now released when
  that request completes. It was resliced to zero length and its capacity kept
  for the life of the connection, so a client could send one large request and
  then park that memory while idle

### Added

- `-max-command-bytes` flag, and `MaxCommandBytes` in `resp.Limits`. Setting it
  to 0 disables the check
- `SECURITY.md` now states what the request limits do and do not bound, with
  measured numbers: peak memory is roughly three times the configured value, and
  the bound is per connection rather than per server

## [0.1.0] - 2026-08-08

### Added

- TCP server with one goroutine per connection
- RESP2 subset codec written from scratch, hardened against fragmentation,
  malformed frames and oversized input
- `PING [message]`, `SET`, `GET`, `DEL`, `EXISTS`
- Configurable client limit, idle read timeout, write deadline and frame size
  limits
- Graceful shutdown with a mutation admission gate held inside the commit lock
- Fatal supervisor for conditions that invalidate shared-state assumptions
- Differential conformance tests against real Redis for the documented command
  subset, including a pipelined case that checks reply framing
- Micro-benchmarks for the store, codec and engine
- `docs/protocol.md`, `docs/architecture.md`, `docs/benchmarks.md` and two
  architecture decision records

### Fixed

Both of these were found while writing the protocol documentation and the
conformance suite for this release, before any of it was published.

- Simple String and Error replies no longer split into multiple frames when
  their text contains CR or LF. Error text quotes client-supplied data, so a
  command name containing CRLF produced extra replies and permanently
  desynchronised a pipelining client
- The command name echoed in an unknown-command error is bounded at 128 bytes.
  It is client-supplied and could previously be as long as the bulk-string
  limit

### Changed

- Error strings now follow Redis's rule for command names: an unknown command
  is echoed with the casing the client sent, and a wrong-arity error reports
  the canonical name in lowercase
- `SET` accepts more than three arguments and answers `ERR syntax error` for
  any option, rather than reporting a wrong argument count that is not the
  actual problem
