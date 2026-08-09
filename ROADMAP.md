# Roadmap

Scope for 1.0 is a solid single-node core. Replication, clustering, Pub/Sub,
transactions and additional data types are explicit non-goals.

## v0.1 — Core server ✅

- [x] TCP server, one goroutine per connection
- [x] RESP2 subset parser and encoder, written from scratch
- [x] `PING`, `SET`, `GET`, `DEL`, `EXISTS`
- [x] Client limits, read/write deadlines, size limits
- [x] Graceful shutdown with a mutation admission gate
- [x] Differential conformance against real Redis

## v0.2 — Expiration ✅

- [x] `SET key value EX|PX` — replaces the syntax-error rejection of SET options
- [x] `EXPIRE`, `TTL`, `PERSIST`
- [x] Lazy expiration (correctness) and bounded active expiration (memory)
- [x] Conformance extended to expiration semantics

## v0.3 — Persistence ✅

- [x] Append-only file with canonical effect records
- [x] `everysec` and `always` durability policies, with group commit
- [x] Startup recovery, torn-tail truncation, refusal on structural corruption
- [x] Fail-stop and fatal semantics on persistence failure

## v0.4 — Extended commands ✅

- [x] `MGET`, `INCR`, `DECR` — `INCR`/`DECR` preserve any existing expiry
- [x] Seeded, bounded command-sequence generator for differential testing
- [x] The same generated sequences replayed through the append-only file and
      compared against live state, checked after every step

## v0.5 — Performance ✅

- [x] End-to-end harness with latency distributions, interleaved repetitions and
      a direct count of syscalls per command
- [x] `pprof`-driven optimisation — the target was **syscalls per request**, not
      lock contention. A reply is now flushed when the reader is about to block,
      so a pipelined batch costs one write instead of one per command
      (1.000 → 0.016 writes per command at pipeline 64)
- [x] `docs/benchmarks.md` with results reproducible by one documented command

Sharding is **not** on this list. ADR-0001 reserved it for the case where
measurement justified it, and the measurement says it would address something
invisible in the profile.

## v1.0 — Stable ✅

- [x] A stated compatibility contract, and tests that hold it: the flag surface,
      the append-only file format, and the documented error classes
- [x] Documentation audit — claims verified against behaviour rather than re-read
- [x] Release binaries for linux and darwin on amd64 and arm64, with checksums
      and instructions for verifying a download

From 1.0 the scope below is permanent rather than a milestone artefact: these
are decisions about what this program is, not work that is outstanding.

## Explicit non-goals

Replication · clustering · Pub/Sub · transactions · List/Hash/Set types ·
snapshot persistence · AOF compaction · authentication · TLS · eviction
policies · `MSET` (it cannot be expressed as one canonical AOF record without
extending the record model)
