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

## v0.3 — Persistence

- [ ] Append-only file with canonical effect records
- [ ] `everysec` and `always` durability policies, with group commit
- [ ] Startup recovery, torn-tail truncation, refusal on structural corruption
- [ ] Fail-stop and fatal semantics on persistence failure

## v0.4 — Extended commands

- [ ] `MGET`, `INCR`, `DECR`
- [ ] Seeded, bounded command-sequence generator for differential testing

## v0.5 — Performance

- [ ] End-to-end benchmarks with latency distributions
- [ ] `pprof`-driven optimisation
- [ ] `docs/benchmarks.md` with reproducible results

## v1.0 — Stable

- [ ] Documentation audit
- [ ] Release binaries

## Explicit non-goals

Replication · clustering · Pub/Sub · transactions · List/Hash/Set types ·
snapshot persistence · AOF compaction · authentication · TLS · eviction
policies · `MSET` (it cannot be expressed as one canonical AOF record without
extending the record model)
