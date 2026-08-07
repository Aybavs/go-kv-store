# ADR 0001: Single RWMutex in the Engine, String Values

## Status

Accepted — v0.1.0

## Context

Multiple clients mutate and read shared state concurrently. Two decisions had
to be made together: where synchronisation lives, and how values are
represented.

## Decision

A single `sync.RWMutex`, owned by `internal/engine`. `internal/store` is a
passive data structure with no mutex, no goroutines, no I/O and no clock reads.

Keys and values are stored as Go `string`. Go strings are binary-safe, so this
does not restrict payloads to UTF-8. Data entering the store is copied at the
command layer and never aliases a parser or connection buffer.

No `Store` interface is defined: there is one implementation.

## Alternatives Considered

**Sharded locks.** Rejected: optimisation without measurement. It also
complicates the ordering guarantee the append-only file will need in v0.3 and
breaks multi-key atomicity across shard boundaries. Revisit only if profiling
shows lock contention is the bottleneck.

**Actor / single-goroutine event loop.** Rejected: a channel round-trip per
command, and its free atomicity mainly benefits transactions, which are out of
scope.

**`sync.Map`.** Rejected: it is optimised for read-mostly workloads with
disjoint key sets. Ours is mixed read/write on shared keys and will need atomic
read-modify-write for `INCR`.

**`[]byte` values with explicit cloning, and pooled byte storage.** Rejected:
mutable values reintroduce the aliasing bug class that immutability removes by
construction. Revisit only on profiling evidence of allocation or GC pressure.

## Consequences

- Writes serialise on one lock. Whether that becomes a bottleneck is a question
  for measurement, not assumption: benchmarks land in v0.5 and are recorded in
  `docs/benchmarks.md`. Until then this record claims no performance result.
- Store tests are fully deterministic — no sleeps, no clock abstraction.
- Exact compiler allocation behaviour is not part of the contract. Allocation
  counts are to be established by benchmarks rather than assumed.
- The engine boundary is also the fatal-panic boundary: a panic inside the
  commit path invalidates shared-state assumptions and is reported to the
  supervisor rather than recovered.
