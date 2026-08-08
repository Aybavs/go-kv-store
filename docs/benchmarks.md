# Benchmarks

These are micro-benchmarks establishing a v0.1.0 baseline. End-to-end
throughput and latency distributions arrive in v0.5.

## Environment

| | |
|---|---|
| CPU | Apple M4 (10 cores) |
| OS | macOS 26.4.1 |
| Go | go1.26.1 darwin/arm64 |
| Command | `make bench` (`go test -bench=. -benchmem -run='^$' ./...`) |

## Results

```
pkg: github.com/aybavs/go-kv-store/internal/store
BenchmarkStoreSet-10                 94513977        12.62 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetHit-10             153712180         7.718 ns/op      0 B/op   0 allocs/op
BenchmarkStoreGetMiss-10            624159120         1.923 ns/op      0 B/op   0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/resp
BenchmarkReadCommand-10              11008644       109.0 ns/op  339.30 MB/s    0 B/op   0 allocs/op
BenchmarkWriteBulk-10                36971992        29.08 ns/op       0 B/op   0 allocs/op
BenchmarkWriteError-10               32911388        35.59 ns/op       0 B/op   0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/engine
BenchmarkEngineSet-10                35935316        29.64 ns/op       0 B/op   0 allocs/op
BenchmarkEngineGet-10               100000000        11.55 ns/op       0 B/op   0 allocs/op
BenchmarkEngineParallelReads-10      10541816       112.9 ns/op        2 B/op   0 allocs/op
BenchmarkEngineParallelMixed-10       6977456       172.8 ns/op       10 B/op   1 allocs/op
```

## Reading these numbers

A few things are worth stating explicitly, because a table of ns/op invites
conclusions it does not support.

**The lock costs roughly what it should.** `EngineGet` at 11.55 ns against
`StoreGetHit` at 7.72 ns puts `RLock`/`RUnlock` at a few nanoseconds on an
uncontended mutex. `EngineSet` against `StoreSet` shows the same for the write
path plus the admission check.

**The parallel figures are per-operation, not throughput.** `RunParallel`
spreads the work over all 10 cores, so a higher ns/op there does not mean the
engine got slower — it means each operation now includes contention. Comparing
`EngineParallelReads` to `EngineGet` directly is the mistake to avoid; they are
measuring different situations.

**`EngineParallelMixed` is the sharding number, and it is a baseline, not a
verdict.** One writer per nine readers on a single `RWMutex` costs 172.8 ns/op
here. Nothing in this repository yet demonstrates that this is a bottleneck for
any real workload. It exists so that a future sharding decision can be made
against a measurement rather than an intuition, which is the standing rule from
ADR 0001.

**The allocation in `EngineParallelMixed` is the benchmark's, not the
engine's.** Both parallel benchmarks build their keys with `strconv.Itoa` in
the measured loop. The serial benchmarks, which use constant keys, report zero
allocations for the same code paths.

**`WriteError` costs about 6 ns more than `WriteBulk`.** That is the CR/LF scan
on the line-reply path, measured rather than assumed. Error replies are not on
the hot path, and the scan is allocation-free.

## Notes

- These measure in-process operations only. No network, no persistence.
- No comparison against Redis is made here. `redis-benchmark` will be used in
  v0.5 as an ecosystem baseline, and even then the comparison is not
  apples-to-apples: the two servers do not offer the same feature set or the
  same durability guarantees.
- Numbers from a laptop under an unpinned CPU governor are indicative, not
  reproducible to the last percent. Re-run before drawing a conclusion from a
  difference smaller than the spread between two consecutive runs, which was a
  few percent on this machine.
