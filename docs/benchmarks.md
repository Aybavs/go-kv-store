# Benchmarks

Micro-benchmarks, current as of v0.3.0. End-to-end throughput and latency
distributions arrive in v0.5.

Every number here comes from an actual run on the machine named below. Nothing
is estimated, and nothing is carried over from an earlier version — carrying
numbers forward is what let a 55% regression sit unnoticed, as the last section
describes.

## Environment

| | |
|---|---|
| CPU | Apple M4 (10 cores) |
| OS | macOS 26.4.1 (APFS, internal SSD) |
| Go | go1.26.1 darwin/arm64 |
| Command | `make bench` (`go test -bench=. -benchmem -run='^$' ./...`) |

## Results

```
pkg: github.com/aybavs/go-kv-store/internal/store
BenchmarkStoreSet-10                          82705848        14.72 ns/op       0 B/op   0 allocs/op
BenchmarkStoreSetWithTTL-10                   49347938        24.39 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetHit-10                      100000000        11.26 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetHitWithTTL-10                60900436        19.01 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetMiss-10                     231575439         5.113 ns/op      0 B/op   0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/resp
BenchmarkReadCommand-10                       10687462       108.6 ns/op  340.62 MB/s   0 B/op   0 allocs/op
BenchmarkWriteBulk-10                         42120656        27.66 ns/op       0 B/op   0 allocs/op
BenchmarkWriteError-10                        33781048        35.26 ns/op       0 B/op   0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/engine
BenchmarkEngineSet-10                         26299279        41.89 ns/op       0 B/op   0 allocs/op
BenchmarkEngineGet-10                         71359722        16.29 ns/op       0 B/op   0 allocs/op
BenchmarkEngineParallelReads-10                9131733       130.9 ns/op        2 B/op   0 allocs/op
BenchmarkEngineParallelMixed-10                6504892       184.3 ns/op       10 B/op   1 allocs/op
BenchmarkEngineSetLoggedEverysec-10            1283890       946.0 ns/op       64 B/op   2 allocs/op
BenchmarkEngineSetLoggedAlways-10              1000000      1001 ns/op         64 B/op   2 allocs/op
BenchmarkEngineSetLoggedToDisk-10                  379   3678964 ns/op         64 B/op   2 allocs/op
BenchmarkEngineSetLoggedToDiskParallel-10         1874    756653 ns/op         99 B/op   1 allocs/op
```

## What persistence costs

The first three logged figures use a stub file that accepts everything and syncs
instantly, so they isolate what *this code* costs from what a device costs. The
last two use a real file with a real `fsync`.

| | ns/op | |
|---|---|---|
| `EngineSet` | 41.9 | no log attached |
| `EngineSetLoggedEverysec` | 946 | + hand off to the writer, wait for `write()` |
| `EngineSetLoggedAlways` | 1001 | + wait for `Sync()` |
| `EngineSetLoggedToDisk` | 3 678 964 | a real `fsync` per write |
| `EngineSetLoggedToDiskParallel` | 756 653 | the same, with writers in flight |

Three things are worth reading off this table.

**Most of the in-process cost is a goroutine handoff, not encoding.** Going from
42 ns to 946 ns is the round trip: append to the buffer, wake the writer, wait
on a condition variable for the marker to advance. Encoding a record is a small
part of that.

**`fsync` dominates everything else by four orders of magnitude.** One durable
write to a real file costs about 3.7 ms here. Any reasoning about durability
that starts from the nanosecond figures is reasoning about the wrong number.

**Group commit is real, and this is the measurement that says so.** The same
workload with concurrent writers costs 757 µs per operation instead of 3.7 ms —
about 4.8× — because writers waiting on the same `Sync` share one syscall. Spec
§6.6 claims this follows from the construction rather than from a separate
mechanism; until now nothing checked it.

## A regression this file caught

`docs/benchmarks.md` carried v0.2 numbers while v0.3 put the AOF into the commit
path. The assumption was that with persistence off nothing would change.
Measurement disagreed: `EngineSet` had gone from **32.22 ns to 49.81 ns, 55%
slower, with no log attached at all**.

The cause was an argument evaluated whether or not it was used —
`aof.DeriveSet(...)` built a record at every call site, including when there was
no log to put it in. Deriving only when there is a log brought it to 41.9 ns.

The remaining ~10 ns over the v0.2 baseline is the commit path's shape and is
being kept deliberately. The mutation methods wrap their locked section in a
closure so that the durability wait is structurally outside the lock; that
closure carries two `defer`s and therefore is not inlined. Removing it would
recover the time and give up the ordering guarantee that `guard` runs after the
unlock — without which a panic could leave `onFatal` deadlocked against a held
lock. Ten nanoseconds on a path that already costs 108 ns to decode its own
command is not worth that trade, and if end-to-end work in v0.5 ever says
otherwise, it will say so with a number.

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
