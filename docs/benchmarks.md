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
BenchmarkStoreSet-10                 84212410        14.40 ns/op       0 B/op   0 allocs/op
BenchmarkStoreSetWithTTL-10          49233460        24.38 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetHit-10             100000000        11.00 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetHitWithTTL-10       64393804        18.65 ns/op       0 B/op   0 allocs/op
BenchmarkStoreGetMiss-10            231666231         5.072 ns/op      0 B/op   0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/resp
BenchmarkReadCommand-10              11219654       106.1 ns/op  348.81 MB/s    0 B/op   0 allocs/op
BenchmarkWriteBulk-10                40845501        27.80 ns/op       0 B/op   0 allocs/op
BenchmarkWriteError-10               34072194        35.09 ns/op       0 B/op   0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/engine
BenchmarkEngineSet-10                35691136        32.22 ns/op       0 B/op   0 allocs/op
BenchmarkEngineGet-10                73367198        17.09 ns/op       0 B/op   0 allocs/op
BenchmarkEngineParallelReads-10       8648599       138.9 ns/op        2 B/op   0 allocs/op
BenchmarkEngineParallelMixed-10       6431696       194.7 ns/op       10 B/op   1 allocs/op
```

## What expiration cost, and what nearly cost more

| | v0.1.0 | v0.2.0 | |
|---|---|---|---|
| `StoreGetHit` | 7.72 | 11.00 | +43% |
| `StoreGetMiss` | 1.92 | 5.07 | +164% |
| `EngineGet` | 11.55 | 17.09 | +48% |
| `EngineSet` | 29.64 | 32.22 | +9% |
| `EngineParallelReads` | 112.9 | 138.9 | +23% |
| `EngineParallelMixed` | 172.8 | 194.7 | +13% |

Reads now consult a second map to find out whether a key carries a deadline,
which is where the `store` numbers go. `GetMiss` looks alarming in percentage
terms and is not: it went from one failed map lookup to two, on an operation
that was already down at two nanoseconds.

The interesting part is the number that is **not** in the table. Before the fix
below, `EngineGet` measured **69.85 ns**, more than five times its v0.1.0 cost.
Almost none of that was the second map lookup:

```
BenchmarkClockRead-10    46121582    53.69 ns/op
```

`time.Now` costs about 54 ns on this machine, against roughly 4 ns for the map
work around it, and the engine was reading it on every single read. The engine
now skips the clock entirely when no key in the store carries a deadline, which
brought `EngineGet` back to 17.09 ns. A test pins both directions: the clock is
read when a deadline exists, and not read when none does.

Worth recording, because the design anticipated the wrong cost. Spec §4.3 has a
note reserving an escalation path — caching a `hasTTL` flag beside the value so
the common path needs one lookup — for exactly this situation. Had we taken it
without measuring, we would have optimised roughly 4 ns and left the 54 ns
alone. The recorded escalation path is still available and still unused; the
measurement says it is not where the time goes.

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
