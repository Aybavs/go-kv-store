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
pkg: github.com/aybavs/go-kv-store/internal/engine
BenchmarkEngineSet-10                           27088570   41.64 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineGet-10                           75994660   15.99 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineParallelReads-10                 11567928   101.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineParallelMixed-10                 14622577   83.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineSetLoggedEverysec-10              1241560   947.0 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedAlways-10                1000000   1007 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedToDisk-10                    388   3713027 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedToDiskParallel-10           1879   755708 ns/op	      99 B/op	       1 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/resp
BenchmarkReadCommand-10                         10887110   108.8 ns/op	 340.15 MB/s	       0 B/op	       0 allocs/op
BenchmarkWriteBulk-10                           41956390   28.00 ns/op	       0 B/op	       0 allocs/op
BenchmarkWriteError-10                          34273881   35.30 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/store
BenchmarkStoreSet-10                            84531092   14.13 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreSetWithTTL-10                     49405674   24.34 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetHit-10                        100000000   10.77 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetHitWithTTL-10                  67418256   18.34 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetMiss-10                       240436652   5.247 ns/op	       0 B/op	       0 allocs/op
```

## What persistence costs

The first three logged figures use a stub file that accepts everything and syncs
instantly, so they isolate what *this code* costs from what a device costs. The
last two use a real file with a real `fsync`.

| | ns/op | |
|---|---|---|
| `EngineSet` | 41.6 | no log attached |
| `EngineSetLoggedEverysec` | 947 | + hand off to the writer, wait for `write()` |
| `EngineSetLoggedAlways` | 1007 | + wait for `Sync()` |
| `EngineSetLoggedToDisk` | 3 713 027 | a real `fsync` per write |
| `EngineSetLoggedToDiskParallel` | 755 708 | the same, with writers in flight |

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
no log to put it in. Deriving only when there is a log brought it to 41.6 ns.

The remaining ~10 ns over the v0.2 baseline is the commit path's shape and is
being kept deliberately. The mutation methods wrap their locked section in a
closure so that the durability wait is structurally outside the lock; that
closure carries two `defer`s and therefore is not inlined. Removing it would
recover the time and give up the ordering guarantee that `guard` runs after the
unlock — without which a panic could leave `onFatal` deadlocked against a held
lock. Ten nanoseconds on a path that already costs 108 ns to decode its own
command is not worth that trade, and if end-to-end work in v0.5 ever says
otherwise, it will say so with a number.

## End to end, and the answer to the sharding question

The micro-benchmarks below measure operations in isolation. This section
measures the server, with a client, over a socket — which is what decides
whether any of them matter.

`redis-benchmark` works against this server unmodified, which is one of the
things ADR-0002 chose the RESP2 subset for. Both servers run natively on the
same machine, one at a time, with the same client and parameters.

| 50 connections | SET rps | GET rps | p50 |
|---|---|---|---|
| Redis 8.10 | 117 716 | 115 407 | 0.215 ms |
| go-kv-store | 99 010 | 91 785 | 0.263 / 0.295 ms |
| go-kv-store, `--appendonly --appendfsync everysec` | 83 577 | 92 807 | 0.335 / 0.279 ms |

| 1 connection | GET rps | p50 |
|---|---|---|
| Redis 8.10 | 34 722 | 0.023 ms |
| go-kv-store | 33 613 | 0.023 ms |

At one connection the two are within 3% of each other: both are bound by the
round trip, and our per-request work is not the difference. The gap appears only
under concurrency, where we reach roughly 80–84% of Redis.

Persistence costs about 16% of write throughput under `everysec` and nothing at
all on reads, which is what it should do — reads never touch the log.

### Throughput does not change with core count

| GOMAXPROCS | GET rps |
|---|---|
| 1 | 121 704 |
| 2 | 124 095 |
| 4 | 115 942 |
| 8 | 100 587 |
| 10 | 114 833 |

Flat. That single table settles the sharding question on its own: the engine's
read path costs 4× more per operation at ten cores than at one, so if the lock
were anywhere near the limit, end-to-end throughput at one core would be
visibly higher than at ten. It is not.

### The profile says the same thing more bluntly

300 000 GETs over 50 connections, CPU profile of the server:

```
      flat  flat%   cum%
     3.52s 53.50% 53.50%  syscall.rawsyscalln
     1.24s 18.84% 72.34%  runtime.pthread_cond_wait
     1.09s 16.57% 88.91%  runtime.kevent
     0.39s  5.93% 94.83%  runtime.pthread_cond_signal
     0.30s  4.56% 99.39%  runtime.usleep

                  28.57%  resp.(*Reader).ReadCommand   → bufio fill → read(2)
                  24.77%  resp.(*Writer).Flush         → write(2)
```

**`engine`, `store` and the mutex do not appear at all** — not in the top
fifteen, and below the 0.03s threshold at which nodes were dropped. Around 99%
of CPU is syscalls and scheduler, split roughly evenly between the read and the
write that each command costs.

### What this means for v0.5

Sharding is **not** the optimisation to make. ADR-0001 reserved it for the case
where measurement justified it; measurement says it would address something
invisible in the profile. That question is now closed with data rather than left
open with a worry.

The bottleneck is one `read` and one `write` syscall per command. That is also
why Redis is ahead under concurrency and level with us at one connection: its
single-threaded event loop does work for many connections per wakeup, while a
goroutine per connection pays its own syscalls. Reducing syscalls per request is
the direction — and `docs/architecture.md` already records why the obvious form
of it is wrong, since `bufio.Reader.Buffered()` cannot tell you whether a
complete next command is pending and deferring a flush on that signal
deadlocks. A correct mechanism would need to be something else.

## Reads do not scale, and that is the sharding question

`BenchmarkEngineParallelReads` is the number ADR-0001 says a sharding decision
would be made against. A single figure hides what it is actually saying, so here
it is across core counts:

| cores | reads ns/op | reads M ops/s | mixed ns/op | mixed M ops/s |
|---|---|---|---|---|
| 1 | 23.1 | 43.3 | 27.2 | 36.8 |
| 2 | 76.7 | 13.0 | 57.5 | 17.4 |
| 4 | 81.7 | 12.2 | 66.2 | 15.1 |
| 8 | 111.2 | 9.0 | 75.2 | 13.3 |
| 10 | 101.3 | 9.9 | 83.0 | 12.1 |

**Total throughput falls as cores are added.** Going from one core to two costs
3.3×. This is a property of `sync.RWMutex`, not a defect here: `RLock`
increments a counter shared by every reader, so the cache line holding it
ping-pongs between cores, and that costs more than serialising would.

**This is not on its own an argument for sharding.** The loop does nothing but
lock, read and unlock. A real request also parses a command — 108 ns before the
engine is reached — and makes syscalls, which cost microseconds. What fraction
of a real request the lock actually is remains unmeasured, and that is what the
end-to-end work in v0.5 is for. ADR-0001's rule stands: sharding is a decision
to be made against profiling data, and this is not yet that data.

One observation worth carrying into v0.5 rather than explaining away here: the
mixed workload is *faster* than the pure-read one above two cores, which is
counter-intuitive when it holds an exclusive lock a tenth of the time. A
plausible reason is that a writer parks readers instead of leaving them
contending on the shared counter, but that is a hypothesis and has not been
measured.

## A measurement error in the benchmarks themselves

Both parallel benchmarks used to build their keys inside the measured loop, with
`strconv.Itoa` and a string concatenation. That put allocator behaviour into the
number the sharding decision was supposed to turn on.

| | before | after |
|---|---|---|
| `EngineParallelReads` | 130.9 ns, 2 B/op | 101.5 ns, 0 B/op |
| `EngineParallelMixed` | 184.3 ns, 10 B/op, 1 alloc | 83.7 ns, 0 B/op |

The mixed figure was more than half benchmark overhead. Keys are now built once,
before the timer starts.

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
