# Benchmarks

Micro-benchmarks, current as of v0.4.0. End-to-end throughput and latency
distributions arrive in v0.5.

Every number here comes from an actual run on the machine named below. Nothing
is estimated, and nothing is carried over from an earlier version — carrying
numbers forward is what let a 55% regression sit unnoticed, as one of the
sections below describes.

One artefact is deliberately kept from v0.3.0 and labelled as such: the CPU
profile at the end. It is a profile rather than a number, nothing on the syscall
path changed in v0.4, and the end-to-end tables above were re-measured and agree
with what it says.

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
BenchmarkEngineSet-10                           26613415   42.10 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineGet-10                           73244793   16.18 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineMGet-10                          11357994   98.74 ns/op	      96 B/op	       1 allocs/op
BenchmarkEngineIncr-10                          12001899   101.8 ns/op	       7 B/op	       0 allocs/op
BenchmarkEngineIncrWithTTL-10                    6745112   180.2 ns/op	       7 B/op	       0 allocs/op
BenchmarkEngineParallelReads-10                 11195404   97.91 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineParallelMixed-10                 14981140   78.06 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineSetLoggedEverysec-10              1286781   928.3 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedAlways-10                1000000   1001 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedToDisk-10                    386   3683548 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedToDiskParallel-10           1911   753691 ns/op	     100 B/op	       1 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/resp
BenchmarkReadCommand-10                         11027164   105.6 ns/op	 350.38 MB/s	       0 B/op	       0 allocs/op
BenchmarkWriteBulk-10                           41480030   28.15 ns/op	       0 B/op	       0 allocs/op
BenchmarkWriteError-10                          33330092   35.45 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/store
BenchmarkStoreSet-10                            84244201   14.14 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreSetWithTTL-10                     49243227   24.45 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetHit-10                        100000000   10.96 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetHitWithTTL-10                  63959635   18.54 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetMiss-10                       227094896   5.244 ns/op	       0 B/op	       0 allocs/op
```

## What the v0.4 commands cost

| | ns/op | allocs |
|---|---|---|
| `EngineGet` | 16.2 | 0 |
| `EngineMGet` (4 keys) | 98.7 | 1 |
| `EngineSet` | 42.1 | 0 |
| `EngineIncr` | 101.8 | 0 |
| `EngineIncrWithTTL` | 180.2 | 0 |

`MGET`'s one allocation is the result slice, which is the return value itself
and not avoidable without handing the caller a buffer to fill.

**The gap between the two `Incr` figures is the clock, not the expiry lookup.**
`time.Now` costs about 54 ns on this machine — the same measurement that made
`EngineGet` five times faster in v0.2 — and `IncrBy` reads it only when some key
in the store carries a deadline. The first figure is a store with no expiries at
all, where the clock is skipped entirely; the second has one, so it is read.
Written the obvious way, with an unconditional `e.now()`, `EngineIncr` measured
**151 ns**. The map lookup that produces the record's `PXAT` is a few
nanoseconds of the difference; the clock is the rest.



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

The micro-benchmarks above measure operations in isolation. This section
measures the server, with a client, over a socket — which is what decides
whether any of them matter.

`redis-benchmark` works against this server unmodified, which is one of the
things ADR-0002 chose the RESP2 subset for. Both servers run natively on the
same machine, one at a time, with the same client and parameters. Every figure
below is the **median of three runs**, and the runs themselves are reported
where the spread matters.

    redis-benchmark -p <port> -t set,get -n 100000 -c 50 -q
    redis-benchmark -p <port> -t get      -n  20000 -c 1  -q

### A measurement error caught before it was published

The first pass at these numbers had Redis started with `--daemonize yes`. That
instance answered a single-connection `GET` benchmark at **10 035 rps with a p50
of 0.095 ms**, against our 35 236 rps at 0.023 ms — a result that would have let
this file claim we are three times faster than Redis. The same Redis build
started in the foreground answers **35 971 rps at 0.023 ms**.

So the finding was about how the oracle was launched, not about either server.
It is recorded because the wrong number was flattering, which is exactly when a
measurement gets published without a second look.

### Throughput

| 50 connections | SET rps | GET rps | p50 |
|---|---|---|---|
| Redis 8.10 | 122 549 | 119 332 | 0.215 ms |
| go-kv-store | 101 523 | 102 881 | 0.263 ms |

| 1 connection | GET rps | p50 |
|---|---|---|
| Redis 8.10 | 36 563 | 0.023 ms |
| go-kv-store | 35 399 | 0.023 ms |

At one connection the two are within 3% of each other: both are bound by the
round trip, and our per-request work is not the difference. The gap appears only
under concurrency, where we reach roughly 83–86% of Redis.

**Our run-to-run spread is much wider than Redis's**, and that is worth stating
rather than hiding behind a median. Across three runs Redis varied by 0.6% on
`SET`; we varied by 9%. A single-threaded event loop has less scheduling
variance to expose than a goroutine per connection does.

### What `everysec` costs, and what the noise will and will not support

Measured as four interleaved pairs, so drift affects both sides equally:

| plain SET rps | `everysec` SET rps | delta |
|---|---|---|
| 105 152 | 108 696 | +3% |
| 117 371 | 109 051 | −7% |
| 115 875 | 95 147 | −18% |
| 116 686 | 107 296 | −8% |

Median cost about 7%, and reads are unaffected — they never touch the log, which
is the behaviour that matters. But the pair-to-pair range runs from +3% to −18%,
so **only the direction and rough magnitude should be taken from this table**.
The isolated cost of the commit path is in the micro-benchmarks above, where it
can be measured without a network in the way. v0.3.0 reported 16% from a single
pass; that figure sits inside this spread and should not have been quoted to two
significant figures.

### Throughput does not change with core count

Median of three runs each, `GET` over 50 connections:

| GOMAXPROCS | GET rps |
|---|---|
| 1 | 124 224 |
| 2 | 122 249 |
| 4 | 118 906 |
| 8 | 116 550 |
| 10 | 116 279 |

Flat, with a slight decline. That single table settles the sharding question on
its own: the engine's read path costs 4× more per operation at ten cores than at
one, so if the lock were anywhere near the limit, end-to-end throughput at one
core would be visibly higher than at ten. It is not — and one core is in fact
the *fastest* configuration by a few percent, which points the other way
entirely.

### The profile says the same thing more bluntly

Measured at v0.3.0 and kept, per the note at the top of this file: nothing on
the syscall path changed in v0.4, and the re-measured tables above agree with
what it says. 300 000 GETs over 50 connections, CPU profile of the server:

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
| 1 | 22.4 | 44.7 | 27.6 | 36.2 |
| 2 | 68.1 | 14.7 | 52.7 | 19.0 |
| 4 | 79.8 | 12.5 | 68.5 | 14.6 |
| 8 | 129.3 | 7.7 | 81.5 | 12.3 |
| 10 | 107.6 | 9.3 | 88.8 | 11.3 |

**Total throughput falls as cores are added.** Going from one core to two costs
3.0×. This is a property of `sync.RWMutex`, not a defect here: `RLock`
increments a counter shared by every reader, so the cache line holding it
ping-pongs between cores, and that costs more than serialising would.

**This is not on its own an argument for sharding, and the end-to-end section
above is why.** The loop does nothing but lock, read and unlock. A real request
also parses a command — 106 ns before the engine is reached — and makes
syscalls, which cost microseconds. Measured end to end, throughput does not
improve with core count and the lock does not appear in the profile at all. So
this table describes a real property of `sync.RWMutex` that a real request never
gets near. ADR-0001 reserved sharding for the case where profiling justified it;
the profiling exists now and does not.

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

- The micro-benchmarks measure in-process operations only. No network.
- The Redis comparison in the end-to-end section is **not** a claim that either
  implementation is faster. The two do not offer the same feature set or the
  same durability guarantees. It is a sanity check that our per-request work is
  in the same range as a mature implementation's, and at one connection it is.
- Numbers from a laptop under an unpinned CPU governor are indicative, not
  reproducible to the last percent. Re-run before drawing a conclusion from a
  difference smaller than the run-to-run spread — measured at up to 9% for the
  end-to-end figures on this machine, and a few percent for the in-process
  ones.
