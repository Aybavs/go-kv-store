# Benchmarks

Current as of v0.5.0. Micro-benchmarks, end-to-end throughput, latency
distributions, and syscalls per command.

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
BenchmarkEngineSet-10                           26698110   42.03 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineGet-10                           75720728   16.46 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineMGet-10                          11651818   102.9 ns/op	      96 B/op	       1 allocs/op
BenchmarkEngineIncr-10                          12296089   100.8 ns/op	       7 B/op	       0 allocs/op
BenchmarkEngineIncrWithTTL-10                    6782724   180.3 ns/op	       7 B/op	       0 allocs/op
BenchmarkEngineParallelReads-10                 12447416   95.83 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineParallelMixed-10                 15013527   79.24 ns/op	       0 B/op	       0 allocs/op
BenchmarkEngineSetLoggedEverysec-10              1294947   918.3 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedAlways-10                1206110   992.8 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedToDisk-10                    390   3694721 ns/op	      64 B/op	       2 allocs/op
BenchmarkEngineSetLoggedToDiskParallel-10           1939   752174 ns/op	      99 B/op	       1 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/resp
BenchmarkReadCommand-10                         10991577   105.7 ns/op	 350.06 MB/s	       0 B/op	       0 allocs/op
BenchmarkWriteBulk-10                           41061730   28.45 ns/op	       0 B/op	       0 allocs/op
BenchmarkWriteError-10                          33230218   34.91 ns/op	       0 B/op	       0 allocs/op

pkg: github.com/aybavs/go-kv-store/internal/store
BenchmarkStoreSet-10                            84306554   14.36 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreSetWithTTL-10                     49391607   24.33 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetHit-10                        100000000   10.72 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetHitWithTTL-10                  62551718   17.92 ns/op	       0 B/op	       0 allocs/op
BenchmarkStoreGetMiss-10                       237145050   5.032 ns/op	       0 B/op	       0 allocs/op
```


## What the v0.4 commands cost

| | ns/op | allocs |
|---|---|---|
| `EngineGet` | 16.5 | 0 |
| `EngineMGet` (4 keys) | 102.9 | 1 |
| `EngineSet` | 42.0 | 0 |
| `EngineIncr` | 100.8 | 0 |
| `EngineIncrWithTTL` | 180.3 | 0 |

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
| `EngineSet` | 42.0 | no log attached |
| `EngineSetLoggedEverysec` | 918 | + hand off to the writer, wait for `write()` |
| `EngineSetLoggedAlways` | 993 | + wait for `Sync()` |
| `EngineSetLoggedToDisk` | 3 694 721 | a real `fsync` per write |
| `EngineSetLoggedToDiskParallel` | 752 174 | the same, with writers in flight |

Three things are worth reading off this table.

**Most of the in-process cost is a goroutine handoff, not encoding.** Going from
42 ns to 918 ns is the round trip: append to the buffer, wake the writer, wait
on a condition variable for the marker to advance. Encoding a record is a small
part of that.

**`fsync` dominates everything else by four orders of magnitude.** One durable
write to a real file costs about 3.7 ms here. Any reasoning about durability
that starts from the nanosecond figures is reasoning about the wrong number.

**Group commit is real, and this is the measurement that says so.** The same
workload with concurrent writers costs 752 µs per operation instead of 3.7 ms —
about 4.9× — because writers waiting on the same `Sync` share one syscall. Spec
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

## Syscalls per command, and what v0.5 did about them

ROADMAP named *syscalls per request* as the v0.5 target, so `internal/server`
grew a harness that counts them directly instead of inferring them from
throughput. `net.Conn` is an interface, so this needs no production code: a test
assigns a wrapped listener to the server. Reproduce with:

    make bench-e2e

Configurations run interleaved (A B A B), never in blocks, and the report prints
the spread beside the median. Latency is per *batch*: at pipeline 1 a batch is
one command, above 1 the two are different quantities.

### The asymmetry that started it

Measured before anything changed:

| workload | reads/cmd | writes/cmd |
|---|---|---|
| request/response | 1.000 | 1.000 |
| pipeline 8 | 0.125 | **1.000** |
| pipeline 64 | 0.016 | **1.000** |

Sixty-four commands arrived in one segment and were parsed out of one buffer;
sixty-four replies left in sixty-four separate writes. The two sides of the same
connection were an order of magnitude apart.

### After: flush when the reader is about to block

Eleven interleaved repetitions, both behaviours alternating **inside one
process**. That matters: this machine's end-to-end spread is up to 9%, so two
runs minutes apart could not separate anything smaller.

| workload | before cmd/s | after cmd/s | writes/cmd |
|---|---|---|---|
| GET, 10 conns, pipeline 8 | 322 241 | 732 262 | 1.000 → 0.125 |
| GET, 10 conns, pipeline 64 | 409 084 | 4 342 347 | 1.000 → 0.016 |
| GET, 50 conns, pipeline 64 | 397 316 | 5 642 551 | 1.000 → 0.016 |
| SET, 10 conns, pipeline 64 | 403 802 | 1 937 437 | 1.000 → 0.016 |
| SET, pipeline 64, `everysec` | 242 294 | 468 605 | 1.000 → 0.016 |

Writes per command now follow reads exactly.
[ADR 0006](design-decisions/0006-flush-when-the-reader-blocks.md) has the
mechanism and the alternatives it rules out.

### Request/response, where the claim was that nothing changes

| workload | before | after |
|---|---|---|
| GET, 1 conn | 45 424 ±3.5% | 44 919 ±3.9% |
| GET, 10 conns | 94 030 ±2.3% | 94 373 ±1.2% |
| GET, 50 conns | 124 898 ±11.2% | 119 943 ±5.7% |

One and ten connections are flat and stable enough to say so.

**Fifty connections was reported as unsettled at v0.5.0 and has since been
settled by measuring it properly.** Three interleaved runs had given −6.5%,
−4.0% and +0.2%, with the before arm's spread at 9–13% each time. Re-run with
that workload alone and fifteen repetitions:

| | cmd/s | spread |
|---|---|---|
| before | 107 084 | **63.0%** |
| after | 108 336 | 16.4% |

The medians are within 1.2% of each other, so there is no difference to explain.
The earlier deltas were the noisy arm's median moving, not the change.

The spread is the actual result here, and it points the other way from the
worry: deferring the flush makes throughput at this concurrency **four times
more predictable**. Flushing inside the read path removes a wakeup per command
from the scheduler's work, and that shows up in the variance long before it
shows up in the median.

### Latency, per batch

| workload | p50 before | p50 after |
|---|---|---|
| GET, 10 conns, pipeline 8 | 245 µs | 102 µs |
| GET, 10 conns, pipeline 64 | 1.571 ms | 133 µs |
| GET, 50 conns, pipeline 64 | 7.936 ms | 428 µs |

---

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

| 50 connections, no pipelining | SET rps | GET rps | p50 |
|---|---|---|---|
| Redis 8.10 | 126 263 | 126 263 | 0.207 ms |
| go-kv-store | 111 982 | 107 875 | 0.239 / 0.255 ms |

| 50 connections, `-P 16` | GET rps | p50 |
|---|---|---|
| Redis 8.10 | 1 379 310 | 0.471 ms |
| go-kv-store | 1 550 388 | 0.287 ms |

Without pipelining we reach roughly 85–89% of Redis: both sides pay a syscall
per command, and Redis's event loop amortises its wakeups better than a
goroutine per connection does.

**With pipelining the ordering reverses**, consistently across three interleaved
pairs, at higher throughput and lower p50. That is the v0.5 change showing up
against an independent client rather than against our own harness.

It is **not** a claim that this implementation is faster than Redis. The two do
not offer the same feature set or the same durability guarantees, this is one
workload shape on one machine, and Redis is doing more per command than we are.
What the row supports is narrower and is the only thing claimed: on a pipelined
workload, per-request syscalls are no longer the thing holding this server
back.

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
| 1 | 124 069 |
| 2 | 123 916 |
| 4 | 117 925 |
| 8 | 116 279 |
| 10 | 114 548 |

Flat, with a slight decline. That single table settles the sharding question on
its own: the engine's read path costs 4× more per operation at ten cores than at
one, so if the lock were anywhere near the limit, end-to-end throughput at one
core would be visibly higher than at ten. It is not — and one core is in fact
the *fastest* configuration by a few percent, which points the other way
entirely.

### The profile says the same thing more bluntly

Reproducible now rather than collected by hand:

    make bench-profile
    go tool pprof -top -nodecount=10 cpu.prof

300 000 GETs over 50 connections without pipelining, before and after v0.5:

```
before                                   after
 12.25s 77.88%  syscall.rawsyscalln       12.42s 78.21%  syscall.rawsyscalln
  1.11s  7.06%  runtime.pthread_cond_wait  1.20s  7.56%  runtime.pthread_cond_wait
  0.92s  5.85%  runtime.usleep             0.88s  5.54%  runtime.usleep
  0.79s  5.02%  runtime.kevent             0.78s  4.91%  runtime.kevent

  6.18s 39.29%  bufio.(*Reader).fill  cum  9.34s 58.82%  bufio.(*Reader).fill  cum
  6.08s 38.65%  bufio.(*Writer).Flush cum
```

**`engine`, `store` and the mutex do not appear at all**, three milestones after
that was first measured, and around 99% of CPU is syscalls and scheduler.

**The attribution moved and the total did not, which is worth understanding
before reading the second column as a regression.** `Flush` no longer appears as
its own subtree because the flush now happens *inside* the read path, so its
cost is counted under `fill`. Total syscall time is unchanged (77.9% against
78.2%) and so is throughput on this workload (118 090 against 116 256 cmd/s,
inside the spread) — which is the expected result, since a request/response
client has nothing to batch.

### What this meant for v0.5, and what is left

Sharding is **not** the optimisation to make. ADR-0001 reserved it for the case
where measurement justified it; measurement says it would address something
invisible in the profile. That question is closed with data rather than left
open with a worry.

The bottleneck was one `read` and one `write` syscall per command. **Half of it
is gone**: a reply is now flushed when the reader is about to block, so a
pipelined batch costs one write rather than one per command. See the syscalls
section above and ADR 0006.

The read half remains. A request/response client still costs one read and one
write per command, and nothing here can batch what a client sends one command at
a time. Doing better needs something that serves many connections per wakeup —
an event loop — which would replace the concurrency model this project set out
to build. It is not a 0.x change and is not on the roadmap.

## Reads do not scale, and that is the sharding question

`BenchmarkEngineParallelReads` is the number ADR-0001 says a sharding decision
would be made against. A single figure hides what it is actually saying, so here
it is across core counts:

| cores | reads ns/op | reads M ops/s | mixed ns/op | mixed M ops/s |
|---|---|---|---|---|
| 1 | 23.0 | 43.5 | 26.8 | 37.3 |
| 2 | 68.5 | 14.6 | 54.6 | 18.3 |
| 4 | 75.7 | 13.2 | 62.0 | 16.1 |
| 8 | 130.1 | 7.7 | 80.9 | 12.4 |
| 10 | 97.3 | 10.3 | 76.3 | 13.1 |

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

### The mixed workload really is faster, and it is no longer a hypothesis

Since v0.2 this file has carried an observation it could not explain: the mixed
workload is *faster* than the pure-read one above two cores, which is
counter-intuitive when it takes an exclusive lock a tenth of the time. The
suggested reason — a writer parks readers instead of leaving them contending on
the shared reader counter — was labelled a hypothesis because nothing had varied
the one quantity that would test it.

Sweeping the write fraction at ten cores, five repetitions each, medians:

| writes | ns/op |
|---|---|
| none | 105.9 |
| 1 in 1000 | 124.2 |
| 1 in 100 | 109.6 |
| 1 in 10 | **77.6** |
| 1 in 4 | 79.1 |
| 1 in 2 | 136.6 |

**Throughput is not monotonic in the write fraction.** It improves as writers are
added, is fastest at roughly one write in ten, and collapses once exclusive
locking dominates at one in two. Pure reads are slower than a workload with 10%
writes. That is the shape the parking explanation predicts and the shape a
"writes are pure overhead" model cannot produce, so the hypothesis is supported
rather than merely plausible.

One point does not fit the smooth story: one write in a thousand measured slower
than pure reads, and slower than one in a hundred. Recorded rather than smoothed
over — a single writer that rarely appears may be enough to disturb the readers
without being frequent enough to keep them parked, but that is a guess and this
file has already spent one version calling a guess a hypothesis.

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
  ones. The harness prints that spread next to every median so the comparison
  does not have to be made from memory.
