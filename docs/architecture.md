# Architecture

## Overview

    Client
      │ TCP
      ▼
    ┌──────────────────┐
    │ server           │  listener, connection set, limits, shutdown
    └────────┬─────────┘
             │ borrowed bytes
             ▼
    ┌──────────────────┐
    │ resp             │  RESP2 codec — the only package that knows the wire format
    └────────┬─────────┘
             ▼
    ┌──────────────────┐
    │ command          │  arity/type validation, dispatch, ownership boundary
    └────────┬─────────┘
             ▼
    ┌──────────────────┐
    │ engine           │  the only RWMutex, mutation ordering, admission gate
    └────────┬─────────┘
             ▼
    ┌──────────────────┐
    │ store            │  passive map: no locks, goroutines, I/O or clock reads
    └──────────────────┘

Dependencies run one way: `server → command → engine → store`, with `resp`
shared by `server`. Rules that must hold:

- `command` must not import `store`
- `server` must not import `store`
- `store` must not import `resp` or know about connections or goroutines

## Request lifecycle

1. The connection goroutine decodes one RESP2 array into `[][]byte`. **These
   slices are borrowed** — they point into a reused buffer and are valid only
   until the next decode.
2. `command` validates arity, then converts anything it will keep into an owned
   `string`. This is the ownership boundary.
3. `engine` takes its lock, applies the mutation or read, releases it.
4. The command returns a plain-Go `Reply`; the server encodes it as RESP and
   flushes.

Each completed response is flushed on its own. Batching is deliberately not
attempted: `bufio.Reader.Buffered()` cannot tell you whether a *complete* next
command is pending, and deferring a flush on that signal can deadlock — the
decoder blocks on an incomplete frame while the client waits for a reply still
sitting in our writer.

## Reply framing

RESP2 has two reply shapes, and they have different safety properties. Bulk
strings carry a length prefix, so their contents are opaque. Simple Strings and
Errors are single lines whose only terminator is the CRLF the encoder appends.

Error text quotes client-supplied data — an unknown command name, for one — so
a line reply is a place where attacker-controlled bytes reach the wire without
a length prefix in front of them. A CR or LF inside one ends the frame early,
and every byte after it is read by the client as an additional reply. The
damage is not to the one exchange but to the stream: a pipelining client
answers each subsequent command with the previous one's leftovers and never
resynchronises.

`resp.Writer` therefore maps CR and LF to spaces on the line-reply path, and
only there. Bulk strings pass through untouched, because for them those bytes
are payload. Keeping the rule in the encoder rather than at each call site is
deliberate: it is a property of the wire format, and `resp` is the package that
owns the wire format.

The quoted text is bounded as well as sanitised. Values may be as large as the
bulk-string limit, so an unbounded echo would let a small request produce a
large reply.

## Expiration

> Logical expiration and physical deletion are separate events.

A key is absent from the client's perspective the moment its deadline passes.
That is decided on the read path, under `RLock`, by comparing the deadline
against the clock — no write lock, no deletion. Reclaiming the memory is the
active worker's job and happens later.

Collapsing the two would be the easy mistake. A read that deleted what it found
expired would turn every `GET` into a potential writer, and would make the
worker's timing observable: a client could infer when reclamation last ran from
how a `DEL` counted. Two tests pin the separation directly, asserting both that
the key is invisible and that its entry is still physically present.

`store` keeps a second map holding only TTL-bearing keys. That index is what
makes bounded reclamation viable — sampling the main map finds nothing when
expiring keys are sparse. No claim is made about *which* keys a cycle examines;
Go's map iteration order is not a randomness contract. The guarantee is that
work per cycle is bounded and reclamation is eventual.

The worker is stopped before the mutation admission gate closes, not refused by
it. Reclamation is not a client mutation, and routing it through `ErrDraining`
would conflate two different things.

`store` never reads the clock — time is a parameter — and `engine` takes its
clock at construction. Both exist so that expiry is reachable in tests by moving
time rather than by waiting for it.

## Persistence

Off unless `--appendonly` is given. When it is on, every mutation is recorded as
a **canonical effect** — the resulting state, never the command that caused it.
`EXPIRE` becomes a `SET` carrying the value the key already holds, and `PERSIST`
becomes a `SET` with no expiry. See [ADR 0004](design-decisions/0004-canonical-effect-logging.md)
for the counterexample that rules command logging out.

### File format

Records are RESP2 arrays, decoded by the same hardened codec the wire protocol
uses. The codec is shared; the semantics are not. Two decoders sit over one
encoding and neither may reach the other: replay must never trigger client-only
behaviour, and must never append records back into the log.

The file opens with a 16-byte header — 8 bytes of magic, a 4-byte format version,
4 reserved — so a server pointed at a foreign file refuses to start rather than
interpreting its contents as data. An empty file is a new log, not a damaged
one, and that distinction decides whether the server starts.

### The commit path

Under one acquisition of the engine lock: derive the effect, append it to the
log's in-memory buffer, apply it to memory. Then release the lock and wait for
durability outside it. Two properties follow from the shape rather than from
care — persisted order equals applied order, and the store lock is never held
across disk I/O. [ADR 0005](design-decisions/0005-model-a-durability.md) covers
the consequence: visibility and durability acknowledgement are different
boundaries.

Progress is tracked as three logical sequence numbers rather than one overloaded
position:

    appliedSeq   last mutation applied to memory
    writtenSeq   last record fully delivered to the OS via write()
    syncedSeq    last record made durable via Sync()      (syncedSeq <= writtenSeq)

`everysec` waits for `writtenSeq`; `always` waits for `syncedSeq`. Group commit
is not a separate mechanism: a flush takes the whole pending buffer and syncs it
once, so everything in that buffer shares the syscall by construction.

A partial `write()` is treated as the normal contract it is. `writtenSeq` never
advances past a record that was not delivered whole.

`INCR` and `DECR` are the first commands whose effect depends on the value
already stored, which is exactly the shape
[ADR 0004](design-decisions/0004-canonical-effect-logging.md) argues about. Their
record is `SET key <result> PXAT <deadline>`, and the deadline is read from the
store as the absolute instant it holds — `store.ExpiresAt` exists for this and
nothing else. Rebuilding it by adding the *remaining* time to the current instant
would produce a different number, and in a record a different number is a
different expiry.

### Recovery

Three outcomes, and keeping them apart is the policy:

| Stream ends | Meaning | Action |
|---|---|---|
| between records | complete log | replay all of it |
| part-way through a record | torn tail — what a crash produces | truncate to the last boundary, continue |
| structurally wrong, anywhere | corruption | refuse to start, report the byte offset |

One `replayNow` is captured at the start and used for every deadline comparison,
so a long replay cannot have a key expire part-way through it.

An expired `SET` record **ensures the key is absent** rather than being skipped.
Skipping would resurrect the value that record replaced: `SET k old` followed by
an expired `SET k new PXAT T` must leave `k` gone, not holding `old`.

## Concurrency model

One goroutine per connection. All shared state is behind a single
`sync.RWMutex` in `engine`. Reads take `RLock` and run concurrently; mutations
take `Lock` and serialise.

This is the simplest model that is demonstrably correct. Sharding is not a
planned feature — it is a decision to be made against profiling data, if ever.
See [ADR 0001](design-decisions/0001-storage-concurrency-and-value-representation.md).

## Ownership

> Parser-returned byte slices are borrowed and temporary. Anything retained
> beyond command execution is copied into store-owned immutable memory.

Values are Go `string`, which is immutable and binary-safe. This removes the
aliasing bug class by construction rather than by discipline. Two tests assert
it directly, because the race detector cannot catch single-goroutine aliasing.

## Shutdown

    RUNNING → DRAINING → STOPPED

On `SIGINT`/`SIGTERM` the server stops accepting, closes mutation admission
inside the engine's own lock, and signals handlers. A command already executing
finishes and returns its reply; commands the client had merely buffered are not
started. Idle clients do not block shutdown: their parked reads are released by
setting an immediate read deadline.

The admission gate lives inside the commit lock rather than at the connection
level, because a connection-level check can be passed immediately before
shutdown and admitted afterwards.

> Once `BeginDrain()` returns, no new mutation can be admitted.

v0.1 has no persistence, so there is no finalisation stage yet. v0.3 adds it
between DRAINING and STOPPED.

## Failure handling

Errors fall into three classes:

| Class | Example | Effect |
|---|---|---|
| Client error | unknown command, wrong arity, unsupported option | RESP error, connection stays open |
| Connection-fatal | oversized frame, malformed RESP | RESP error, connection closed |
| Server-fatal | panic inside the engine commit path | reported to the supervisor, process exits non-zero |

A protocol error closes the connection because there is no reliable
resynchronisation point in the stream after one.

Panics do not cross goroutine boundaries in Go, so fatal conditions travel
through an explicit supervisor channel rather than by propagating a panic.
Connection-level recovery checks whether the supervisor has already fired so it
can never swallow an engine-fatal panic and keep serving.

The supervisor broadcasts by closing a channel rather than delivering a value.
A delivered value can be received exactly once, which is how a fatal raised
during a graceful shutdown was lost: the shutdown path had already committed to
its own case and nothing read the channel again, so the process exited zero
after an invariant violation. See
[ADR 0003](design-decisions/0003-fatal-conditions-are-broadcast.md).

## Testing strategy

Each layer is tested at the level where its property is actually visible.

| Property | Where it is pinned |
|---|---|
| Framing, fragmentation, malformed input, size limits | `resp` unit tests and a fuzz target |
| Encoder/decoder agreement | `resp` round-trip test, the property the v0.3 AOF will rely on |
| No buffer aliasing | dedicated ownership tests in `command` and `server` |
| Mutation admission under contention | `engine`, concurrent writers against a drain |
| Shutdown, limits, deadlines | `server`, over real TCP |
| Command semantics | `conformance`, differentially against real Redis |
| Interactions no scenario list contains | `cmdgen` sequences, run differentially and through recovery |

The conformance suite is the only one not written against our own expectations,
which is what makes it worth the dependency on an external server. It compares
error *class* rather than message text, because the documented contract is the
class.

### Generated sequences

`internal/cmdgen` produces bounded command sequences from a seed, over four
keys. Two suites consume them: the conformance suite compares each sequence
against Redis step by step, and `engine` replays the same sequence out of the
append-only file and compares the recovered state against the live one.

Two rules keep them from being flaky. A sequence is a pure function of its seed
— no clock, no global rand, no map iteration — so a divergence is reproducible
from the seed alone, and the seed list is fixed rather than clock-derived. And
every generated expiry is at least 100 seconds, because the two servers are
asked in turn and a short TTL would be comparing the clock rather than the
implementations. Expiry *transitions* stay in the handwritten scenarios, where
the wait is deliberate and bounded.

The recovery comparison happens after **every** step rather than once at the
end, and that was not a stylistic choice. Only 3 of the 64 final key-states
across the seed list are decided by a TTL-preserving `INCR`; every other key is
last written by a later `SET`, `DEL` or `EXPIRE` whose record is correct either
way. An end-state comparison therefore hid the exact defect the suite exists to
catch.
