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
after an invariant violation.

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

The conformance suite is the only one not written against our own expectations,
which is what makes it worth the dependency on an external server. It compares
error *class* rather than message text, because the documented contract is the
class.
