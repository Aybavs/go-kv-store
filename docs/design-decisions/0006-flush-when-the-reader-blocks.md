# ADR 0006: Flush a Reply When the Reader Is About to Block

## Status

Accepted — v0.5.0

## Context

Until v0.5 the connection loop flushed after every reply. That is one `write`
syscall per command, and for a client that pipelines, most of those syscalls buy
nothing: the replies are going to the same socket and could travel together.

The asymmetry was measured before anything was changed. On a 64-deep pipeline
the server performed **0.016 reads per command and 1.000 writes**. Sixty-four
commands arrived in one segment and were parsed out of one buffer; sixty-four
replies left in sixty-four separate writes.

A CPU profile of the same shape put ~78% of server time in raw syscalls, split
roughly evenly between `bufio.(*Reader).fill` and `bufio.(*Writer).Flush`.
`engine`, `store` and the mutex did not appear at all — the third consecutive
milestone at which that has been true.

## The rejected answer, which the codebase has warned about since v0.1

Defer the flush while `bufio.Reader.Buffered() > 0`.

**This deadlocks.** `Buffered()` says bytes are pending, not that a *complete*
command is. A client that has sent a partial command and is waiting for the
previous reply before finishing it leaves the server blocked in the decoder
while the reply sits in the writer. Neither side moves.

`docs/architecture.md` has carried that warning since v0.1 without a resolution.
This ADR is the resolution.

## Also rejected

**A "is a complete command buffered?" predicate.** It answers the question
honestly, and it costs a second, non-consuming implementation of frame parsing
that must agree with the real one forever. Two parsers over one wire format is a
bug shape this project has already paid for once: v0.3 found that a torn tail
and a structurally corrupt record were distinguished only by their message text,
and had to make the distinction structural instead.

**An event loop over `epoll`/`kqueue`.** It is what Redis does and it would do
more than this. It also replaces the concurrency model wholesale, and
goroutine-per-connection is one of the things this project exists to build and
understand. Not a 0.x change.

**`writev`.** Go's `net` package does not expose scatter-gather writes in a form
that helps this shape, and `bufio` already coalesces.

## Decision

**Invert the question.** Rather than asking *may I defer this flush?*, flush at
the one moment when deferring would be wrong: immediately before the reader
issues a blocking read.

`net.Conn` is an interface, so the reader is handed a wrapper whose `Read`
flushes any pending reply before delegating.

Nothing has to decide whether a command is complete. The reader running out of
buffered bytes **is** the condition that matters, and it reports it by asking
for more. The consequences fall out rather than needing care:

- A request/response client is unaffected. Its buffer is empty after every
  command, so the hook fires before every read and the flush lands exactly where
  it landed before: one write per reply, no added latency.
- A pipelined batch costs one write. Commands are parsed out of the buffer with
  no underlying read between them, so nothing flushes until the batch is
  exhausted.
- Memory was already bounded. `resp.Writer` buffers 4 KiB and flushes when full,
  so a client pipelining ten thousand commands cannot make the server hold ten
  thousand replies.

## What the decision requires elsewhere

A deferred flush moves work onto paths that did not previously have it, and two
of them need naming, because neither is visible in a benchmark.

**The exit paths.** `serve` returns from the top of its loop when the server is
draining — before any read, so nothing triggers the deferred flush. A reply
already encoded at that moment would be dropped: the client sees the connection
close with no answer to a command the server had executed. `serveConn` flushes
after `serve` returns, and a test constructs that state directly rather than
waiting for the drain to land in the right microsecond.

**The panic path deliberately does not flush.** `recoverConn` runs after an
invariant may already be broken, and emitting a half-written reply is worse than
emitting none.

**A failed flush surfaces as a read error**, which is `ReadCommand`'s third error
class: a transport failure, closed without replying. That is the correct
handling — explaining the failure to the client means writing to the thing that
just failed. The flusher latches, because `resp.Writer`'s contract forbids
flushing after a write error: the buffer holds a partial frame.

**The write deadline is set per reply, not per flush.** It is absolute, so the
one established when the last reply was encoded still governs the write that
sends the batch, a few microseconds later. Setting another at flush time would
add a syscall to the path whose purpose is to remove them.

## What it cost, measured

Eleven interleaved repetitions, both behaviours alternating inside one process,
because this machine's end-to-end spread is up to 9% and two runs minutes apart
could not separate anything smaller.

| workload | before | after | writes/cmd |
|---|---|---|---|
| GET, 10 conns, pipeline 8 | 322 241 | 732 262 | 1.000 → 0.125 |
| GET, 10 conns, pipeline 64 | 409 084 | 4 342 347 | 1.000 → 0.016 |
| GET, 50 conns, pipeline 64 | 397 316 | 5 642 551 | 1.000 → 0.016 |
| SET, 10 conns, pipeline 64 | 403 802 | 1 937 437 | 1.000 → 0.016 |
| SET, pipeline 64, `everysec` | 242 294 | 468 605 | 1.000 → 0.016 |

Writes per command now follow reads exactly.

Request/response is flat at one and ten connections, which is the claim that
mattered as much as the speedup. At fifty connections the difference is **not
separable from noise by this method**: three interleaved runs gave −6.5%, −4.0%
and +0.2%, with the before arm's own spread at 9–13%, and the two arms matched
when that workload was measured on its own. That is recorded as unsettled rather
than rounded to a conclusion.

## What this does not address

The read side. A request/response client still costs one read and one write per
command, and that is where the remaining syscalls are. Reducing it needs
something that serves many connections per wakeup — an event loop — which is out
of scope for the reasons above.
