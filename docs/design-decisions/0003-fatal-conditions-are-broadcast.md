# ADR 0003: Fatal Conditions Are Broadcast, Not Delivered

## Status

Accepted — v0.1.0

## Context

Panics do not cross goroutine boundaries in Go. A connection goroutine that
discovers the shared state can no longer be trusted cannot simply panic and
expect the process to stop in a controlled way — the panic unwinds that
goroutine, `recover` in the connection handler catches it, and the server keeps
serving from state nobody can account for.

So a component that detects such a condition reports it to a supervisor, and the
lifecycle goroutine turns that into a non-zero exit. The question is how the
report travels.

## Decision

The supervisor **closes a channel**. It does not send a value.

```go
func (s *Supervisor) Fatal(err error) {
    s.once.Do(func() {
        s.mu.Lock()
        s.cause = err
        s.mu.Unlock()
        close(s.done)
    })
}
```

The cause is stored beside the channel and read with `Cause()`. `Fired()` reports
whether the condition has been raised at all, without blocking.

## Why a delivered value is wrong

A value sent on a channel is received **exactly once**. Whichever waiter happens
to read it consumes the report, and every other waiter waits forever for
something that will never arrive.

That is not hypothetical here. It shipped, and it was found in the PR-3 branch
review:

```
RunWithReady:
    select {
    case <-ctx.Done():    ← SIGTERM wins the race
        return s.gracefulShutdown()
    case <-s.sup.Done():  ← never reached; the select already committed
        return s.fatalShutdown(s.sup.Cause())
    }
```

Once the shutdown branch was taken, nothing read the fatal channel again. The
process logged `shutdown: complete` and **exited 0 after an invariant
violation** — the worst possible outcome, because an operator has no signal at
all that anything went wrong.

Closing broadcasts: every waiter observes it, in any order, as many times as
they look.

## What this decision requires elsewhere

The channel is only half of it. Two places also have to check:

- **`gracefulShutdown` checks `Fired()` before deciding its return value.** The
  select has already committed to the shutdown branch by then, so this is the
  last point at which a fatal can change the exit code.
- **Connection-level recovery checks `Fired()` before swallowing a panic.**
  Otherwise a panic that originated in the engine commit path — already reported
  — would be recovered by the connection handler, which would go on serving.

The guarantee is deliberately scoped to what is knowable: a fatal raised at any
point before the shutdown decides its outcome is surfaced. One raised after the
drain has genuinely completed is not, and must not be. Waiting for a report that
may never come would make every clean shutdown slower for no gain.

## Alternatives considered

**Deliver the error as a channel value.** Rejected — it is the defect above.

**Panic and let it propagate.** Rejected: it does not work in Go across
goroutines, and the connection handler's `recover` would swallow it. The
`Fired()` check in `recoverConn` exists precisely because a panic on the fatal
path must not be treated as an ordinary connection panic.

**A `sync.Once` plus a boolean flag polled by the lifecycle goroutine.**
Rejected: polling adds latency to the one path where latency is least welcome,
and a channel close is already the primitive that means "this happened, and
everyone can see it".

## Consequences

- Any number of components can wait on the fatal signal without coordinating.
- `Fatal` is idempotent. Only the first call records a cause and closes the
  channel, so a cascade of failures reports the first one rather than the last.
- Testing it is straightforward, and the tests are deterministic: the fatal path
  is driven directly rather than through `Run`, because which branch `Run`'s
  select takes depends on scheduling. A test that went through `Run` would be
  asserting that a race turned out a particular way.
