# ADR 0004: Log Canonical Effects, Not Client Commands

## Status

Accepted — v0.3.0

## Context

The append-only file has to record enough to reconstruct state after a crash.
The obvious choice is to log the client's command, which is what Redis does.

## Decision

Log the **resulting durable state mutation**, not the command that caused it.
Three shapes:

```
SET <key> <value>
SET <key> <value> PXAT <absolute-ms>
DEL <key> [<key> ...]
```

`EXPIRE` becomes a `SET` carrying the value the key already holds. `PERSIST`
becomes a `SET` with no expiry. `INCR`, when it arrives, becomes a `SET` of the
result.

## Why command logging fails

It makes recovery depend on prior state, so any read-modify-write command
silently breaks it:

```
t=0    SET k 5 EX 30          command log → SET k 5 PXAT 30000
t=10   INCR k → 6             (INCR preserves the TTL; expiry is still t=30)
t=30   k becomes logically absent
t=50   crash
t=50   replay: "SET k 5 PXAT 30000" has expired
                "INCR k" recreates k from nothing → k=1, no TTL     ← WRONG
```

An effect record is independent of every record before it, so the failure has
nowhere to originate.

Redis logs commands because it has hundreds of them and very large values, which
makes effect logging expensive. Our mutation surface is two shapes, so effect
logging is both smaller and more correct here. This is a case where copying the
reference implementation would have been the wrong call, and the reason is
specific rather than a matter of taste.

## Consequences

**Active expiration never appends anything.** A key that expired is already
absent on replay, by the rule that an expired `SET` record ensures absence. The
worker stays purely a memory-reclamation concern, which is a simplification the
decision *causes* rather than merely permits.

**`DEL` must be one variadic record.** Recovery restores a prefix and stops at
the first incomplete record, so splitting a three-key delete into three would
let a crash restore a partial one. One complete record is one recovery atomicity
unit.

**`MSET` is deferred** because it cannot be expressed as a single record in this
vocabulary. The vocabulary is not claimed to be complete forever; extending it
is a deliberate decision rather than an accident.

**Derivation reads current state.** `EXPIRE` and `PERSIST` need the value the
key already holds, so derivation happens inside the same lock acquisition as the
write. That is a constraint on where the code can live, and it is why the commit
path is shaped the way ADR-0005 describes.
