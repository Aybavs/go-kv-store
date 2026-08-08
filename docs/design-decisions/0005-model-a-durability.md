# ADR 0005: Memory Becomes Visible Before the Durability Acknowledgement

## Status

Accepted — v0.3.0

## Context

A mutation has to be ordered against memory and against the log. Which comes
first is a real choice, not an implementation detail.

## Decision

**Model A.** Under one acquisition of the engine lock: derive the effect, append
it to the log's in-memory buffer, apply it to memory. Then release the lock and
wait for durability outside it.

```
mutation admitted
→ canonical effect ordered
→ memory applied / becomes logically visible
→ AOF progresses to written or synced
→ ACK according to the durability policy
```

Two properties hold structurally rather than by care:

- **`persisted order == applied order`**, because the append and the apply
  happen under the same lock acquisition, in that order.
- **The store lock is never held across disk I/O**, because the append is a pure
  in-memory operation. Real `write()` and `Sync()` happen in the writer
  goroutine.

## The consequence, stated rather than buried

> **Visibility and durability acknowledgement are different boundaries.** Another
> client may observe a mutation after its in-memory linearisation point but
> before the originating client receives its durability acknowledgement.

`always` guarantees that a successful ACK follows a successful `Sync`. It does
not promise that unsynced mutations are invisible to concurrent readers.

Checking persistence state before a mutation protects only against
**already-known** failures. It cannot prevent a failure caused by the current
write. Memory/disk divergence is therefore not claimed to be impossible; the
failure semantics below define what happens when it occurs.

## Rejected: strict durability before visibility

A single writer goroutine would derive, append, `write`, `Sync`, and only then
apply to memory. Because memory would lag the log, effect derivation would read
stale state — two in-flight `INCR`s on one key would both derive `SET k 1` —
requiring a pending write-set overlay consulted during derivation. That is a
second transient source of truth for in-flight state.

It is a legitimate design with a stronger availability story on write failure,
and it is not obviously worse. It was rejected for v1.0 because it also pushes
visibility behind durability, which can defeat the purpose of an `everysec`
policy entirely.

## Failure semantics

```
persistence already failed BEFORE admission
    → reject the mutation without changing memory     (-ERR persistence unavailable)

persistence fails AFTER an admitted mutation was applied
    → durability and shared-state integrity are uncertain
    → the process enters a fatal state and shuts down non-zero
```

Fatal is the only honest response to the second case. The realistic failure path
is that `write()` succeeds into page cache and the error surfaces at `Sync()`
time, by which point many mutations have already been acknowledged. Clients
blocked waiting are woken with an error; their mutation is already in memory and
may already have been read by someone else, and that irreducible ambiguity is
exactly why the condition is not recoverable.

Reads continue during the fatal-shutdown window. After such an exit, restart
replays the valid prefix and reconstructs a consistent state — the same kind of
valid prefix a power loss produces.

## `everysec` is not Redis's `everysec`

Redis acknowledges before the write. We acknowledge only after `write()` has
succeeded, so an ACK here means the data reached the operating system. A machine
or power failure can still lose writes made since the last successful `Sync`.

Same name, stronger guarantee. RESP compatibility must not be allowed to imply
an equivalence that does not exist, which is why the difference is stated in the
flag's own help text and not only here.

## Honest limitation: no checksums

RESP structural validation detects torn tails and structurally invalid records.
It does not detect every possible bit-level corruption: a flipped bit that still
forms a valid effect record is undetectable without a checksum. Checksums are not
added in v1.0 solely for completeness; they are listed as a future format
improvement. This is stated in the README as well, because it is the kind of gap
a user would reasonably assume closed.
