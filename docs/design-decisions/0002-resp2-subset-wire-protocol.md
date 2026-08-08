# ADR 0002: A Small RESP2 Subset Instead of a Proprietary Protocol

## Status

Accepted — v0.1.0

## Context

The server needs a wire protocol. Writing one from scratch teaches protocol
design; adopting RESP2 gives interoperability. The deciding factor was
testability: without an independent reference implementation, the only thing
vouching for our command semantics is our own test suite.

## Decision

Implement a deliberately small RESP2-compatible subset: arrays of bulk strings
for requests; Simple String, Error, Integer, Bulk String, Null Bulk String and
Array for replies. The parser is written from scratch and hardened against
fragmentation, partial reads, malformed frames and oversized input.

Real Redis becomes a reference oracle for differential conformance testing —
the role SQLite played in the sibling `sql-query-engine` project. `redis-cli`
and `redis-benchmark` work without any client tooling of our own, so `kv-cli`
is optional rather than required.

Conformance covers **only** the command semantics we document. Redis behaviour
outside our documented subset is not part of our contract.

## Alternatives Considered

**A custom text protocol (Memcached-style: text command line, length-prefixed
payload).** Rejected. It would have made protocol design our own, but there is
no oracle, no ecosystem benchmarking tool, and a client would have to be
written before anything could be exercised end to end.

**A fully binary protocol (fixed header, opcode, lengths).** Rejected as
premature: no measured performance problem justifies losing the ability to
inspect traffic with ordinary tools.

## Consequences

- Protocol *implementation* learning is fully retained: framing, TCP
  fragmentation, buffered IO, malformed input, size limits.
- Protocol *format design* is not exercised; that trade was made deliberately.
- Adopting a format is not the same as adopting its safety properties. RESP2's
  single-line reply types have no length prefix, so their only terminator is
  the trailing CRLF — which means any reply built by concatenating
  client-supplied text is a frame-splitting vector until something neutralises
  CR and LF. We shipped that defect and found it while writing this document;
  it is fixed in the encoder, where the wire format is owned. The general
  lesson is that a borrowed format brings borrowed obligations, and they are
  not listed anywhere in the format's description.
- For the same reason, any client-supplied text quoted back in an error must be
  bounded. Our bulk-string limit is 64 MiB, so an unbounded echo is an
  amplification primitive.
- Naming collisions carry risk. Our `everysec` durability policy (v0.3) will be
  stronger than Redis's; wherever a name is shared, the difference must be
  stated explicitly rather than implied to be equivalent.
- `redis-benchmark` results are an ecosystem baseline only. They are never
  presented as an apples-to-apples claim that this implementation is faster
  than Redis.

## Notes on fidelity

Matching Redis is a means, not an end. Where the two differ we either match it
deliberately or record the deviation in `docs/protocol.md`; there is no third
category. Error-string casing was decided this way at v0.1.0: an unknown
command echoes the client's casing and a wrong-arity error reports the
canonical lowercase name, because Redis distinguishes the two cases for a
reason that survives restatement — an unknown command has no canonical form to
report.
