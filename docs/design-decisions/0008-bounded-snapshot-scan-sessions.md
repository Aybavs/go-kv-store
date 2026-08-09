# ADR 0008: Bound SCAN with Expiring Key-Name Snapshot Sessions

## Status

Accepted — v1.1.0

## Context

A key browser needs to enumerate keys without making one unbounded network
reply. The first implementation used a numeric offset as a stateless cursor:
every `SCAN` call copied all logically live names, sorted the complete snapshot,
then selected one page. It kept the write path unchanged and had no server-side
cursor state, but those properties were not enough. The cost paid once by
`KEYS` was paid again for every `SCAN` page.

The mandatory benchmark gate measured the failure rather than inferring it. At
100,000 keys a page cost about 20.2 ms and allocated 1.606 MB regardless of
COUNT. A full traversal took 199.710 seconds at COUNT 10, 19.872 seconds at
COUNT 100, and 1.990 seconds at COUNT 1000, allocating 16.056 GB, 1.606 GB, and
160.563 MB respectively. The design was stopped before adding an index.

Three replacement families were considered: a stateless hash-range cursor, a
maintained ordered or bucket index, and a bounded server-side snapshot session.
The browser needs modest point-in-time pagination, not a key-ordering subsystem
with permanent write-path complexity, so the session design was prototyped and
subjected to a second gate before documentation was approved.

## Decision

`SCAN 0` captures every logically live key name under the engine data `RLock`
using one clock reading. It then releases that lock, applies the fixed byte glob
`MATCH` to the full snapshot in place, and sorts the retained names bytewise.
Values are never captured.

If the requested COUNT completes the result in one page, the server returns
cursor `0` and retains no session. Otherwise a separately locked session manager
stores the filtered, sorted names and returns a cryptographically random,
nonzero uint64 rendered as decimal. Cursors are opaque and single-use: each
successful nonterminal continuation deletes the presented token and returns a
fresh token for the same session. Completion deletes the session and returns
`0`.

The public contract follows from what is retained:

- The traversal is a point-in-time snapshot of key names, not values. Inserts
  after capture are absent. Names deleted or expired later may still appear,
  values may change, and following every replacement cursor to `0` returns
  every captured matching name exactly once.
- `MATCH` defaults to `*` and is fixed at creation because filtering has already
  happened. A continuation may omit it or repeat identical bytes. A change
  fails without consuming the cursor.
- `COUNT` is only page state and may change between calls. It does not bound the
  snapshot/filter/sort work on the initial call. Continuations perform none of
  those phases.
- Clients must not interpret cursor values or depend on the current bytewise
  key ordering.
- Unknown, expired, completed and consumed cursors are all invalid. Restarting
  the process invalidates every outstanding cursor. Malformed, negative and
  overflowing cursor text is invalid before session lookup.

The server retains at most 16 unfinished sessions and a conservative 128 MiB
estimate globally. A session expires after 30 seconds without a successful
continuation. Admission first removes expired sessions; if the count or memory
bound would still be crossed, it returns `ERR scan session limit reached`.
These are fixed policy constants, not public flags.

Memory accounting includes the key-slice capacity times a conservative
16-byte string header, every retained key byte, the MATCH pattern bytes, and 64
bytes of session overhead. Arithmetic saturates on overflow, so an estimate can
fail closed rather than wrap below the cap. This is an accounting bound, not an
RSS prediction: allocator metadata and runtime retention are outside it.

The session manager's mutex is independent of the engine data `RWMutex`. The
implementation never holds both. Filtering, sorting, session admission,
paging, reply construction, RESP encoding, and network writes all happen
without the data lock. Continuations do not touch the store at all.

There is no cleanup goroutine. Creation and continuation lazily remove sessions
whose inactivity deadline has arrived; completion removes its own session.
Server shutdown clears all sessions, including after the last registered
handler leaves if the shutdown budget elapsed first. Every removal releases
both the entry and its retained-byte counter.

Discovery is read-only. `KEYS`, `SCAN`, and `DBSIZE` append no AOF record and
wait for no durability acknowledgement. AOF format version 1 is unchanged.
Sessions are neither persisted nor rebuilt during recovery; recovered data can
be scanned only by starting again at cursor `0`.

## Benchmark gate

The replacement gate used the same Apple M4 / Go 1.26.1 developer laptop and
measured 1k, 10k, and 100k datasets at COUNT 10, 100, and 1000. The accepted
matrix contained 63 rows, 20 fixed iterations and three repetitions per row.
Every one of the 27 traversal repetitions reported exactly one snapshot, one
filter, and one sort.

At 100k, snapshot time under the data `RLock` averaged 1.426 ms, full matching
averaged 1.873 ms outside the lock, and sorting averaged 18.655 ms outside it.
First pages averaged 22.096/22.021/22.045 ms for COUNT 10/100/1000.
Continuations averaged 1.237–1.483 µs with zero measured allocation. Complete
traversals averaged 24.746/22.380/22.028 ms and allocated 1,605,872 bytes once.

Against the rejected 100k baseline, those traversals are 8,070.3×, 888.0×,
and 90.3× faster, with 9,998.5×, 999.9×, and 100.0× less allocation. One 100k
session retains an estimated 2,488,959 bytes; sixteen retain 37.979 MiB, 29.67%
of the cap. Completion and exact-30-second expiry released all accounting with
zero measured allocation.

Under continuous repeated-capture load, the interleaved harness recorded
identical baseline/load p50/p95/p99 values: GET 42/84/84 ns and SET
83/125/125 ns. This does not prove zero maximum or tail impact—the harness did
not record maxima, and loaded SET sample throughput was 5.9–6.4% lower. The
decision is therefore scoped to bounded interactive browsing at the measured
100k-key scale, not an unbounded-session or server-wide latency guarantee.

The complete commands and all 1k/10k/100k results are published in
[`docs/benchmarks.md`](../benchmarks.md).

## Rejected: keep the stateless sorted-offset baseline

It has the smallest state model and the largest measured traversal cost. COUNT
does not constrain snapshot or sort work, so small pages multiply O(N) capture,
O(N log N) sorting, lock exposure, and allocation by the number of pages. The
100k/COUNT-10 result is enough to reject it for the intended browser.

Its mutation semantics were also weaker: rebuilding a different snapshot for
every numeric offset allowed misses and duplicates. The session design instead
fixes key-name membership for one bounded traversal.

## Rejected: add a maintained ordered or bucket index

An index could target O(log N + COUNT) page reads, but permanently charges every
write for a browsing optimization. SET would need exact insert/overwrite
membership, DEL and active expiration would need exact removal, lazy expiration
would need stale-entry reconciliation, and AOF recovery would need to rebuild
data and index without divergence. It also adds permanent O(N) memory and new
ordering invariants.

The measured session replacement already makes a 100k/COUNT-10 traversal about
25 ms while leaving all mutation and recovery paths unchanged. There is no
evidence justifying the index's permanent cost. It remains an optimization that
requires a new design and measurements, not a follow-up hidden inside SCAN.

## Rejected: use a stateless hash-range cursor

Hash ranges could remove sorting without changing writes, but each page would
still examine and allocate an O(N) live snapshot. The old 100k snapshot alone
was about 1.307 ms and 1.606 MB, implying roughly 16.056 GB of snapshot
allocation across 10,000 COUNT-10 pages before hashing and filtering. Collision
ordering and range-boundary semantics would also become protocol invariants.

It trades the dominant sort away but keeps the repeated full-keyspace work that
the traversal gate was designed to catch.

## Consequences

The first page remains an O(N) capture/filter plus O(M log M) sort for M
matching names, and it retains O(M) state until completion or expiry. A client
can exhaust the global session allowance and receive a resource error. Deleted
key names can keep their bytes reachable for up to the inactivity TTL. Restart
requires the client to begin again.

In return, later pages are independent of dataset size, a complete traversal
pays the expensive phases exactly once, writes and recovery acquire no new
index invariants, the store stays passive, and retained state has explicit time,
count, and memory bounds.
