# ADR 0007: What v1.0 Stabilises, and What It Does Not

## Status

Accepted — v1.0.0

## Context

Every milestone up to this one added behaviour. This one adds a promise.

Semantic versioning says a `1.x` release is backward compatible with every other
`1.x`, but it does not say *with respect to what*. For a library the answer is
usually the exported API; this repository has no exported API, because every
package lives under `internal/` and the compiler refuses to let an external
module import any of it. That is a consequence of the layout chosen in v0.1, not
an oversight, and it means the question has to be answered from scratch: what is
the surface a user of this program actually depends on?

A promise nobody has written down cannot be kept or broken. Worse, an unwritten
promise gets broken by an ordinary-looking pull request, and nobody notices until
someone else's system stops working.

## Decision

A `1.x` release is compatible with every other `1.x` in these respects:

| Covered | Why |
|---|---|
| Every command already listed in `docs/protocol.md` and the **shape** of its reply | This is what a client program is written against. A `GET` that started replying with an array would break every caller. A minor release may add a command; it may not remove or reshape an existing documented one |
| Error **classes**, as enumerated in `docs/protocol.md` | A client can branch on "this was a wrong-arity error". The enumeration is the contract |
| Flag names and their **defaults** | See below — a default is behaviour nobody typed |
| Append-only file **format version 1** | A file written by any `1.x` is readable by any other `1.x`. Data outlives the process that wrote it |
| Process **exit codes** | A supervisor branches on them. `0` means the drain completed and persistence finalised; non-zero means it did not |

And explicitly **not** compatible in these:

| Not covered | Why |
|---|---|
| Exact error message **text** | The class is the contract; the text exists to be read by a person. Freezing it makes a clearer message a breaking change |
| Log output, its format and its fields | Diagnostics, not an interface. Anything a program should branch on belongs in an exit code |
| Performance figures | `docs/benchmarks.md` is a measurement of one machine at one version, not a guarantee |
| The Go packages under `internal/` | Not importable. There is no API here to stabilise |
| Behaviour outside the documented subset | An unimplemented Redis command answers with the unknown-command error today. That is not a promise that it always will |

### Defaults are part of the contract

A flag default is behaviour a user never typed and therefore never opted into.
Changing `-max-command-bytes` or `-appendfsync` in a patch release would change
a running system silently, and the operator would have no reason to read the
release notes for a patch. So a `1.x` release may **add** a flag, and may not
remove one, rename one, or change what an existing one defaults to.

### The promise is pinned by tests, not only by prose

Three of the five covered rows are mechanically checkable, and each has a test
whose job is to fail when the promise is about to be broken:

- the flag surface — every name and every default, so a rename fails a test
  rather than reaching a user;
- the file format — a committed fixture, replayed by the current reader, so a
  format change that would strand existing files cannot land quietly;
- the error classes — the exact strings in one table, so a reworded message is a
  deliberate edit rather than a side effect.

The remaining two are already covered by suites that exist: reply shapes by the
differential conformance tests against real Redis, and exit codes by the
durability and shutdown suites.

The command promise is monotonic, not frozen at the exact v1.0 list. v1.1 adds
`KEYS`, `SCAN`, and `DBSIZE`; older documented commands keep their shapes, while
the new nested `SCAN` shape joins the protected surface from the release that
introduces it. Treating every addition as a major version would make a stable
server impossible to extend without making existing clients any safer.

This matters more than the prose. A stability promise that lives only in a
document is broken by a pull request that looks reasonable, and the test is what
turns "we intend to" into "you will be told".

## Rejected: stabilise everything, including message text and log format

It sounds safer and it is worse.

Error text exists to be read by a human at a terminal. Freezing it means a
message that turns out to be confusing cannot be improved without a major
version, so it never gets improved. The same applies to log lines, which change
as diagnostics change — and a program that parses our log output has built on a
surface we would then be unable to develop.

The cost of excluding them is real and is accepted: a user who greps for exact
error text has no promise. The mitigation is that they do not need to, because
the class enumeration exists precisely so that branching on an error does not
require string matching.

## Rejected: stay on `0.x`

This is what a project does when it wants the freedom of instability without
saying so out loud.

It does not fit the facts. The command surface has not changed since v0.4, the
file format has not changed since v0.3, and the flags have not changed since
v0.3. The contract is stable in practice; declining to say so would be a way of
avoiding the obligation rather than a technical position.

## What a 2.0 would be for

Naming this now makes the escape hatch a decision rather than a surprise:

- **Extending the canonical record vocabulary.** `MSET` is deferred precisely
  because it cannot be expressed as one record, and one complete record is one
  recovery atomicity unit ([ADR 0004](0004-canonical-effect-logging.md)).
  Extending the vocabulary means format version 2 and a decision about whether
  version 1 files are still readable.
- **RESP3.** A different wire format, not an addition to this one.
- **Removing or renaming a flag**, or changing a default.

None of these are planned. They are written down so that "why would there ever
be a 2.0?" has an answer other than a shrug.

## What 1.0 does not mean

It does not mean finished. Single node, strings only, no authentication, no
TLS, no checksums in the append-only file — all still true, all still in the
README's limitations. Some of those are permanent scope decisions and some are
simply not done, and the release notes say which is which.

`1.0` means the contract above is stable. That is a smaller claim than
"complete", and it is the one that is true.
