# Security

## Status

**This project is not production-ready.** It is built to explore storage and
networking internals. Do not expose it to an untrusted network.

## Known limitations

- No authentication and no TLS. Anyone who can reach the port has full access.
- No per-client memory accounting; a client may hold a large output buffer.
- The dataset must fit in memory; there is no eviction policy.
- Request size limits exist and are configurable, but they have not been audited
  against a determined attacker. What they do and do not bound is described
  below, with measured numbers rather than estimates.

## Request memory bounds

`-max-command-bytes` (default 128 MiB) bounds the total argument bytes of one
request. It is the limit that decides how much a single connection can make the
server hold at once: `-max-array-elements` and `-max-bulk-length` each bound one
dimension and multiply without it, so their defaults alone permit a 64 GiB
frame. The check runs against each declared length before any payload is read,
so an oversized request is refused rather than buffered.

Two things it does not do, both measured on an Apple M4:

- **Peak memory is several times the configured value**, not equal to it.
  Filling a 128 MiB command limit peaks at about 519 MB resident (median of five
  runs, Apple M4). The decode buffer grows by doubling, so during a grow both
  arrays are live, and every array it has outgrown stays resident until the
  collector returns it.

  Until v0.5.1 the final growth also overshot the limit itself: a payload the
  size of the whole limit filled the buffer, and the two bytes of CRLF after it
  asked for an array twice the limit. The same measurement was **648 MB** then.
  The new array is now capped at the limit, which `internal/resp` pins with a
  test rather than leaving to the allocator.
- **It bounds one connection, not the server.** With `-max-clients` at its
  default of 1024, the worst case is that figure multiplied by the client limit.
  Lower both flags together if the server is reachable by untrusted clients.

A connection does not keep the peak after the request that caused it: a decode
buffer above 1 MiB is released once its command completes, so a client cannot
send one large request and then park that memory while idle.

Setting `-max-command-bytes` to 0 disables the check and restores the unbounded
behaviour. Do not do this on an untrusted network.

## Reporting

Open a GitHub issue for anything that is not sensitive. For anything you would
rather not disclose publicly, use GitHub's private vulnerability reporting on
this repository.
