# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-08-08

### Fixed

- **Unbounded memory growth from a single connection.** `-max-array-elements`
  and `-max-bulk-length` each bounded one dimension of a request and multiplied
  without anything bounding their product, so the defaults permitted a 64 GiB
  frame. Measured before the fix: one connection sending a 300 MiB request it
  never completed drove resident memory from 5 MB to 1.09 GB. A new
  `-max-command-bytes` limit (default 128 MiB) bounds the total, checked against
  each declared length before any payload is read
- A decode buffer grown past 1 MiB by one large request is now released when
  that request completes. It was resliced to zero length and its capacity kept
  for the life of the connection, so a client could send one large request and
  then park that memory while idle

### Added

- `-max-command-bytes` flag, and `MaxCommandBytes` in `resp.Limits`. Setting it
  to 0 disables the check
- `SECURITY.md` now states what the request limits do and do not bound, with
  measured numbers: peak memory is roughly three times the configured value, and
  the bound is per connection rather than per server

## [0.1.0] - 2026-08-08

### Added

- TCP server with one goroutine per connection
- RESP2 subset codec written from scratch, hardened against fragmentation,
  malformed frames and oversized input
- `PING [message]`, `SET`, `GET`, `DEL`, `EXISTS`
- Configurable client limit, idle read timeout, write deadline and frame size
  limits
- Graceful shutdown with a mutation admission gate held inside the commit lock
- Fatal supervisor for conditions that invalidate shared-state assumptions
- Differential conformance tests against real Redis for the documented command
  subset, including a pipelined case that checks reply framing
- Micro-benchmarks for the store, codec and engine
- `docs/protocol.md`, `docs/architecture.md`, `docs/benchmarks.md` and two
  architecture decision records

### Fixed

Both of these were found while writing the protocol documentation and the
conformance suite for this release, before any of it was published.

- Simple String and Error replies no longer split into multiple frames when
  their text contains CR or LF. Error text quotes client-supplied data, so a
  command name containing CRLF produced extra replies and permanently
  desynchronised a pipelining client
- The command name echoed in an unknown-command error is bounded at 128 bytes.
  It is client-supplied and could previously be as long as the bulk-string
  limit

### Changed

- Error strings now follow Redis's rule for command names: an unknown command
  is echoed with the casing the client sent, and a wrong-arity error reports
  the canonical name in lowercase
- `SET` accepts more than three arguments and answers `ERR syntax error` for
  any option, rather than reporting a wrong argument count that is not the
  actual problem
