# go-kv-store

[![CI](https://github.com/aybavs/go-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/aybavs/go-kv-store/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/aybavs/go-kv-store)](go.mod)
[![Release](https://img.shields.io/github/v/release/aybavs/go-kv-store)](https://github.com/aybavs/go-kv-store/releases)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A Redis-inspired in-memory key-value store built from scratch in Go to explore
TCP networking, concurrency, protocol design, expiration, persistence and
storage engine design.

**What it is:** a readable, tested implementation of how a key-value server
works end to end — from bytes on a socket, through a protocol parser and a
command layer, to a synchronised in-memory store — with its command semantics
checked against real Redis.

**What it is not:** a Redis replacement. Single node, strings only, no
authentication, no TLS. See [Limitations](#limitations).

## Compatibility

From 1.0, every `1.x` release is compatible with every other in five respects:
the documented **command set and reply shapes**, the **error classes** in
[docs/protocol.md](docs/protocol.md), **flag names and their defaults**,
**append-only file format version 1**, and **process exit codes**.

Deliberately not covered: exact error message text (the class is the contract,
the wording exists to be read), log output, performance figures, and the Go
packages under `internal/` — which are not importable, so there is no API here
to stabilise.

Three of those five are pinned by tests rather than by intent, so a rename, a
changed default or a format bump fails the build instead of reaching you.
[ADR 0007](docs/design-decisions/0007-what-v1-stabilises.md) has the reasoning
and what a 2.0 would be for.

## Why

Because "I used Redis" and "I know how Redis works" are different sentences.
Every layer here — the wire format, the ownership rules, the lock placement,
the shutdown contract — was a decision with alternatives, and the ones that
mattered are written down in [docs/design-decisions](docs/design-decisions).

## Features

- **RESP2-compatible subset** — `redis-cli` connects and works, with a parser
  written from scratch and hardened against fragmentation, malformed frames and
  oversized input
- **Differential testing against real Redis** — the documented command subset is
  compared against a reference implementation, not just against our own
  expectations
- **One lock, in one place** — `engine` owns the only mutex; `store` is a passive
  data structure with no locks, goroutines, I/O or clock reads, so its tests are
  fully deterministic
- **No buffer aliasing** — parser output is borrowed; anything the store keeps is
  copied to an immutable string, asserted by dedicated tests
- **Graceful shutdown that means something** — mutation admission closes inside
  the commit lock, so a request cannot slip in after draining begins
- **Seeded command-sequence generation** — bounded random sequences over a small
  key space, run against both Redis and our own crash recovery, because the
  states worth testing are the ones nobody thinks to write down
- **One write syscall per batch, not per reply** — a reply is flushed when the
  reader is about to block, which is the only moment at which holding it would
  be wrong. Syscalls per command are counted directly rather than inferred

## Architecture

    Client ──TCP──▶ server ──▶ command ──▶ engine ──▶ store
                       │           │          │
                      resp    ownership   the only
                    (codec)    boundary    RWMutex

See [docs/architecture.md](docs/architecture.md).

## Install

Prebuilt binaries for linux and darwin on amd64 and arm64 are attached to every
[release](https://github.com/Aybavs/go-kv-store/releases), with a `SHA256SUMS`
file beside them. Verify the download before running it:

    curl -sSLO https://github.com/Aybavs/go-kv-store/releases/download/v1.0.0/kv-server_v1.0.0_darwin_arm64.tar.gz
    curl -sSLO https://github.com/Aybavs/go-kv-store/releases/download/v1.0.0/SHA256SUMS
    shasum -a 256 --ignore-missing -c SHA256SUMS
    tar xzf kv-server_v1.0.0_darwin_arm64.tar.gz

`--ignore-missing` matters: `SHA256SUMS` covers all four archives, and without it
the three you did not download are reported as `FAILED open or read`. On Linux
the command is `sha256sum --ignore-missing -c SHA256SUMS`.

**What this establishes and what it does not.** The checksums detect a corrupted
or truncated download. They are not signatures: anyone who could replace the
archive could replace `SHA256SUMS` alongside it, so this is an integrity check
against transport, not a proof of provenance.

## Quick start

From source:

    go build -o bin/kv-server ./cmd/kv-server
    ./bin/kv-server --port 6380

In another terminal:

    $ redis-cli -p 6380
    127.0.0.1:6380> SET language go
    OK
    127.0.0.1:6380> GET language
    "go"
    127.0.0.1:6380> EXISTS language
    (integer) 1
    127.0.0.1:6380> DEL language
    (integer) 1

With Docker:

    docker build -t go-kv-store .
    docker run -p 6380:6380 go-kv-store

## Commands

| Command | Reply |
|---|---|
| `PING [message]` | `PONG`, or the message |
| `SET key value [EX s \| PX ms]` | `OK` |
| `GET key` | value, or nil if absent |
| `DEL key [key ...]` | number of keys removed |
| `EXISTS key [key ...]` | number of keys present |
| `EXPIRE key seconds` | `1` if applied, `0` if no such key |
| `TTL key` | seconds left, `-1` no TTL, `-2` no key |
| `PERSIST key` | `1` if a TTL was removed, `0` otherwise |
| `MGET key [key ...]` | one value per key, nil where absent |
| `INCR key` / `DECR key` | the value after the change; any TTL is preserved |

Full wire format, error classes and the deviations from Redis are in
[docs/protocol.md](docs/protocol.md).

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | bind address |
| `--port` | `6380` | listen port |
| `--max-clients` | `1024` | maximum concurrent connections |
| `--timeout` | `0` | idle read timeout (0 disables) |
| `--shutdown-timeout` | `10s` | graceful shutdown budget |
| `--max-bulk-length` | `64MiB` | maximum bulk string length |
| `--max-array-elements` | `1024` | maximum arguments per command |
| `--max-command-bytes` | `128MiB` | maximum total argument bytes in one command |
| `--loglevel` | `info` | `debug`, `info`, `warn`, `error` |
| `--appendonly` | `false` | write an append-only file so data survives a restart |
| `--appendfilename` | `appendonly.aof` | path to the append-only file |
| `--appendfsync` | `everysec` | `always` or `everysec` — see below |

`--appendfsync everysec` is **not** Redis's `everysec`. Redis acknowledges
before the write; we acknowledge only once `write()` has succeeded, so an
acknowledgement means the data reached the operating system. A machine or power
failure can still lose writes made since the last successful `Sync`. Same name,
stronger guarantee.

CLI flags only — there is no config file and no environment-variable layer, so
there is no precedence rule to learn.

## Benchmarks

Micro-benchmarks, end-to-end throughput, latency distributions and syscalls per
command are in [docs/benchmarks.md](docs/benchmarks.md), each reproducible by a
documented command.

## Roadmap

Expiration (v0.2), append-only persistence with crash recovery (v0.3), extended
commands (v0.4), performance work (v0.5). Next is v1.0: a documentation audit
and a stable contract. See [ROADMAP.md](ROADMAP.md).

## Development

    make build       # build the binary
    make test        # run tests
    make test-race   # run tests under the race detector
    make lint        # gofmt + go vet
    make bench       # run benchmarks

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Limitations

Stated plainly rather than buried:

- **Single node.** No replication, no clustering.
- **No authentication, no TLS.** Anyone who can reach the port has full access.
- **Persistence is opt-in.** Without `--appendonly`, data is lost on restart.
- **No checksums in the append-only file.** Structural validation catches torn
  tails and invalid records, but a flipped bit that still forms a valid record
  is undetectable. Listed as a future format improvement rather than shipped for
  completeness.
- **The append-only file is never rewritten or compacted, so it grows without
  bound.** It records every mutation, not the current state: writing one key
  6 000 times produces a 243 KB file holding one live key, and recovery replays
  all of it. Size it against your write volume rather than your dataset, and
  restart time grows with the file. Snapshotting and AOF rewrite are non-goals,
  not omissions.
- **Expiration is per-key only.** No eviction policy and no maxmemory.
- **Strings only.** No List, Hash or Set.
- **No transactions and no Pub/Sub.**
- **No `MSET`.** Not an oversight: it cannot be expressed as one canonical
  append-only record, and one complete record is one recovery atomicity unit.
  Extending the record vocabulary is a decision for its own milestone. `MGET` is
  here because it is read-only and never touches the log.
- **The dataset must fit in memory**, and nothing evicts anything.
- **RESP2 only, and a deliberately small subset of it.** No RESP3, no inline
  commands, no `HELLO`, `CLIENT`, `INFO`, `SELECT` or `FLUSHDB`; there is one
  database. [docs/protocol.md](docs/protocol.md) lists every supported command
  and every deviation from Redis.
- **Not intended for production workloads.**

## License

MIT. See [LICENSE](LICENSE).
