# Contributing

## Requirements

Go 1.26 or newer. Docker or a local Redis is needed for conformance tests.

## Setup

    git clone https://github.com/aybavs/go-kv-store
    cd go-kv-store
    make build

## Tests

    make test
    make test-race

Conformance tests need a running Redis. One target brings it up, runs them and
takes it down again:

    make conformance

Or drive it by hand, which is the same thing:

    docker compose up -d
    REDIS_ADDR=127.0.0.1:6379 go test ./internal/conformance/
    docker compose down

Without `REDIS_ADDR` they skip with an explanatory message. CI fails if they
skip there, since a silent skip would turn the differential comparison into a
no-op that still reports green.

### Generated sequences

Part of the suite runs seeded command sequences against both servers and
compares them step by step. The seed list is fixed, so a divergence CI reports
is one you can run again. The failure message prints the seed, the diverging
step and the whole sequence leading to it.

    KV_GEN_SEED=42 go test ./internal/conformance/ -run TestGeneratedSequencesMatchRedis
    KV_GEN_RUNS=200 go test ./internal/conformance/ -run TestGeneratedSequencesMatchRedis

`KV_GEN_SEED` runs one seed — use the one from a failure. `KV_GEN_RUNS` runs
seeds 1..N, for looking further than CI does. Neither is set in CI: a seed taken
from the clock would report failures nobody could reproduce.

## Benchmarks

    make bench

### End-to-end measurement

The micro-benchmarks measure operations in isolation. The end-to-end harness
measures the server with real clients over real sockets, and reports the figure
v0.5 is about — **syscalls per command** — counted directly rather than inferred
from throughput.

    make bench-e2e                       # 5 repetitions, interleaved
    KV_BENCH=1 KV_BENCH_REPS=3 go test ./internal/server/ -run TestBenchEndToEnd -v

    make bench-profile                   # writes cpu.prof
    go tool pprof -top -nodecount=15 cpu.prof

It runs only under `KV_BENCH=1`, and CI does not run it. A benchmark CI must
pass is a benchmark that eventually gets weakened to keep CI green. One test in
that file does always run — `TestSyscallCounterCountsWhatItClaims` — because
every number the harness reports rests on the counter being right.

Configurations are interleaved rather than run in blocks, and the report prints
the spread beside the median. A difference smaller than the spread is not
separable from noise on that machine, and saying so is a result.

Never publish benchmark numbers that were not actually produced, and always
record the environment alongside them.

## Formatting and linting

    make lint

CI additionally runs `staticcheck`.

## Working on this codebase

Two rules are worth stating because they have already caught real defects here:

- A failing test is evidence. Never weaken an assertion, extend a timeout or
  add a wait to production code to make one pass — establish which of the two
  is wrong first.
- When a test guards something important, prove it is not vacuous: break the
  code deliberately and confirm the test catches it. Several tests in this
  repository were only trustworthy after that step, and one of them turned out
  to assert nothing at all.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/). Keep commits
small, logical and single-purpose:

    feat: add basic SET and GET commands
    fix: prevent race during key expiration
    perf: reduce command parser allocations
    test: add concurrent client tests
    docs: document persistence architecture

## Pull requests

Open a branch, push, and use the PR template. PRs are squash-merged, so the PR
title becomes the commit message on `main` and must follow Conventional
Commits.

Significant design changes — new commands, protocol changes, storage or
persistence changes — start with an issue before the code.
