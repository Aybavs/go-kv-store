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

## Benchmarks

    make bench

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
