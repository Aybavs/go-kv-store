BINARY := kv-server

.PHONY: build run test test-race conformance lint bench bench-e2e bench-profile clean

build:
	go build -o bin/$(BINARY) ./cmd/kv-server

run: build
	./bin/$(BINARY)

test:
	go test ./...

test-race:
	go test -race ./...

# Brings up the reference Redis, runs the differential suite against it, and
# takes it back down. Without a reachable Redis the suite skips, which is easy
# to mistake for a pass.
conformance:
	docker compose up -d --wait
	REDIS_ADDR=127.0.0.1:6379 go test -count=1 ./internal/conformance/; status=$$?; \
		docker compose down; exit $$status

lint:
	@test -z "$$(gofmt -l .)" || (echo "gofmt found issues:"; gofmt -l .; exit 1)
	go vet ./...

# -run='^$$' keeps the test suite out of a benchmark run. Without it every
# package's tests execute first and their output is interleaved with the
# numbers, which makes the result awkward to record verbatim in
# docs/benchmarks.md.
bench:
	go test -bench=. -benchmem -run='^$$' ./...

# The end-to-end harness. Not part of `make test`: it takes minutes, and a
# benchmark that CI must pass is one that gets weakened to keep CI green.
bench-e2e:
	KV_BENCH=1 go test -count=1 -timeout 30m -v -run TestBenchEndToEnd ./internal/server/

bench-profile:
	KV_BENCH=1 KV_BENCH_PROFILE=cpu.prof go test -count=1 -timeout 30m -v -run TestBenchProfile ./internal/server/
	@echo "now: go tool pprof -top -nodecount=15 cpu.prof"

clean:
	rm -rf bin cpu.prof
