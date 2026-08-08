BINARY := kv-server

.PHONY: build run test test-race lint bench clean

build:
	go build -o bin/$(BINARY) ./cmd/kv-server

run: build
	./bin/$(BINARY)

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	@test -z "$$(gofmt -l .)" || (echo "gofmt found issues:"; gofmt -l .; exit 1)
	go vet ./...

# -run='^$$' keeps the test suite out of a benchmark run. Without it every
# package's tests execute first and their output is interleaved with the
# numbers, which makes the result awkward to record verbatim in
# docs/benchmarks.md.
bench:
	go test -bench=. -benchmem -run='^$$' ./...

clean:
	rm -rf bin
