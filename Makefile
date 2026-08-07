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

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin
