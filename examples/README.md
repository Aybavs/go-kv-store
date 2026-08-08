# Examples

## `go-client`

Exercises every command the server implements, using
[redigo](https://github.com/gomodule/redigo) — an ordinary RESP2 client library
with no knowledge of this project. That is the point: go-kv-store speaks a
dialect real clients already know, so it needs no tooling of its own.

Start the server, then run the example:

    go build -o bin/kv-server ./cmd/kv-server
    ./bin/kv-server &

    cd examples/go-client && go run .

`go run` rather than `go build`: building inside the module leaves the binary
next to the source.

It is a separate Go module. The server binary contains no third-party Redis
code, and keeping the example's dependency out of the main module is what keeps
that easy to verify:

    go list -deps ./cmd/kv-server | grep -c redigo   # 0

`redis-cli` works too, and needs no code at all:

    redis-cli -p 6380 SET language go
    redis-cli -p 6380 GET language
