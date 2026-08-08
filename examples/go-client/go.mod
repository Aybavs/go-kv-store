// A module of its own, so the client library this example needs never becomes a
// dependency of the server. go-kv-store's binary contains no third-party Redis
// code, and keeping the example separate is what makes that easy to verify.
module github.com/aybavs/go-kv-store/examples/go-client

go 1.26

require github.com/gomodule/redigo v1.9.3
