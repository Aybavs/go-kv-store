// Command go-client exercises every command go-kv-store implements, using an
// ordinary RESP2 client library rather than anything written for this project.
// That is the point of the example: the server speaks a dialect real clients
// already know, so it needs no bespoke tooling.
//
//	go run ./examples/go-client -addr 127.0.0.1:6380
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/gomodule/redigo/redis"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	flag.Parse()

	conn, err := redis.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial %s: %v (is kv-server running?)", *addr, err)
	}
	defer conn.Close()

	pong, err := redis.String(conn.Do("PING"))
	check(err, "PING")
	fmt.Printf("PING            -> %s\n", pong)

	echo, err := redis.String(conn.Do("PING", "hello"))
	check(err, "PING hello")
	fmt.Printf("PING hello      -> %q\n", echo)

	_, err = redis.String(conn.Do("SET", "language", "go"))
	check(err, "SET")
	fmt.Printf("SET language go -> OK\n")

	value, err := redis.String(conn.Do("GET", "language"))
	check(err, "GET")
	fmt.Printf("GET language    -> %q\n", value)

	// Keys and values are binary-safe: any byte is allowed, CRLF and NUL
	// included. A length-prefixed protocol is what makes that true.
	binary := "a\x00b\r\nc"
	_, err = redis.String(conn.Do("SET", "binary", binary))
	check(err, "SET binary")
	back, err := redis.String(conn.Do("GET", "binary"))
	check(err, "GET binary")
	fmt.Printf("binary value    -> round-tripped intact: %t\n", back == binary)

	// A missing key is a null bulk string, which redigo surfaces as ErrNil —
	// distinct from an empty stored value.
	if _, err := redis.String(conn.Do("GET", "absent")); err != redis.ErrNil {
		log.Fatalf("GET on a missing key returned %v, want redis.ErrNil", err)
	}
	fmt.Printf("GET absent      -> nil\n")

	n, err := redis.Int(conn.Do("EXISTS", "language", "binary", "absent"))
	check(err, "EXISTS")
	fmt.Printf("EXISTS x3       -> %d present\n", n)

	// MGET answers one element per key in request order, with a null bulk
	// string in place where a key is absent — not a shorter array.
	values, err := redis.Values(conn.Do("MGET", "language", "absent", "binary"))
	check(err, "MGET")
	fmt.Printf("MGET x3         -> %s\n", describe(values))

	// A counter starts from zero, so INCR on a fresh key is 1.
	c, err := redis.Int64(conn.Do("INCR", "counter"))
	check(err, "INCR")
	d, err := redis.Int64(conn.Do("DECR", "counter"))
	check(err, "DECR")
	fmt.Printf("INCR then DECR  -> %d then %d\n", c, d)

	// An expiry survives a read-modify-write: this is the property the
	// append-only file records as SET key <result> PXAT <the same deadline>.
	_, err = redis.String(conn.Do("SET", "sessions", "1", "EX", "100"))
	check(err, "SET EX")
	if _, err := redis.Int64(conn.Do("INCR", "sessions")); err != nil {
		log.Fatalf("INCR on a key with a TTL: %v", err)
	}
	ttl, err := redis.Int64(conn.Do("TTL", "sessions"))
	check(err, "TTL")
	fmt.Printf("TTL after INCR  -> %d seconds, the deadline it already had\n", ttl)

	removed, err := redis.Int64(conn.Do("PERSIST", "sessions"))
	check(err, "PERSIST")
	after, err := redis.Int64(conn.Do("TTL", "sessions"))
	check(err, "TTL")
	fmt.Printf("PERSIST         -> %d, and TTL is now %d\n", removed, after)

	// EXPIRE with a non-positive value deletes the key and reports whether it
	// was there, which is Redis's behaviour rather than an error.
	gone, err := redis.Int64(conn.Do("EXPIRE", "sessions", "0"))
	check(err, "EXPIRE 0")
	fmt.Printf("EXPIRE k 0      -> %d, the key is deleted\n", gone)

	n, err = redis.Int(conn.Do("DEL", "language", "binary", "counter"))
	check(err, "DEL")
	fmt.Printf("DEL x3          -> %d removed\n", n)

	// Errors are values a client can act on, not transport failures: the
	// connection stays open and usable afterwards.
	if _, err := conn.Do("TOTALLYBOGUS"); err != nil {
		fmt.Printf("unknown command -> %v\n", err)
	}
	if _, err := redis.String(conn.Do("PING")); err != nil {
		log.Fatalf("connection unusable after a command error: %v", err)
	}
	fmt.Printf("PING            -> connection still healthy\n")
}

// describe renders an MGET reply so the null element is visible rather than
// printed as an empty string next to a genuinely empty value.
func describe(values []interface{}) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		if v == nil {
			parts = append(parts, "nil")
			continue
		}
		parts = append(parts, fmt.Sprintf("%q", v.([]byte)))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
