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

	n, err = redis.Int(conn.Do("DEL", "language", "binary"))
	check(err, "DEL")
	fmt.Printf("DEL x2          -> %d removed\n", n)

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

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
