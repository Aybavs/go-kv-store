package server

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestMaxClientsRejectsWithError(t *testing.T) {
	addr, _ := startServer(t, func(c *Config) { c.MaxClients = 1 })

	first := dial(t, addr)
	first.send(t, "PING")
	if got := first.readReply(t); got != "+PONG" {
		t.Fatalf("first client got %q", got)
	}

	second, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer second.Close()

	second.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 128)
	n, err := second.Read(buf)
	if err != nil {
		t.Fatalf("expected a rejection reply: %v", err)
	}
	got := string(buf[:n])
	if want := "-ERR max number of clients reached\r\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if _, err := second.Read(buf); err == nil {
		t.Fatal("expected the rejected connection to be closed")
	}
}

func TestReadTimeoutClosesIdleConnection(t *testing.T) {
	addr, _ := startServer(t, func(c *Config) { c.ReadTimeout = 200 * time.Millisecond })

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the idle connection to be closed by the read timeout")
	}
}

func TestOversizedFrameClosesConnection(t *testing.T) {
	addr, _ := startServer(t, func(c *Config) {
		c.Limits.MaxBulkLength = 8
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$20\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("expected an error reply: %v", err)
	}
	if buf[0] != '-' {
		t.Fatalf("got %q, want an error reply", string(buf[:n]))
	}
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected connection to be closed after an oversized frame")
	}
}

// TestShutdownWithIdleClient asserts that an idle client does not block
// shutdown.
func TestShutdownWithIdleClient(t *testing.T) {
	addr, cancel := startServer(t, nil)
	c := dial(t, addr)
	c.send(t, "PING")
	if got := c.readReply(t); got != "+PONG" {
		t.Fatalf("got %q", got)
	}

	start := time.Now()
	cancel()

	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64)
	if _, err := c.conn.Read(buf); err == nil {
		t.Fatal("expected the connection to be closed by shutdown")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("idle client blocked shutdown for %v", elapsed)
	}
}

// Asserts what a client can observe: shortly after shutdown begins, mutations
// stop succeeding. Not that the next command after cancel() is refused —
// BeginDrain runs asynchronously, so one arriving in between is legitimately
// served. The exact invariant is pinned by TestBeginDrainUnderContention.
func TestMutationsStopBeingAcceptedAfterShutdown(t *testing.T) {
	addr, cancel := startServer(t, nil)
	c := dial(t, addr)

	c.send(t, "SET", "k", "v")
	if got := c.readReply(t); got != "+OK" {
		t.Fatalf("SET before shutdown = %q, want +OK", got)
	}

	cancel()

	deadline := time.Now().Add(3 * time.Second)
	for attempt := 0; ; attempt++ {
		if time.Now().After(deadline) {
			t.Fatal("mutations were still being accepted 3s after shutdown began")
		}

		key := fmt.Sprintf("k%d", attempt)
		frame := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(key), key)

		c.conn.SetWriteDeadline(time.Now().Add(time.Second))
		if _, err := c.conn.Write([]byte(frame)); err != nil {
			return // connection closed: mutations can no longer even be sent
		}

		c.conn.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 128)
		n, err := c.conn.Read(buf)
		if err != nil {
			return // closed without replying
		}
		if buf[0] == '-' {
			return // explicitly refused
		}
		if got := string(buf[:n]); got != "+OK\r\n" {
			t.Fatalf("unexpected reply during shutdown: %q", got)
		}
		// Still served: shutdown has not reached DRAINING yet. Retry.
	}
}
