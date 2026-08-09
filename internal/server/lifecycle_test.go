package server

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/resp"
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

// TestPendingReplyIsDeliveredWhenDrainingBegins guards the exit path that the
// deferred flush created.
//
// serve returns from the top of its loop when the server is draining, before
// any read — so nothing triggers the flush that a blocking read would. A reply
// already encoded at that moment sits in the writer and, without serveConn's
// final flush, is never sent: the client sees the connection close with no
// answer to a command the server had already executed.
//
// The situation is constructed rather than waited for. Reaching it through a
// live server would mean the drain landing between the reply being encoded and
// the loop coming round, which is a window no test can schedule — and this
// project has been bitten four times by tests that asserted a scheduling
// outcome instead of building one.
func TestPendingReplyIsDeliveredWhenDrainingBegins(t *testing.T) {
	cli, srv := net.Pipe()
	defer cli.Close()

	s := New(DefaultConfig(), engine.New(func(error) {}), nil, NewSupervisor(),
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	w := resp.NewWriter(srv)
	fl := &connFlusher{w: w}
	// Encoded but not flushed: exactly the state serve is in when it has just
	// answered a command and is about to look at the drain flag.
	if err := writeReply(w, command.Simple("OK")); err != nil {
		t.Fatalf("writeReply: %v", err)
	}
	if w.Buffered() == 0 {
		t.Fatal("the reply was not left buffered; the test is not in the state it means to test")
	}

	close(s.draining)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r := resp.NewReader(&flushBeforeRead{src: srv, fl: fl}, DefaultConfig().Limits)
		s.serveConn(srv, r, w, fl)
		srv.Close()
	}()

	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 5)
	n, err := io.ReadFull(cli, got)
	if err != nil {
		t.Fatalf("the pending reply never arrived (%d bytes, %v); it was dropped when draining began", n, err)
	}
	if string(got) != "+OK\r\n" {
		t.Fatalf("got %q, want %q", got, "+OK\r\n")
	}
	<-done
}

// TestRequestResponseClientIsNotWaitedOn is the deadlock test.
//
// Deferring a flush is only safe if it happens before the reader blocks. If the
// trigger were ever moved to something that waits for more input — bytes
// buffered, a command count, a timer — this hangs, because the client will not
// send anything else until it has its reply.
func TestRequestResponseClientIsNotWaitedOn(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	for i := 0; i < 3; i++ {
		c.send(t, "PING")
		if got := c.readReply(t); got != "+PONG" {
			t.Fatalf("reply %d = %q; a reply withheld pending more input would hang here", i, got)
		}
	}
}

// A command split across two reads still gets its reply, and the reply is not
// held back waiting for whatever the client sends next.
func TestSplitCommandStillGetsItsReply(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	frame := "*2\r\n$4\r\nECHO\r\n"                        // deliberately not a command we support
	if _, err := c.conn.Write([]byte(frame)); err != nil { // first half
		t.Fatalf("write: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // force a second read on the server side
	if _, err := c.conn.Write([]byte("$2\r\nhi\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := c.readReply(t)
	if !strings.HasPrefix(got, "-ERR unknown command") {
		t.Fatalf("got %q, want an unknown-command error", got)
	}
}
