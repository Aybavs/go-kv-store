package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
)

// startServer boots a server on an ephemeral port and returns its address and
// a cancel func that shuts it down.
func startServer(t *testing.T, mutate func(*Config)) (string, context.CancelFunc) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	if mutate != nil {
		mutate(&cfg)
	}

	sup := NewSupervisor()
	eng := engine.New(sup.Fatal)
	reg := command.New(eng)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, eng, reg, sup, logger)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- srv.RunWithReady(ctx, ready) }()

	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("server exited before ready: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not become ready")
	}

	addr := srv.Addr().String()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not shut down")
		}
	})
	return addr, cancel
}

// client is a minimal raw RESP client for tests.
type client struct {
	conn net.Conn
	br   *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &client{conn: conn, br: bufio.NewReader(conn)}
}

func (c *client) send(t *testing.T, parts ...string) {
	t.Helper()
	var b []byte
	b = append(b, '*')
	b = append(b, []byte(itoa(len(parts)))...)
	b = append(b, '\r', '\n')
	for _, p := range parts {
		b = append(b, '$')
		b = append(b, []byte(itoa(len(p)))...)
		b = append(b, '\r', '\n')
		b = append(b, p...)
		b = append(b, '\r', '\n')
	}
	if _, err := c.conn.Write(b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// readReply returns one raw reply line, plus the payload for bulk strings.
func (c *client) readReply(t *testing.T) string {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := c.br.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	line = line[:len(line)-2] // strip CRLF
	if len(line) > 0 && line[0] == '$' && line != "$-1" {
		n := atoi(line[1:])
		payload := make([]byte, n+2)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return line + "\r\n" + string(payload[:n])
	}
	return line
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

func TestServerPing(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)
	c.send(t, "PING")
	if got := c.readReply(t); got != "+PONG" {
		t.Fatalf("got %q, want %q", got, "+PONG")
	}
}

func TestServerSetGetDelExists(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	c.send(t, "SET", "k", "v")
	if got := c.readReply(t); got != "+OK" {
		t.Fatalf("SET got %q", got)
	}
	c.send(t, "GET", "k")
	if got := c.readReply(t); got != "$1\r\nv" {
		t.Fatalf("GET got %q", got)
	}
	c.send(t, "EXISTS", "k")
	if got := c.readReply(t); got != ":1" {
		t.Fatalf("EXISTS got %q", got)
	}
	c.send(t, "DEL", "k")
	if got := c.readReply(t); got != ":1" {
		t.Fatalf("DEL got %q", got)
	}
	c.send(t, "GET", "k")
	if got := c.readReply(t); got != "$-1" {
		t.Fatalf("GET after DEL got %q", got)
	}
}

func TestServerUnknownCommandKeepsConnectionAlive(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	c.send(t, "BOGUS")
	got := c.readReply(t)
	if len(got) == 0 || got[0] != '-' {
		t.Fatalf("got %q, want an error reply", got)
	}
	c.send(t, "PING")
	if got := c.readReply(t); got != "+PONG" {
		t.Fatalf("connection unusable after error: %q", got)
	}
}

func TestServerMalformedFrameClosesConnection(t *testing.T) {
	addr, _ := startServer(t, nil)
	c := dial(t, addr)

	if _, err := c.conn.Write([]byte("+INLINE\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 256)
	// Expect an error reply followed by EOF.
	if _, err := c.conn.Read(buf); err != nil {
		t.Fatalf("expected an error reply before close: %v", err)
	}
	if _, err := c.conn.Read(buf); err == nil {
		t.Fatal("expected connection to be closed after a malformed frame")
	}
}

func TestServerConcurrentClients(t *testing.T) {
	addr, _ := startServer(t, nil)
	const clients = 16

	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		go func(i int) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			c := &client{conn: conn, br: bufio.NewReader(conn)}
			key := "k" + itoa(i)
			for j := 0; j < 50; j++ {
				c.send(t, "SET", key, "v")
				conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, err := c.br.ReadString('\n'); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < clients; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
	}
}

// newBoundServer builds a Server with a bound listener but no running accept
// loop, so a shutdown path can be driven directly instead of through Run's
// select. Driving it directly is what makes the shutdown assertions
// deterministic: Run picks a ready select case at random, which decides the path
// for reasons that have nothing to do with the behaviour under test.
func newBoundServer(t *testing.T) (*Server, *Supervisor, *bytes.Buffer) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"

	sup := NewSupervisor()
	eng := engine.New(sup.Fatal)
	reg := command.New(eng)

	var logs bytes.Buffer
	srv := New(cfg, eng, reg, sup, slog.New(slog.NewTextHandler(&logs, nil)))

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv.ln = ln
	t.Cleanup(func() { _ = ln.Close() })

	return srv, sup, &logs
}

// Regression test: a fatal delivered as a channel value could be received only
// once, so once ctx.Done() won the select, nothing read the report and the
// process exited 0 after an invariant violation.
//
// Driven directly rather than through Run, which depends on which ready select
// case Go happens to pick.
func TestGracefulShutdownSurfacesPendingFatal(t *testing.T) {
	srv, sup, logs := newBoundServer(t)
	fatal := errors.New("engine invariant violated during shutdown")

	sup.Fatal(fatal)

	err := srv.gracefulShutdown()
	if !errors.Is(err, fatal) {
		t.Fatalf("gracefulShutdown returned %v, want the pending fatal cause; a "+
			"reported fatal must not be swallowed by a drain", err)
	}
	if strings.Contains(logs.String(), "shutdown: complete") {
		t.Errorf("shutdown logged completion despite a pending fatal:\n%s", logs.String())
	}
}

// TestGracefulShutdownReturnsNilWhenNothingFailed is the counterpart: without a
// fatal, the same path must report success. Without this, the test above could
// be satisfied by a shutdown that always failed.
func TestGracefulShutdownReturnsNilWhenNothingFailed(t *testing.T) {
	srv, _, logs := newBoundServer(t)

	if err := srv.gracefulShutdown(); err != nil {
		t.Fatalf("gracefulShutdown returned %v, want nil for a clean drain", err)
	}
	if !strings.Contains(logs.String(), "shutdown: complete") {
		t.Errorf("clean shutdown did not log completion:\n%s", logs.String())
	}
}

// TestRunSurfacesFatalWhileServing covers the other entry into the fatal path:
// a condition reported while the server is simply running, with no shutdown
// signal involved.
func TestRunSurfacesFatalWhileServing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"

	sup := NewSupervisor()
	eng := engine.New(sup.Fatal)
	reg := command.New(eng)
	srv := New(cfg, eng, reg, sup, slog.New(slog.NewTextHandler(io.Discard, nil)))

	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- srv.RunWithReady(context.Background(), ready) }()
	<-ready

	fatal := errors.New("engine invariant violated while serving")
	sup.Fatal(fatal)

	select {
	case err := <-done:
		if !errors.Is(err, fatal) {
			t.Fatalf("Run returned %v, want the fatal cause", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after a fatal condition was reported")
	}
}
