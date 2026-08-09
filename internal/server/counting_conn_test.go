package server

import (
	"net"
	"sync/atomic"
)

// The v0.5 target named in ROADMAP.md is syscalls per request, so that is the
// number this measures — directly, rather than inferring it from throughput.
//
// Throughput is a proxy for the syscall count and a noisy one: v0.4 measured a
// run-to-run spread of up to 9% end to end on this machine. A count of Read and
// Write calls is exact, deterministic, and unaffected by what else the machine
// is doing, which makes it the only figure here that can carry a claim on its
// own.
//
// net.Conn is an interface, so counting needs no production code at all: a test
// in this package wraps the listener and assigns s.ln directly.
type connCounter struct {
	reads        atomic.Int64
	writes       atomic.Int64
	bytesRead    atomic.Int64
	bytesWritten atomic.Int64
}

func (c *connCounter) reset() {
	c.reads.Store(0)
	c.writes.Store(0)
	c.bytesRead.Store(0)
	c.bytesWritten.Store(0)
}

type countingListener struct {
	net.Listener
	counter *connCounter
}

func (l countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, counter: l.counter}, nil
}

// countingConn counts the calls, not the bytes moved, because a call is what
// costs a syscall. Bytes are recorded alongside so a change that trades many
// small writes for one large one is visible as both.
type countingConn struct {
	net.Conn
	counter *connCounter
}

// The call is counted before it is made, not after, and that ordering is
// load-bearing rather than stylistic.
//
// Counting afterwards leaves a window in which the client has the reply but the
// counter does not yet know about the write that delivered it. A test that
// reads the counter as soon as its last reply arrives can therefore observe one
// write fewer than actually happened. That is not hypothetical: it is what CI
// on Linux reported — 63 writes for a batch of 64 — while macOS passed
// consistently, because there the server usually finished the whole batch
// before the client's first read returned, so the window never opened.
//
// Counting first closes it by construction: bytes cannot reach the client
// before the write that carries them has been counted.
func (c *countingConn) Read(p []byte) (int, error) {
	c.counter.reads.Add(1)
	n, err := c.Conn.Read(p)
	c.counter.bytesRead.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.counter.writes.Add(1)
	n, err := c.Conn.Write(p)
	c.counter.bytesWritten.Add(int64(n))
	return n, err
}
