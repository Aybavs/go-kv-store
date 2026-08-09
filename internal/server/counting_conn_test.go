package server

import (
	"net"
	"sync/atomic"
)

// connCounter counts syscalls directly rather than inferring them from
// throughput, which on this machine varies by up to 9% between runs. A count of
// Read and Write calls is exact and is the only figure here that carries a claim
// on its own.
//
// net.Conn is an interface, so this needs no production code: a test wraps the
// listener and assigns s.ln.
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

// Counted before the call, not after: counting afterwards leaves a window where
// the client has the reply and the counter does not yet know about the write
// that delivered it, so a test reading the counter as its last reply arrives
// sees one write too few. Linux CI reported 63 for a batch of 64.
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
