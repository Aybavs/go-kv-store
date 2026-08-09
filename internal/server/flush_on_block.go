package server

import (
	"errors"
	"io"

	"github.com/aybavs/go-kv-store/internal/resp"
)

// Deferring a reply's flush is how a pipelined batch stops costing one write
// syscall per command. The hard part is knowing when deferring is safe.
//
// docs/architecture.md has recorded the wrong answer since v0.1: deferring on
// bufio.Reader.Buffered() deadlocks, because that tells you bytes are pending,
// not that a *complete* command is. The decoder blocks on a half-arrived frame
// while the client waits for a reply sitting in our writer, and neither moves.
//
// The mechanism here inverts the question instead of answering it. Rather than
// asking "may I defer this flush?", it flushes at the one moment when deferring
// would be wrong: immediately before the reader issues a blocking read. Nothing
// has to decide whether a command is complete, because the reader running out
// of buffered bytes is exactly the condition that matters, and it reports it by
// asking for more.
//
// The consequences fall out rather than needing care:
//
//   - A request/response client is unaffected. Its buffer is empty after every
//     command, so the hook fires before every read and the flush lands where it
//     lands today: one write per reply, no added latency.
//   - A pipelined batch costs one write. Commands are parsed out of the buffer
//     with no underlying read between them, so nothing flushes until the batch
//     is exhausted.
//   - Memory is already bounded. resp.Writer buffers 4 KiB and flushes when
//     full, so a client pipelining ten thousand commands cannot make the server
//     hold ten thousand replies.
//
// Rejected: a "is a complete command buffered?" predicate. It means a second,
// non-consuming implementation of frame parsing that has to agree with the real
// one forever, and two parsers over one wire format is a bug shape this project
// has already paid for once.
// The flusher is held as a concrete pointer rather than as a func value: this
// sits on the read path of every command, so the call it makes should be one
// the compiler can see through.
type flushBeforeRead struct {
	src io.Reader
	fl  *connFlusher
}

func (f *flushBeforeRead) Read(p []byte) (int, error) {
	if err := f.fl.flush(); err != nil {
		return 0, err
	}
	return f.src.Read(p)
}

// errFlushFailed is returned to the reader, so a failed flush surfaces as
// ReadCommand's third error class — a transport failure, closed without a
// reply. That is the correct handling: the connection is broken, and trying to
// explain that to the client means writing to the thing that just failed.
var errFlushFailed = errors.New("server: writing the pending reply failed")

// connFlusher owns the pending-reply flush for one connection.
type connFlusher struct {
	w      *resp.Writer
	failed bool
}

// flush sends whatever is buffered.
//
// It deliberately does not touch the write deadline. handleConn sets one
// immediately before encoding each reply and the deadline is absolute, so the
// one in force here was set microseconds ago by the most recent reply — and for
// a pipelined batch, by the last reply in it, which is the right budget for the
// write that sends them all. Setting another would cost a syscall to replace a
// deadline that has barely aged, on the path whose whole purpose is to make one
// syscall do the work of many.
//
// Once a flush has failed it stays failed. resp.Writer's contract is that a
// caller must never Flush after a write error, because the buffer holds a
// partial frame; retrying on the next read would emit it.
func (f *connFlusher) flush() error {
	if f.failed {
		return errFlushFailed
	}
	if f.w.Buffered() == 0 {
		return nil
	}
	if err := f.w.Flush(); err != nil {
		f.failed = true
		return errFlushFailed
	}
	return nil
}

// fail latches without flushing, for a reply that failed part-way through
// encoding. Those bytes are a truncated frame and must never reach the client.
func (f *connFlusher) fail() { f.failed = true }
