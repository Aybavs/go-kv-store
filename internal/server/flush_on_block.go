package server

import (
	"errors"
	"io"

	"github.com/aybavs/go-kv-store/internal/resp"
)

// flushBeforeRead sends the pending reply immediately before the reader would
// block, which is the only moment at which holding it would be wrong. See
// ADR 0006 for why the obvious trigger, bufio.Reader.Buffered, deadlocks.
//
// The flusher is a concrete pointer, not a func value: this is on the read path
// of every command.
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

// errFlushFailed reaches the reader, so a failed flush surfaces as
// ReadCommand's transport-failure class: closed without a reply. Explaining the
// failure to the client would mean writing to the thing that just failed.
var errFlushFailed = errors.New("server: writing the pending reply failed")

// connFlusher owns the pending-reply flush for one connection.
type connFlusher struct {
	w      *resp.Writer
	failed bool
}

// flush sends whatever is buffered.
//
// It does not set the write deadline. serve sets one before encoding each
// reply, and a deadline is absolute, so the one in force here is microseconds
// old — for a batch, the one set by its last reply.
//
// A failed flush latches: resp.Writer must never be flushed after a write
// error, because the buffer holds a partial frame.
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

// fail latches without flushing: those bytes are a truncated frame.
func (f *connFlusher) fail() { f.failed = true }
