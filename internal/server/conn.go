package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"time"

	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/resp"
)

func (s *Server) handleConn(conn net.Conn) {
	defer s.releaseConn(conn)
	defer s.recoverConn(conn)

	w := resp.NewWriter(conn)
	fl := &connFlusher{w: w}

	var src io.Reader = conn
	if !s.cfg.flushEveryReply {
		src = &flushBeforeRead{src: conn, fl: fl}
	}
	r := resp.NewReader(src, s.cfg.Limits)

	s.serveConn(conn, r, w, fl)
}

// serveConn runs the command loop and flushes whatever it left behind. A call
// rather than a defer, so the flush is reached on every ordinary exit and none
// of the panicking ones.
//
// serve returns from the top of its loop when draining, before any read, so
// nothing triggers the deferred flush and an encoded reply would be dropped. A
// panic must not reach it: emitting a half-written reply is worse than none.
func (s *Server) serveConn(conn net.Conn, r *resp.Reader, w *resp.Writer, fl *connFlusher) {
	s.serve(conn, r, w, fl)
	_ = fl.flush()
}

func (s *Server) serve(conn net.Conn, r *resp.Reader, w *resp.Writer, fl *connFlusher) {
	for {
		// Draining means "finish work already begun", not "consume whatever
		// the client had already buffered".
		if s.isDraining() {
			return
		}
		if s.cfg.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		} else {
			_ = conn.SetReadDeadline(time.Time{})
		}

		args, err := r.ReadCommand()
		if err != nil {
			s.reportReadError(conn, w, fl, err)
			return
		}

		reply := s.reg.Dispatch(args)

		// Set per reply, not per flush. The deadline is absolute, so the one
		// established here still governs the flush that follows a few
		// microseconds later — including the deferred one, and including a
		// flush bufio performs on its own when the buffer fills mid-reply.
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		if err := writeReply(w, reply); err != nil {
			// Those bytes are a truncated frame; they must not be sent.
			fl.fail()
			s.log.Debug("write failed", "err", err)
			return
		}
		if s.cfg.flushEveryReply {
			if err := fl.flush(); err != nil {
				s.log.Debug("flush failed", "err", err)
				return
			}
		}
	}
}

// reportReadError writes a final error to the client where that is meaningful,
// then lets the caller close the connection.
func (s *Server) reportReadError(conn net.Conn, w *resp.Writer, fl *connFlusher, err error) {
	var pe *resp.ProtocolError
	switch {
	case errors.Is(err, io.EOF):
		return // clean client disconnect
	case errors.As(err, &pe):
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		_ = w.WriteError("ERR Protocol error: " + pe.Msg)
		// Through the flusher, so a connection whose writes have already failed
		// is not written to again.
		_ = fl.flush()
		s.log.Debug("protocol error, closing connection", "err", err)
	default:
		// Includes errFlushFailed, raised by the deferred flush and surfaced by
		// the reader as a transport failure. Nothing is written back: the write
		// side is the thing that just broke.
		s.log.Debug("read failed", "err", err)
	}
}

// recoverConn handles a panic raised below the engine boundary. An
// engine-fatal panic has already been reported to the supervisor, so it must
// never be swallowed here.
func (s *Server) recoverConn(conn net.Conn) {
	r := recover()
	if r == nil {
		return
	}
	if s.sup.Fired() {
		s.log.Error("panic on the fatal path, connection abandoned", "panic", r)
		return
	}
	s.log.Error("connection panic recovered",
		"panic", r,
		"remote", conn.RemoteAddr().String(),
		"stack", string(debug.Stack()))
}

// writeReply encodes a command.Reply as RESP. This is the only place where the
// two representations meet.
func writeReply(w *resp.Writer, reply command.Reply) error {
	switch reply.Kind {
	case command.ReplySimple:
		return w.WriteSimpleString(reply.Str)
	case command.ReplyError:
		return w.WriteError(reply.Str)
	case command.ReplyInt:
		return w.WriteInt(reply.Int)
	case command.ReplyBulk:
		return w.WriteBulk(reply.Str)
	case command.ReplyNullBulk:
		return w.WriteNullBulk()
	case command.ReplyArray:
		if err := w.WriteArrayHeader(len(reply.Array)); err != nil {
			return err
		}
		for _, item := range reply.Array {
			if err := writeReply(w, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown reply kind %d", reply.Kind)
	}
}
