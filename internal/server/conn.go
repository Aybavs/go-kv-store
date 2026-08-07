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

	r := resp.NewReader(conn, s.cfg.Limits)
	w := resp.NewWriter(conn)

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
			s.reportReadError(conn, w, err)
			return
		}

		reply := s.reg.Dispatch(args)

		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		if err := writeReply(w, reply); err != nil {
			s.log.Debug("write failed", "err", err)
			return
		}
		// Each completed command response is flushed. Batching is deliberately
		// not attempted here; see spec §7.1.
		if err := w.Flush(); err != nil {
			s.log.Debug("flush failed", "err", err)
			return
		}
	}
}

// reportReadError writes a final error to the client where that is meaningful,
// then lets the caller close the connection.
func (s *Server) reportReadError(conn net.Conn, w *resp.Writer, err error) {
	var pe *resp.ProtocolError
	switch {
	case errors.Is(err, io.EOF):
		return // clean client disconnect
	case errors.As(err, &pe):
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		_ = w.WriteError("ERR Protocol error: " + pe.Msg)
		_ = w.Flush()
		s.log.Debug("protocol error, closing connection", "err", err)
	default:
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
