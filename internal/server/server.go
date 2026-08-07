// Package server owns the listener, the connection set and the shutdown state
// machine. It never touches the store directly.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/resp"
)

// ErrShutdownTimeout is returned when clients did not drain within
// Config.ShutdownTimeout.
var ErrShutdownTimeout = errors.New("shutdown timed out while draining clients")

type Config struct {
	Addr            string
	MaxClients      int
	ReadTimeout     time.Duration // 0 disables the idle read deadline
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	Limits          resp.Limits
}

func DefaultConfig() Config {
	return Config{
		Addr:            "127.0.0.1:6380",
		MaxClients:      1024,
		ReadTimeout:     0,
		WriteTimeout:    30 * time.Second,
		ShutdownTimeout: 10 * time.Second,
		Limits:          resp.DefaultLimits(),
	}
}

type Server struct {
	cfg Config
	eng *engine.Engine
	reg *command.Registry
	sup *Supervisor
	log *slog.Logger

	ln net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	nClient int

	draining chan struct{}
	wg       sync.WaitGroup
}

func New(cfg Config, eng *engine.Engine, reg *command.Registry, sup *Supervisor, log *slog.Logger) *Server {
	return &Server{
		cfg:      cfg,
		eng:      eng,
		reg:      reg,
		sup:      sup,
		log:      log,
		conns:    make(map[net.Conn]struct{}),
		draining: make(chan struct{}),
	}
}

func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Run listens and serves until ctx is cancelled or a fatal condition is
// reported.
func (s *Server) Run(ctx context.Context) error {
	return s.RunWithReady(ctx, nil)
}

// RunWithReady is Run with a channel closed once the listener is bound. Tests
// use it to avoid racing on Addr().
func (s *Server) RunWithReady(ctx context.Context, ready chan<- struct{}) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.log.Info("listening", "addr", ln.Addr().String())
	if ready != nil {
		close(ready)
	}

	go s.acceptLoop()

	select {
	case <-ctx.Done():
		return s.gracefulShutdown()
	case err := <-s.sup.C():
		return s.fatalShutdown(err)
	}
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		if !s.admit(conn) {
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// admit enforces MaxClients. Over the limit the client gets an explicit error
// rather than an unexplained hang in the accept backlog.
func (s *Server) admit(conn net.Conn) bool {
	s.mu.Lock()
	if s.nClient >= s.cfg.MaxClients {
		s.mu.Unlock()
		w := resp.NewWriter(conn)
		_ = w.WriteError("ERR max number of clients reached")
		_ = w.Flush()
		_ = conn.Close()
		s.log.Debug("rejected connection: max clients reached")
		return false
	}
	s.nClient++
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	return true
}

func (s *Server) releaseConn(conn net.Conn) {
	s.mu.Lock()
	if _, ok := s.conns[conn]; ok {
		delete(s.conns, conn)
		s.nClient--
	}
	s.mu.Unlock()
	_ = conn.Close()
}

// gracefulShutdown implements RUNNING -> DRAINING -> STOPPED. v0.1 has no
// persistence, so the finalisation stage is a no-op; v0.3 adds it here.
func (s *Server) gracefulShutdown() error {
	s.log.Info("shutdown: draining")
	_ = s.ln.Close()
	s.eng.BeginDrain()
	close(s.draining)

	// Unblock handlers parked in a socket read so they observe s.draining.
	s.mu.Lock()
	for c := range s.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	if s.waitConns(s.cfg.ShutdownTimeout) {
		s.log.Info("shutdown: complete")
		return nil
	}

	s.log.Warn("shutdown: timeout, closing remaining connections")
	s.closeAllConns()
	s.waitConns(2 * time.Second)
	return ErrShutdownTimeout
}

// fatalShutdown is a distinct path: there is no drain guarantee and no
// durability claim. See spec §7.5.
func (s *Server) fatalShutdown(cause error) error {
	s.log.Error("fatal condition, shutting down", "err", cause)
	_ = s.ln.Close()
	s.eng.BeginDrain()
	close(s.draining)
	s.closeAllConns()
	s.waitConns(2 * time.Second)
	return cause
}

func (s *Server) closeAllConns() {
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
}

func (s *Server) waitConns(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func (s *Server) isDraining() bool {
	select {
	case <-s.draining:
		return true
	default:
		return false
	}
}
