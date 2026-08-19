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

	// flushEveryReply restores the pre-v0.5 behaviour of one write syscall per
	// reply. Unexported and false by default, so it does not exist as far as
	// any caller outside this package is concerned.
	//
	// It is here for the measurement rather than for configuration. Comparing
	// the two behaviours across two commits would mean two runs minutes apart
	// on a machine whose end-to-end spread v0.4 measured at up to 9%; with the
	// switch, the harness interleaves them inside one process, which is the
	// only way the difference means anything.
	flushEveryReply bool
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

	// Reclamation runs for as long as the server serves. It is not what makes
	// expired keys disappear — the read path does that — so nothing depends on
	// it having started.
	s.eng.StartExpiration()

	go s.acceptLoop()

	select {
	case <-ctx.Done():
		return s.gracefulShutdown()
	case <-s.sup.Done():
		return s.fatalShutdown(s.sup.Cause())
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
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
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
	clearScanSessions := false
	s.mu.Lock()
	if _, ok := s.conns[conn]; ok {
		delete(s.conns, conn)
		s.nClient--
		clearScanSessions = s.nClient == 0 && s.isDraining()
	}
	s.mu.Unlock()
	// A handler can outlive bounded shutdown and create a session after the
	// shutdown path's immediate clear. The last real handler closes that gap,
	// without ever overlapping the server mutex and the session-manager lock.
	if clearScanSessions {
		s.eng.ClearScanSessions()
	}
	_ = conn.Close()
}

// gracefulShutdown implements RUNNING -> DRAINING -> STOPPED. v0.1 has no
// persistence, so the finalisation stage is a no-op; v0.3 adds it here.
func (s *Server) gracefulShutdown() error {
	defer s.eng.ClearScanSessions()
	s.log.Info("shutdown: draining")
	_ = s.ln.Close()

	// Before the gate, deliberately. The worker takes the write lock and is not
	// a client mutation, so refusing it through the admission gate would
	// conflate reclamation with a request; stopping it first means the question
	// never arises.
	stopCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	if err := s.eng.StopExpiration(stopCtx); err != nil {
		s.log.Warn("expiration worker did not stop in time", "err", err)
	}
	cancel()

	s.eng.BeginDrain()
	close(s.draining)

	// Unblock handlers parked in a socket read so they observe s.draining.
	s.mu.Lock()
	for c := range s.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	drained := s.waitConns(s.cfg.ShutdownTimeout)

	// Persistence finalisation, between draining and stopped. Mutations have
	// already stopped being admitted, so whatever is still buffered is all
	// there will ever be — and a clean shutdown is the point where "written"
	// stops being good enough.
	//
	// A failure here is reported to the supervisor rather than returned
	// directly, so it goes through the same fatal path as any other durability
	// failure and the exit code says the same thing.
	finCtx, finCancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	if err := s.eng.Finalize(finCtx); err != nil {
		s.log.Error("shutdown: persistence did not finalise cleanly", "err", err)
		s.sup.Fatal(err)
	}
	finCancel()

	// The select has committed to this branch, so this is the last place a fatal
	// can be observed, and checking here is what stops the process exiting 0
	// after an invariant violation. Scoped to what is knowable: one reported
	// after the outcome is decided is not surfaced, and must not be.
	if s.sup.Fired() {
		cause := s.sup.Cause()
		s.log.Error("fatal condition reported during shutdown", "err", cause)
		s.closeAllConns()
		s.waitConns(2 * time.Second)
		return cause
	}

	if drained {
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
	defer s.eng.ClearScanSessions()
	// Best effort: the process is going down either way, and a worker still
	// holding the lock must not delay reporting why.
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	_ = s.eng.StopExpiration(stopCtx)
	cancel()

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

// waitConns waits for connection goroutines to finish. It reports whether they
// all finished, and returns early if a fatal condition is reported — there is no
// point draining politely once the server's own invariants are untrustworthy.
func (s *Server) waitConns(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-s.sup.Done():
		return false
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
