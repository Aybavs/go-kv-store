package server

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/resp"
)

// The six shutdown behaviours the design spec enumerates and the suite did not
// cover. Two are error paths nothing had executed: ErrShutdownTimeout, and the
// branch where final persistence fails. Both decide the exit code.
//
// Each situation is constructed rather than waited for; reaching most of them
// through a live server needs a signal landing in a specific microsecond.

// blockingConn is a connection whose handler is busy rather than parked in a
// socket read: a command in flight, or a write to a client that is not reading.
// Deadlines do not move it, which is the point — the drain's "wake everyone up"
// step sets a read deadline, and that must not be what these tests rely on.
// Close is the only thing that unblocks it, so closeAllConns is exercised for
// real rather than assumed.
type blockingConn struct {
	net.Conn
	released chan struct{}
	once     sync.Once
}

func newBlockingConn() *blockingConn {
	// A real pipe underneath, so Addr and the rest behave; only the blocking is
	// synthetic.
	_, srv := net.Pipe()
	return &blockingConn{Conn: srv, released: make(chan struct{})}
}

func (c *blockingConn) Read(p []byte) (int, error) {
	<-c.released
	return 0, io.EOF
}

func (c *blockingConn) Write(p []byte) (int, error) {
	<-c.released
	return 0, errors.New("connection closed during shutdown")
}

func (c *blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *blockingConn) SetWriteDeadline(time.Time) error { return nil }

func (c *blockingConn) Close() error {
	c.once.Do(func() { close(c.released) })
	return c.Conn.Close()
}

// register attaches a fake handler to the server: a connection in the set and a
// goroutine in the wait group, which is exactly what a real one contributes to
// shutdown. body decides when that handler finishes.
func register(s *Server, conn net.Conn, body func()) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.nClient++
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		body()
	}()
}

// Spec §10.2 (2): a command already executing when the signal arrives is
// allowed to finish. Shutdown waits for it rather than cutting it off, and only
// reports a clean drain once it has.
func TestShutdownWaitsForACommandAlreadyExecuting(t *testing.T) {
	srv, _, logs := newBoundServer(t)
	srv.cfg.ShutdownTimeout = 5 * time.Second

	const work = 150 * time.Millisecond
	finished := make(chan struct{})
	conn := newBlockingConn()
	register(srv, conn, func() {
		// Stands in for a command mid-dispatch: not parked in a read, so the
		// drain's read deadline does not reach it.
		time.Sleep(work)
		close(finished)
		_ = conn.Close()
	})

	start := time.Now()
	err := srv.gracefulShutdown()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("gracefulShutdown = %v, want nil: the handler finished inside the budget", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("shutdown returned before the in-flight command finished")
	}
	if elapsed < work {
		t.Fatalf("shutdown took %v, less than the %v the handler needed; it did not wait", elapsed, work)
	}
	if !strings.Contains(logs.String(), "shutdown: complete") {
		t.Errorf("a drain that waited successfully did not log completion:\n%s", logs.String())
	}
}

// Spec §10.2 (3): of several pipelined commands, only the one already started
// may finish. The rest are not consumed, even though their bytes are sitting in
// the reader's buffer.
//
// The drain is triggered by the read itself, so the sequence is exact rather
// than timed: the reader hands over both commands in one Read and closes the
// draining channel as it does. serve therefore answers the first and finds the
// gate closed before it can look at the second.
func TestDrainStopsAtTheCommandAlreadyStarted(t *testing.T) {
	srv, _, _ := newBoundServer(t)

	two := "*1\r\n$4\r\nPING\r\n" + "*1\r\n$4\r\nPING\r\n"
	cli, conn := net.Pipe()
	defer cli.Close()

	src := &drainOnRead{data: []byte(two), draining: srv.draining}
	w := resp.NewWriter(conn)
	fl := &connFlusher{w: w}
	r := resp.NewReader(&flushBeforeRead{src: src, fl: fl}, DefaultConfig().Limits)

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.serveConn(conn, r, w, fl)
		_ = conn.Close()
	}()

	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := io.ReadAll(cli)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("reading replies: %v", err)
	}
	<-done

	if string(got) != "+PONG\r\n" {
		t.Fatalf("replies = %q, want exactly one +PONG: the second pipelined command "+
			"was served after draining began", got)
	}
	if src.reads != 1 {
		t.Errorf("the reader was called %d times, want 1; serve went back for more input after draining", src.reads)
	}
}

// drainOnRead delivers its payload in one Read and closes the draining channel
// at the same moment, so the drain lands exactly between two buffered commands.
type drainOnRead struct {
	data     []byte
	draining chan struct{}
	reads    int
	closed   bool
}

func (d *drainOnRead) Read(p []byte) (int, error) {
	d.reads++
	if len(d.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, d.data)
	d.data = d.data[n:]
	if !d.closed {
		d.closed = true
		close(d.draining)
	}
	return n, nil
}

// Spec §10.2 (4): a client that is not reading cannot hold the server open. Its
// handler is stuck in a write that will never complete, so the drain cannot
// finish politely — the budget expires, the connection is closed, and shutdown
// reports the timeout rather than hanging.
func TestNonReadingClientCannotHoldShutdownOpen(t *testing.T) {
	srv, _, logs := newBoundServer(t)
	srv.cfg.ShutdownTimeout = 100 * time.Millisecond

	conn := newBlockingConn()
	register(srv, conn, func() {
		// Blocks until Close, which is what a write to a client that never
		// reads amounts to once its socket buffer is full.
		_, _ = conn.Write([]byte("+OK\r\n"))
	})

	start := time.Now()
	err := srv.gracefulShutdown()
	elapsed := time.Since(start)

	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("gracefulShutdown = %v, want ErrShutdownTimeout", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("shutdown took %v; a stuck writer delayed it far past the budget", elapsed)
	}
	if !strings.Contains(logs.String(), "closing remaining connections") {
		t.Errorf("the timeout path did not close the remaining connections:\n%s", logs.String())
	}
}

// Spec §10.2 (5): the timeout itself. ErrShutdownTimeout is a documented return
// value that nothing exercised before this test, so the branch that produces it
// had never run.
func TestShutdownTimeoutIsReported(t *testing.T) {
	srv, _, _ := newBoundServer(t)
	srv.cfg.ShutdownTimeout = 50 * time.Millisecond

	conn := newBlockingConn()
	register(srv, conn, func() {
		buf := make([]byte, 1)
		_, _ = conn.Read(buf) // returns only when Close releases it
	})

	err := srv.gracefulShutdown()
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("gracefulShutdown = %v, want ErrShutdownTimeout", err)
	}

	// main turns a non-nil return into a non-zero exit, so assert the value
	// itself and not errors.Is(sentinel, nil), which is always false.
	if err == nil {
		t.Fatal("a shutdown that could not drain returned nil, which exits 0")
	}
}

// recordingFile is a log device that remembers what it was asked to do.
type recordingFile struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	syncs  int
	closed bool
}

func (f *recordingFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}

func (f *recordingFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return nil
}

func (f *recordingFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *recordingFile) stats() (bytes int, syncs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Len(), f.syncs
}

// syncFailFile accepts every byte and then refuses to make them durable, which
// is the failure the finalisation stage exists to catch.
type syncFailFile struct{ err error }

func (f syncFailFile) Write(p []byte) (int, error) { return len(p), nil }
func (f syncFailFile) Sync() error                 { return f.err }
func (f syncFailFile) Close() error                { return nil }

// slowSyncFile does not fail, it simply never finishes in time. That is a
// different shutdown outcome from a device that reports an error, and the
// difference is what makes it worth having: the log never enters a failed state,
// so the supervisor has nothing to say, and the only thing standing between a
// half-synced file and an exit code of zero is gracefulShutdown reporting the
// finalisation failure itself.
type slowSyncFile struct{ release chan struct{} }

func (f slowSyncFile) Write(p []byte) (int, error) { return len(p), nil }
func (f slowSyncFile) Sync() error                 { <-f.release; return nil }
func (f slowSyncFile) Close() error                { return nil }

// newLoggedServer is newBoundServer with persistence attached, so the
// finalisation stage of shutdown has something to finalise.
func newLoggedServer(t *testing.T, f aof.File, p aof.Policy) (*Server, *Supervisor, *bytes.Buffer) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"

	sup := NewSupervisor()
	eng := engine.New(sup.Fatal)
	eng.AttachLog(aof.Open(f, p, sup.Fatal), p)
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

// Spec §10.2 (6): under everysec a write is acknowledged once it reaches the
// operating system, so at the moment shutdown begins there may be data that has
// never been synced. The finalisation stage between draining and stopped is
// what turns that into a durable file, and this asserts it through the server's
// own shutdown path rather than through the aof package alone.
func TestShutdownSyncsWritesPendingUnderEverysec(t *testing.T) {
	f := &recordingFile{}
	srv, _, logs := newLoggedServer(t, f, aof.EverySec)

	if err := srv.eng.Set("k", "v", engine.NoExpiry()); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := srv.gracefulShutdown(); err != nil {
		t.Fatalf("gracefulShutdown = %v, want nil", err)
	}

	written, syncs := f.stats()
	if written == 0 {
		t.Fatal("nothing reached the file; the record was lost rather than finalised")
	}
	if syncs == 0 {
		t.Fatal("shutdown completed without a single Sync; everysec data was left unsynced")
	}
	if !strings.Contains(logs.String(), "shutdown: complete") {
		t.Errorf("clean shutdown did not log completion:\n%s", logs.String())
	}
}

// Spec §10.2 (7): if that final Sync fails, the process must not exit as though
// nothing happened.
//
// This covers the property. It does not, on its own, pin the branch that
// delivers it: a device that reports an error also fails the log's own writes,
// so the supervisor learns about it by a second route and the shutdown would
// come back non-zero even if the finalisation branch did nothing. Verified by
// mutation, which is why the test below exists as well.
func TestFinalSyncFailureProducesANonZeroShutdown(t *testing.T) {
	boom := errors.New("device refused to sync")
	srv, _, logs := newLoggedServer(t, syncFailFile{err: boom}, aof.EverySec)

	if err := srv.eng.Set("k", "v", engine.NoExpiry()); err != nil {
		// Under everysec the write is acknowledged, so this must still succeed;
		// the failure appears at Sync time.
		t.Fatalf("Set: %v", err)
	}

	err := srv.gracefulShutdown()
	if err == nil {
		t.Fatal("gracefulShutdown returned nil after the final Sync failed; " +
			"the process would exit 0 with data that was never made durable")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("gracefulShutdown = %v, want the sync failure as its cause", err)
	}
	if !strings.Contains(logs.String(), "persistence did not finalise cleanly") {
		t.Errorf("the finalisation failure was not logged:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "shutdown: complete") {
		t.Errorf("shutdown logged completion despite failing to finalise:\n%s", logs.String())
	}
}

// Spec §10.2 (7), the half the test above cannot reach.
//
// Here the device never fails — it is simply slower than the shutdown budget.
// The log stays healthy, so the supervisor has nothing of its own to report, and
// the only thing between a file that was never fully synced and an exit code of
// zero is gracefulShutdown reporting the finalisation failure itself.
//
// Written after mutation showed the error-reporting version passing with that
// reporting removed.
func TestFinalisationTimeoutProducesANonZeroShutdown(t *testing.T) {
	release := make(chan struct{})
	// Released at the end so the writer goroutine parked in Sync can exit.
	defer close(release)

	srv, sup, logs := newLoggedServer(t, slowSyncFile{release: release}, aof.EverySec)
	srv.cfg.ShutdownTimeout = 100 * time.Millisecond

	if err := srv.eng.Set("k", "v", engine.NoExpiry()); err != nil {
		t.Fatalf("Set: %v", err)
	}

	err := srv.gracefulShutdown()

	if err == nil {
		t.Fatal("gracefulShutdown returned nil although finalisation never completed; " +
			"the process would exit 0 with a file that was never synced")
	}
	if !strings.Contains(logs.String(), "persistence did not finalise cleanly") {
		t.Errorf("the finalisation failure was not logged:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "shutdown: complete") {
		t.Errorf("shutdown logged completion despite unfinished finalisation:\n%s", logs.String())
	}
	// The route matters: nothing else reported this, so the supervisor learned
	// of it from the finalisation branch.
	if !sup.Fired() {
		t.Error("the finalisation failure never reached the supervisor")
	}
}
