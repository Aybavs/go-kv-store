package aof

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeFile is the second real implementation the seam exists for. Partial
// writes and Sync failures are rare enough that they would never appear in
// ordinary testing, which is exactly why they need to be produced on demand.
type fakeFile struct {
	mu sync.Mutex

	written []byte
	syncs   int
	closed  bool

	// writeLimit caps how many bytes a single Write accepts, so a batch is
	// delivered in pieces. Zero means no limit.
	writeLimit int
	// writeErr is returned by Write, after writeLimit bytes have been taken.
	writeErr error
	syncErr  error
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := len(p)
	if f.writeLimit > 0 && n > f.writeLimit {
		n = f.writeLimit
	}
	f.written = append(f.written, p[:n]...)
	return n, f.writeErr
}

func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return f.syncErr
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeFile) bytes() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.written...)
}

func (f *fakeFile) syncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.syncs
}

func openTestLog(t *testing.T, f File, p Policy) (*Log, <-chan error) {
	t.Helper()
	fatal := make(chan error, 1)
	l := Open(f, p, func(err error) {
		select {
		case fatal <- err:
		default:
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = l.Close(ctx)
	})
	return l, fatal
}

func awaitOrFail(t *testing.T, l *Log, seq uint64, p Policy) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- l.Await(seq, p) }()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatalf("Await(%d) never returned; a waiter must wake on progress, failure or shutdown", seq)
		return nil
	}
}

func TestAppendAndAwaitWritten(t *testing.T) {
	f := &fakeFile{}
	l, _ := openTestLog(t, f, EverySec)

	seq, err := l.Append(DeriveSet("k", "v", time.Time{}, false))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 1 {
		t.Fatalf("first sequence = %d, want 1", seq)
	}
	if err := awaitOrFail(t, l, seq, EverySec); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if len(f.bytes()) == 0 {
		t.Fatal("Await returned before anything reached the file")
	}
}

// TestEverySecDoesNotWaitForSync is the difference between the two policies,
// and the reason they are separate at all.
func TestEverySecDoesNotWaitForSync(t *testing.T) {
	f := &fakeFile{}
	l, _ := openTestLog(t, f, EverySec)

	seq, _ := l.Append(DeriveSet("k", "v", time.Time{}, false))
	if err := awaitOrFail(t, l, seq, EverySec); err != nil {
		t.Fatalf("Await: %v", err)
	}
	_, written, synced := l.Markers()
	if written < seq {
		t.Fatalf("written = %d, want at least %d", written, seq)
	}
	if synced >= seq && f.syncCount() == 0 {
		t.Fatal("synced advanced without a Sync having happened")
	}
}

func TestAlwaysWaitsForSync(t *testing.T) {
	f := &fakeFile{}
	l, _ := openTestLog(t, f, Always)

	seq, _ := l.Append(DeriveSet("k", "v", time.Time{}, false))
	if err := awaitOrFail(t, l, seq, Always); err != nil {
		t.Fatalf("Await: %v", err)
	}
	if _, _, synced := l.Markers(); synced < seq {
		t.Fatalf("Await returned under Always with synced = %d, want at least %d", synced, seq)
	}
	if f.syncCount() == 0 {
		t.Fatal("Await returned under Always without any Sync")
	}
}

// TestPartialWriteDoesNotAdvancePastAnIncompleteRecord is the property spec
// §6.5 states explicitly and the one a real disk will almost never demonstrate.
// A record delivered half-way is not durable in any sense, so the marker must
// stop at the last record that made it whole.
func TestPartialWriteDoesNotAdvancePastAnIncompleteRecord(t *testing.T) {
	// One byte at a time, then fail. Whatever prefix arrived, the marker may
	// only name records that were fully delivered.
	f := &fakeFile{writeLimit: 1, writeErr: errors.New("disk full")}
	l, fatal := openTestLog(t, f, EverySec)

	first, _ := l.Append(DeriveSet("k1", "v1", time.Time{}, false))
	second, _ := l.Append(DeriveSet("k2", "v2", time.Time{}, false))

	if err := awaitOrFail(t, l, second, EverySec); err == nil {
		t.Fatal("Await returned success after the write failed")
	}
	select {
	case <-fatal:
	case <-time.After(2 * time.Second):
		t.Fatal("a write failure was never reported as fatal")
	}

	_, written, _ := l.Markers()
	delivered := len(f.bytes())

	// The first record's own bytes are the bound: written may only name it if
	// all of them arrived.
	firstRecord := encodeAll(t, DeriveSet("k1", "v1", time.Time{}, false))
	if written >= first && delivered < len(firstRecord) {
		t.Fatalf("written = %d after only %d of %d bytes of that record were delivered",
			written, delivered, len(firstRecord))
	}
	if written >= second {
		t.Fatalf("written = %d names a record the failing write never finished", written)
	}
}

// TestPartialWriteIsRetried: a Write that accepts less than the whole buffer
// without erroring is not a failure, it is the normal contract of write(2). The
// batch must be delivered completely across several calls.
func TestPartialWriteIsRetried(t *testing.T) {
	f := &fakeFile{writeLimit: 3} // no error, just small bites
	l, _ := openTestLog(t, f, EverySec)

	rec := DeriveSet("key", "value", time.Time{}, false)
	seq, _ := l.Append(rec)
	if err := awaitOrFail(t, l, seq, EverySec); err != nil {
		t.Fatalf("Await: %v", err)
	}

	want := encodeAll(t, rec)
	if got := f.bytes(); string(got) != string(want) {
		t.Fatalf("file holds %q, want %q", got, want)
	}
}

// TestWriterMakingNoProgressFails guards against a spin: a Write that returns
// (0, nil) forever would otherwise loop without end inside the writer.
func TestWriterMakingNoProgressFails(t *testing.T) {
	l, fatal := openTestLog(t, stuckFile{}, EverySec)
	seq, _ := l.Append(DeriveSet("k", "v", time.Time{}, false))

	if err := awaitOrFail(t, l, seq, EverySec); err == nil {
		t.Fatal("Await succeeded against a writer that never accepts bytes")
	}
	select {
	case <-fatal:
	case <-time.After(2 * time.Second):
		t.Fatal("a stuck writer was never reported as fatal")
	}
}

type stuckFile struct{}

func (stuckFile) Write(p []byte) (int, error) { return 0, nil }
func (stuckFile) Sync() error                 { return nil }
func (stuckFile) Close() error                { return nil }

func TestSyncFailureIsFatal(t *testing.T) {
	f := &fakeFile{syncErr: errors.New("fsync failed")}
	l, fatal := openTestLog(t, f, Always)

	seq, _ := l.Append(DeriveSet("k", "v", time.Time{}, false))
	if err := awaitOrFail(t, l, seq, Always); err == nil {
		t.Fatal("Await succeeded despite Sync failing")
	}
	select {
	case err := <-fatal:
		if !errors.Is(err, ErrFailed) {
			t.Fatalf("fatal reported %v, want it to wrap ErrFailed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a Sync failure was never reported as fatal")
	}
}

// TestFailedLogStaysFailed: an already-known failure must refuse the next
// mutation before it touches memory, which is what keeps memory and disk from
// diverging further once the log is broken.
func TestFailedLogStaysFailed(t *testing.T) {
	f := &fakeFile{writeErr: errors.New("disk full")}
	l, _ := openTestLog(t, f, EverySec)

	seq, err := l.Append(DeriveSet("k", "v", time.Time{}, false))
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	_ = awaitOrFail(t, l, seq, EverySec)

	if _, err := l.Append(DeriveSet("k2", "v2", time.Time{}, false)); !errors.Is(err, ErrFailed) {
		t.Fatalf("Append after failure = %v, want ErrFailed", err)
	}
	if err := l.Failed(); !errors.Is(err, ErrFailed) {
		t.Fatalf("Failed() = %v, want ErrFailed", err)
	}
}

// TestFatalIsReportedOnce: the supervisor turns this into a shutdown, and a
// second report during that window would be noise at best.
func TestFatalIsReportedOnce(t *testing.T) {
	var mu sync.Mutex
	count := 0
	f := &fakeFile{writeErr: errors.New("disk full")}
	l := Open(f, EverySec, func(error) { mu.Lock(); count++; mu.Unlock() })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = l.Close(ctx)
	})

	seq, _ := l.Append(DeriveSet("k", "v", time.Time{}, false))
	_ = awaitOrFail(t, l, seq, EverySec)
	// Any further activity must not report again.
	_, _ = l.Append(DeriveSet("k2", "v2", time.Time{}, false))
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("onFatal called %d times, want exactly 1", count)
	}
}

// gatedFile holds Sync open until the test releases it, so waiters are
// provably parked at the moment the failure happens. Without the gate the
// waiters arrive after the log has already failed and Await returns without
// ever having been woken — which is a different property, and a weaker one.
type gatedFile struct {
	release chan struct{}
	err     error
}

func (g *gatedFile) Write(p []byte) (int, error) { return len(p), nil }
func (g *gatedFile) Sync() error                 { <-g.release; return g.err }
func (g *gatedFile) Close() error                { return nil }

// TestWaitersWakeOnFailure is spec §6.7's requirement: a caller blocked at the
// moment the log breaks is woken with an error rather than left waiting for a
// marker that will never advance. Their mutation is already applied to memory
// and may already have been seen by other clients, and that ambiguity is
// precisely why the condition is fatal instead of retryable.
func TestWaitersWakeOnFailure(t *testing.T) {
	g := &gatedFile{release: make(chan struct{}), err: errors.New("fsync failed")}
	l, _ := openTestLog(t, g, Always)

	seq, err := l.Append(DeriveSet("k", "v", time.Time{}, false))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	const waiters = 4
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	parked := make(chan struct{}, waiters)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			parked <- struct{}{}
			errs[i] = l.Await(seq, Always)
		}(i)
	}
	for range waiters {
		<-parked
	}
	// Every waiter has entered Await and the Sync is still held open, so none
	// of them can have returned yet.
	time.Sleep(100 * time.Millisecond)
	if _, _, synced := l.Markers(); synced >= seq {
		t.Fatal("the gate did not hold; this test would not be measuring anything")
	}

	close(g.release) // Sync now fails, with everyone already waiting

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("waiters were not woken when the log failed")
	}
	for i, err := range errs {
		if !errors.Is(err, ErrFailed) {
			t.Fatalf("waiter %d got %v, want ErrFailed", i, err)
		}
	}
}

// TestGroupCommit: under Always, writers waiting on the same Sync share one
// syscall by construction rather than through a separate mechanism.
func TestGroupCommit(t *testing.T) {
	f := &fakeFile{}
	l, _ := openTestLog(t, f, Always)

	const writers = 20
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seq, err := l.Append(DeriveSet("k", "v", time.Time{}, false))
			if err != nil {
				t.Errorf("Append: %v", err)
				return
			}
			if err := l.Await(seq, Always); err != nil {
				t.Errorf("Await: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := f.syncCount(); got >= writers {
		t.Fatalf("%d writers caused %d syncs; batching is not happening at all", writers, got)
	}
	t.Logf("%d writers shared %d syncs", writers, f.syncCount())
}

func TestCloseFlushesAndSyncs(t *testing.T) {
	f := &fakeFile{}
	l := Open(f, EverySec, func(err error) { t.Errorf("unexpected fatal: %v", err) })

	rec := DeriveSet("k", "v", time.Time{}, false)
	if _, err := l.Append(rec); err != nil {
		t.Fatalf("Append: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got, want := f.bytes(), encodeAll(t, rec); string(got) != string(want) {
		t.Fatalf("Close did not flush: file holds %q", got)
	}
	// Even under EverySec: a clean shutdown is where "written" stops being
	// good enough.
	if f.syncCount() == 0 {
		t.Fatal("Close did not Sync")
	}
	if !f.closed {
		t.Fatal("Close did not close the file")
	}
	if _, err := l.Append(rec); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	f := &fakeFile{}
	l := Open(f, EverySec, func(err error) { t.Errorf("unexpected fatal: %v", err) })
	ctx := context.Background()
	if err := l.Close(ctx); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSequenceNumbersAreMonotonic(t *testing.T) {
	f := &fakeFile{}
	l, _ := openTestLog(t, f, EverySec)

	var prev uint64
	for i := 0; i < 50; i++ {
		seq, err := l.Append(DeriveSet("k", "v", time.Time{}, false))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if seq != prev+1 {
			t.Fatalf("sequence jumped from %d to %d", prev, seq)
		}
		prev = seq
	}
	if appended, _, _ := l.Markers(); appended != prev {
		t.Fatalf("appended marker = %d, want %d", appended, prev)
	}
}

// TestMarkersNeverInvert: synced <= written <= appended is the invariant the
// three markers exist to express, and a reader of any one of them relies on it.
func TestMarkersNeverInvert(t *testing.T) {
	f := &fakeFile{}
	l, _ := openTestLog(t, f, Always)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			a, w, s := l.Markers()
			if !(s <= w && w <= a) {
				t.Errorf("markers inverted: appended=%d written=%d synced=%d", a, w, s)
				return
			}
		}
	}()

	for i := 0; i < 200; i++ {
		if _, err := l.Append(DeriveSet("k", "v", time.Time{}, false)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	close(stop)
	wg.Wait()
}
