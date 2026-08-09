package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/resp"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
}

func TestSetGetDeleteExists(t *testing.T) {
	e := newTestEngine(t)

	if err := e.Set("a", "1", NoExpiry()); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, ok := e.Get("a"); !ok || v != "1" {
		t.Fatalf("Get = (%q, %v), want (\"1\", true)", v, ok)
	}
	if n := e.Exists([]string{"a", "missing", "a"}); n != 2 {
		t.Fatalf("Exists = %d, want 2 (duplicates counted)", n)
	}
	n, err := e.Delete([]string{"a", "missing"})
	if err != nil || n != 1 {
		t.Fatalf("Delete = (%d, %v), want (1, nil)", n, err)
	}
	if _, ok := e.Get("a"); ok {
		t.Fatal("key still present after Delete")
	}
}

// TestDrainRejectsMutations asserts the admission gate: once BeginDrain
// returns, no further mutation may be admitted. Reads must keep working.
func TestDrainRejectsMutations(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Set("a", "1", NoExpiry()); err != nil {
		t.Fatal(err)
	}

	e.BeginDrain()

	if err := e.Set("b", "2", NoExpiry()); !errors.Is(err, ErrDraining) {
		t.Fatalf("Set after drain = %v, want ErrDraining", err)
	}
	if _, err := e.Delete([]string{"a"}); !errors.Is(err, ErrDraining) {
		t.Fatalf("Delete after drain = %v, want ErrDraining", err)
	}
	if v, ok := e.Get("a"); !ok || v != "1" {
		t.Fatalf("reads must continue while draining, got (%q, %v)", v, ok)
	}
}

// TestConcurrentAccess is the race-detector target for the engine.
func TestConcurrentAccess(t *testing.T) {
	e := newTestEngine(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if err := e.Set("shared", "v", NoExpiry()); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				e.Get("shared")
				e.Exists([]string{"shared"})
			}
		}()
	}
	wg.Wait()
}

// TestGuardReportsFatalThenRepanics covers the package's namesake behaviour.
// A panic in the commit path must both reach onFatal — because panics do not
// cross goroutine boundaries and the supervisor lives in another one — and
// continue unwinding, so the offending goroutine still dies rather than
// carrying on over shared state that may now be inconsistent.
func TestGuardReportsFatalThenRepanics(t *testing.T) {
	var reported error
	e := New(func(err error) { reported = err })

	func() {
		defer func() {
			if recover() == nil {
				t.Error("guard swallowed the panic; it must re-panic so the goroutine dies")
			}
		}()
		defer e.guard()
		panic("simulated commit-path failure")
	}()

	if reported == nil {
		t.Fatal("guard did not report the panic to onFatal")
	}
	if !strings.Contains(reported.Error(), "simulated commit-path failure") {
		t.Fatalf("report does not carry the panic value: %v", reported)
	}
	if !strings.Contains(reported.Error(), "engine commit path panic") {
		t.Fatalf("report is not labelled as a commit-path panic: %v", reported)
	}
	if !strings.Contains(reported.Error(), "engine.TestGuardReportsFatalThenRepanics") {
		t.Fatalf("report does not include a stack trace: %v", reported)
	}
}

// TestNewRejectsNilOnFatal pins the constructor contract. A nil callback would
// make guard() panic on a nil func value before reporting anything, turning a
// diagnosable fatal condition into an opaque one.
func TestNewRejectsNilOnFatal(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New(nil) must panic; a nil onFatal silently defeats fatal reporting")
		}
	}()
	New(nil)
}

// TestBeginDrainUnderContention drains while writers are mid-flight: an
// admitted mutation must be present afterwards, a refused one absent.
//
// The overlap is constructed, not hoped for. A writer runs until it is refused
// and the drain waits until every writer has had one admitted, so all are still
// looping when the gate closes and each is refused exactly once. An earlier
// version used a fixed mutation count and failed in CI, where every writer
// finished before the drain ran.
func TestBeginDrainUnderContention(t *testing.T) {
	e := newTestEngine(t)

	const (
		writers = 8
		// Safety valve: a gate that never closes should fail this test, not hang it.
		maxPerWriter = 1 << 20
	)

	type outcome struct {
		key      string
		admitted bool
	}

	firstAdmit := make(chan struct{}, writers)
	results := make([][]outcome, writers)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out []outcome
			defer func() { results[i] = out }()

			signalled := false
			for j := 0; j < maxPerWriter; j++ {
				key := fmt.Sprintf("k%d-%d", i, j)
				err := e.Set(key, "v", NoExpiry())
				switch {
				case err == nil:
					out = append(out, outcome{key: key, admitted: true})
					if !signalled {
						signalled = true
						firstAdmit <- struct{}{}
					}
				case errors.Is(err, ErrDraining):
					out = append(out, outcome{key: key, admitted: false})
					return // the gate is final: no later attempt could succeed
				default:
					t.Errorf("unexpected error from Set: %v", err)
					return
				}
			}
			t.Errorf("writer %d reached the iteration cap without ever being refused", i)
		}(i)
	}

	// Every writer has had one admitted, so every writer is still in its loop.
	for i := 0; i < writers; i++ {
		<-firstAdmit
	}
	e.BeginDrain()

	wg.Wait()

	admitted, refused := 0, 0
	for _, out := range results {
		for _, r := range out {
			_, present := e.Get(r.key)
			if r.admitted {
				admitted++
				if !present {
					t.Fatalf("Set(%q) reported success but the key is absent", r.key)
				}
			} else {
				refused++
				if present {
					t.Fatalf("Set(%q) reported ErrDraining but the key is present", r.key)
				}
			}
		}
	}

	// Exact, not "at least one": the gate never reopens, so each writer
	// contributes precisely one refusal.
	if refused != writers {
		t.Fatalf("refused = %d, want %d (one per writer)", refused, writers)
	}
	if admitted < writers {
		t.Fatalf("admitted = %d, want at least %d (one per writer before the drain)", admitted, writers)
	}

	// Finality: the gate does not reopen.
	if err := e.Set("after", "v", NoExpiry()); !errors.Is(err, ErrDraining) {
		t.Fatalf("Set after drain = %v, want ErrDraining", err)
	}
	if _, present := e.Get("after"); present {
		t.Fatal("a refused mutation was applied anyway")
	}
}

// TestDeleteCountsEachKeyOnce pins that a repeated key is removed once and
// counted once, matching Redis. Only the first Delete finds it present.
func TestDeleteCountsEachKeyOnce(t *testing.T) {
	e := newTestEngine(t)
	if err := e.Set("a", "1", NoExpiry()); err != nil {
		t.Fatal(err)
	}
	n, err := e.Delete([]string{"a", "a"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n != 1 {
		t.Fatalf("Delete = %d, want 1", n)
	}
}

// TestClockIsReadExactlyWhenItMatters pins both halves of the read path's
// contract with the clock.
//
// Correctness: when a deadline exists, expiry must be judged against the
// injected clock, or every later TTL test would be measuring nothing.
//
// Cost: when no deadline exists, the clock must not be read at all. That is not
// tidiness — time.Now costs about 54 ns on this machine against roughly 4 ns
// for the map lookups around it, so reading it unconditionally made the engine
// read path more than five times slower than v0.1.0 for a store that might hold
// no TTLs whatsoever.
func TestClockIsReadExactlyWhenItMatters(t *testing.T) {
	var calls int
	fixed := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	e := NewWithClock(
		func(err error) { t.Errorf("unexpected fatal: %v", err) },
		func() time.Time { calls++; return fixed },
	)

	t.Run("not read when nothing can expire", func(t *testing.T) {
		if err := e.Set("plain", "v", NoExpiry()); err != nil {
			t.Fatalf("Set: %v", err)
		}
		calls = 0
		if _, ok := e.Get("plain"); !ok {
			t.Fatal("Get did not find the key")
		}
		e.Exists([]string{"plain"})
		e.TTL("plain")
		if calls != 0 {
			t.Fatalf("clock read %d times with no deadline in the store, want 0", calls)
		}
	})

	t.Run("read once per call when a deadline exists", func(t *testing.T) {
		if err := e.Set("timed", "v", ExpiresIn(time.Hour)); err != nil {
			t.Fatalf("Set: %v", err)
		}

		calls = 0
		if _, ok := e.Get("plain"); !ok {
			t.Fatal("Get did not find the key")
		}
		if calls != 1 {
			t.Fatalf("Get read the clock %d times, want 1", calls)
		}

		// Keys in a single EXISTS must be judged against the same instant, so
		// one read covers the whole call however many keys it names.
		calls = 0
		if n := e.Exists([]string{"plain", "timed", "absent"}); n != 2 {
			t.Fatalf("Exists = %d, want 2", n)
		}
		if calls != 1 {
			t.Fatalf("Exists read the clock %d times, want 1 for the whole call", calls)
		}
	})
}

func TestNewRejectsNilClock(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewWithClock accepted a nil clock")
		}
	}()
	NewWithClock(func(error) {}, nil)
}

// testClock is a clock the test moves by hand. Expiry is reached by advancing
// it, never by waiting, so these tests cannot go flaky on a loaded runner.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newClockedEngine(t *testing.T) (*Engine, *testClock) {
	t.Helper()
	c := newTestClock()
	return NewWithClock(func(err error) { t.Errorf("unexpected fatal: %v", err) }, c.now), c
}

// TestLazyExpirationHidesKeyWithoutReclamation is the invariant the whole
// design rests on: a key is gone the moment its deadline passes, on the read
// path, with no worker having run and no write lock taken to delete it.
func TestLazyExpirationHidesKeyWithoutReclamation(t *testing.T) {
	e, clock := newClockedEngine(t)

	if err := e.Set("k", "v", ExpiresIn(10*time.Second)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := e.Get("k"); !ok {
		t.Fatal("key absent before its deadline")
	}

	clock.advance(10 * time.Second)

	if _, ok := e.Get("k"); ok {
		t.Fatal("Get returned an expired key")
	}
	if n := e.Exists([]string{"k"}); n != 0 {
		t.Fatalf("Exists = %d, want 0", n)
	}
	if _, st := e.TTL("k"); st != NoKey {
		t.Fatalf("TTL status = %v, want NoKey", st)
	}
	// Nothing reclaimed it: the entry is still in the map, and that is the
	// point. Visibility and reclamation are separate events.
	if got := e.store.Len(clock.now().Add(-time.Hour)); got != 1 {
		t.Fatalf("entry was physically removed by a read; live-at-earlier-time count = %d, want 1", got)
	}
}

func TestSetClearsTTLThroughEngine(t *testing.T) {
	e, clock := newClockedEngine(t)

	_ = e.Set("k", "v", ExpiresIn(10*time.Second))
	_ = e.Set("k", "v2", NoExpiry())

	clock.advance(time.Hour)
	if _, ok := e.Get("k"); !ok {
		t.Fatal("key expired although the second SET carried no TTL")
	}
	if _, st := e.TTL("k"); st != NoTTL {
		t.Fatalf("TTL status = %v, want NoTTL", st)
	}
}

func TestTTLReportsRemaining(t *testing.T) {
	e, clock := newClockedEngine(t)
	_ = e.Set("k", "v", ExpiresIn(30*time.Second))

	clock.advance(10 * time.Second)
	d, st := e.TTL("k")
	if st != HasTTL || d != 20*time.Second {
		t.Fatalf("TTL = (%v, %v), want (20s, HasTTL)", d, st)
	}

	_ = e.Set("plain", "v", NoExpiry())
	if _, st := e.TTL("plain"); st != NoTTL {
		t.Fatalf("key without a TTL: %v, want NoTTL", st)
	}
	if _, st := e.TTL("absent"); st != NoKey {
		t.Fatalf("missing key: %v, want NoKey", st)
	}
}

func TestExpireAndPersist(t *testing.T) {
	e, clock := newClockedEngine(t)
	_ = e.Set("k", "v", NoExpiry())

	applied, err := e.Expire("k", 10*time.Second)
	if err != nil || !applied {
		t.Fatalf("Expire = (%v, %v), want (true, nil)", applied, err)
	}
	applied, err = e.Expire("absent", 10*time.Second)
	if err != nil || applied {
		t.Fatalf("Expire on a missing key = (%v, %v), want (false, nil)", applied, err)
	}

	removed, err := e.Persist("k")
	if err != nil || !removed {
		t.Fatalf("Persist = (%v, %v), want (true, nil)", removed, err)
	}
	clock.advance(time.Hour)
	if _, ok := e.Get("k"); !ok {
		t.Fatal("key expired after its TTL was removed")
	}
}

// TestDeleteDoesNotCountExpiredKeys: DEL reports how many keys it removed from
// the client's point of view, and an expired key was already gone.
func TestDeleteDoesNotCountExpiredKeys(t *testing.T) {
	e, clock := newClockedEngine(t)
	_ = e.Set("live", "v", NoExpiry())
	_ = e.Set("dead", "v", ExpiresIn(time.Second))

	clock.advance(time.Hour)

	n, err := e.Delete([]string{"live", "dead"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n != 1 {
		t.Fatalf("Delete = %d, want 1 (the expired key was already absent)", n)
	}
}

func TestTTLMutationsRefusedWhileDraining(t *testing.T) {
	e, _ := newClockedEngine(t)
	_ = e.Set("k", "v", NoExpiry())
	e.BeginDrain()

	if err := e.Set("k", "v", ExpiresIn(time.Second)); !errors.Is(err, ErrDraining) {
		t.Fatalf("Set = %v, want ErrDraining", err)
	}
	if _, err := e.Expire("k", time.Second); !errors.Is(err, ErrDraining) {
		t.Fatalf("Expire = %v, want ErrDraining", err)
	}
	if _, err := e.Persist("k"); !errors.Is(err, ErrDraining) {
		t.Fatalf("Persist = %v, want ErrDraining", err)
	}
	// Reads keep working while draining.
	if _, ok := e.Get("k"); !ok {
		t.Fatal("Get failed during drain")
	}
}

// TestActiveExpirationReclaims drives the worker one cycle at a time through an
// injected tick channel. Nothing waits on a real interval, so the test asserts
// reclamation rather than asserting that a sleep was long enough.
func TestActiveExpirationReclaims(t *testing.T) {
	e, clock := newClockedEngine(t)
	for i := range 10 {
		_ = e.Set("k"+strconv.Itoa(i), "v", ExpiresIn(time.Second))
	}
	clock.advance(time.Hour)

	// Every key is already invisible. Only the physical entries remain.
	if n := e.Exists([]string{"k0", "k5", "k9"}); n != 0 {
		t.Fatalf("expired keys still visible: %d", n)
	}
	if got := e.physicalLen(); got != 10 {
		t.Fatalf("physical entries = %d, want 10 before any cycle", got)
	}

	ticks := make(chan time.Time)
	e.startExpiration(ticks, nil)
	t.Cleanup(func() { _ = e.StopExpiration(context.Background()) })

	// One cycle is bounded by expirationSample, so a handful of cycles clears
	// ten keys. Drive them explicitly instead of waiting.
	deadline := time.After(5 * time.Second)
	for e.physicalLen() > 0 {
		select {
		case ticks <- clock.now():
		case <-deadline:
			t.Fatalf("reclamation did not finish; %d entries left", e.physicalLen())
		}
	}
}

// TestActiveExpirationLeavesLiveKeys guards the obvious catastrophe: a worker
// that reclaims everything would pass a "memory was reclaimed" assertion.
func TestActiveExpirationLeavesLiveKeys(t *testing.T) {
	e, clock := newClockedEngine(t)
	_ = e.Set("dead", "v", ExpiresIn(time.Second))
	_ = e.Set("live", "v", ExpiresIn(time.Hour))
	_ = e.Set("plain", "v", NoExpiry())

	clock.advance(time.Minute)

	if removed, _ := e.reclaimOnce(); removed != 1 {
		t.Fatalf("removed = %d, want exactly the one expired key", removed)
	}
	if _, ok := e.Get("live"); !ok {
		t.Fatal("a key with a future deadline was reclaimed")
	}
	if _, ok := e.Get("plain"); !ok {
		t.Fatal("a key with no deadline was reclaimed")
	}
}

func TestStopExpirationIsSafeAndIdempotent(t *testing.T) {
	e, _ := newClockedEngine(t)

	// Bounded, deliberately: StopExpiration waits for the worker to exit, so a
	// worker that never does would hang this test until the whole package times
	// out. A bounded context turns that into a failure that names itself.
	stop := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return e.StopExpiration(ctx)
	}

	// Never started.
	if err := stop(); err != nil {
		t.Fatalf("StopExpiration before StartExpiration: %v", err)
	}

	ticks := make(chan time.Time)
	e.startExpiration(ticks, nil)
	e.startExpiration(ticks, nil) // second call must not start a second worker

	if err := stop(); err != nil {
		t.Fatalf("StopExpiration did not stop the worker: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("second StopExpiration: %v", err)
	}

	// A worker that was still running would receive this and the test would
	// block; the send must not be received by anyone.
	select {
	case ticks <- time.Now():
		t.Fatal("a worker was still consuming ticks after StopExpiration")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestExpirationWorkerUnderReaders runs the worker against concurrent readers.
// The point is the race detector, not the counts.
func TestExpirationWorkerUnderReaders(t *testing.T) {
	e, clock := newClockedEngine(t)
	for i := range 200 {
		_ = e.Set("k"+strconv.Itoa(i), "v", ExpiresIn(time.Second))
	}
	clock.advance(time.Hour)

	ticks := make(chan time.Time)
	e.startExpiration(ticks, nil)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				e.Get("k" + strconv.Itoa(i%200))
				e.TTL("k" + strconv.Itoa(i%200))
				e.Exists([]string{"k1", "k2"})
			}
		}()
	}
	for range 20 {
		ticks <- clock.now()
	}
	close(stop)
	wg.Wait()

	if err := e.StopExpiration(context.Background()); err != nil {
		t.Fatalf("StopExpiration: %v", err)
	}
}

// recordingFile captures what the engine actually wrote, so the effect records
// can be asserted rather than assumed.
type recordingFile struct {
	mu      sync.Mutex
	written []byte
	syncErr error
	// gate, when non-nil, holds every Write open until it is closed.
	gate chan struct{}
}

func (f *recordingFile) Write(p []byte) (int, error) {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, p...)
	return len(p), nil
}

func (f *recordingFile) Sync() error  { return f.syncErr }
func (f *recordingFile) Close() error { return nil }

func (f *recordingFile) records(t *testing.T) []aof.Record {
	t.Helper()
	f.mu.Lock()
	data := append([]byte(nil), f.written...)
	f.mu.Unlock()

	r := resp.NewReader(bytes.NewReader(data), resp.DefaultLimits())
	var out []aof.Record
	for {
		rec, err := aof.Decode(r)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("decoding what the engine wrote: %v", err)
		}
		out = append(out, rec)
	}
}

func newLoggedEngine(t *testing.T, f aof.File, p aof.Policy) (*Engine, *testClock, <-chan error) {
	t.Helper()
	c := newTestClock()
	fatal := make(chan error, 1)
	e := NewWithClock(func(err error) {
		select {
		case fatal <- err:
		default:
		}
	}, c.now)
	l := aof.Open(f, p, func(err error) {
		select {
		case fatal <- err:
		default:
		}
	})
	e.AttachLog(l, p)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = l.Close(ctx)
	})
	return e, c, fatal
}

// TestEffectsAreLoggedNotCommands is the ADR-0004 property end to end: what
// lands in the log is the resulting state, so no record depends on one before
// it. EXPIRE and PERSIST in particular become complete SETs carrying the value.
func TestEffectsAreLoggedNotCommands(t *testing.T) {
	f := &recordingFile{}
	e, clock, _ := newLoggedEngine(t, f, aof.EverySec)

	if err := e.Set("k", "5", NoExpiry()); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := e.Expire("k", 30*time.Second); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if _, err := e.Persist("k"); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	if _, err := e.Delete([]string{"k"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := f.records(t)
	if len(got) != 4 {
		t.Fatalf("wrote %d records, want 4: %+v", len(got), got)
	}

	// EXPIRE is not logged as "EXPIRE k 30"; it is the state the key ends in.
	expire := got[1]
	if expire.Kind != aof.KindSet || expire.Key != "k" || expire.Value != "5" {
		t.Fatalf("EXPIRE logged as %+v, want a SET carrying the value", expire)
	}
	if !expire.HasExpiry || expire.ExpireAtMS != clock.now().Add(30*time.Second).UnixMilli() {
		t.Fatalf("EXPIRE record has deadline %d, want an absolute PXAT", expire.ExpireAtMS)
	}

	// PERSIST likewise: a SET with no expiry, not a "PERSIST k".
	persist := got[2]
	if persist.Kind != aof.KindSet || persist.Value != "5" || persist.HasExpiry {
		t.Fatalf("PERSIST logged as %+v, want a SET with no expiry", persist)
	}

	if got[3].Kind != aof.KindDel || len(got[3].Keys) != 1 || got[3].Keys[0] != "k" {
		t.Fatalf("DEL logged as %+v", got[3])
	}
}

// TestNothingObservableNothingLogged: a call that changes nothing must not
// append a record. Otherwise every failed EXPIRE grows the log.
func TestNothingObservableNothingLogged(t *testing.T) {
	f := &recordingFile{}
	e, _, _ := newLoggedEngine(t, f, aof.EverySec)

	if applied, err := e.Expire("absent", time.Second); err != nil || applied {
		t.Fatalf("Expire on a missing key = (%v, %v)", applied, err)
	}
	if removed, err := e.Persist("absent"); err != nil || removed {
		t.Fatalf("Persist on a missing key = (%v, %v)", removed, err)
	}
	if n, err := e.Delete([]string{"absent"}); err != nil || n != 0 {
		t.Fatalf("Delete of a missing key = (%v, %v)", n, err)
	}

	_ = e.Set("plain", "v", NoExpiry())
	if removed, err := e.Persist("plain"); err != nil || removed {
		t.Fatalf("Persist on a key with no TTL = (%v, %v)", removed, err)
	}

	got := f.records(t)
	if len(got) != 1 {
		t.Fatalf("wrote %d records, want 1 (only the SET changed anything): %+v", len(got), got)
	}
}

// TestDelRecordsOnlyLiveKeys: an expired key is already absent on replay, so
// naming it in the record would be noise, and counting it would be wrong.
func TestDelRecordsOnlyLiveKeys(t *testing.T) {
	f := &recordingFile{}
	e, clock, _ := newLoggedEngine(t, f, aof.EverySec)

	_ = e.Set("live", "v", NoExpiry())
	_ = e.Set("dead", "v", ExpiresIn(time.Second))
	clock.advance(time.Hour)

	n, err := e.Delete([]string{"live", "dead", "absent", "live"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if n != 1 {
		t.Fatalf("Delete reported %d, want 1", n)
	}

	got := f.records(t)
	del := got[len(got)-1]
	if del.Kind != aof.KindDel {
		t.Fatalf("last record is %+v, want a DEL", del)
	}
	if len(del.Keys) != 1 || del.Keys[0] != "live" {
		t.Fatalf("DEL record names %v, want only the live key", del.Keys)
	}
}

// TestAwaitHappensOutsideTheLock is spec §6.4's structural claim. A writer that
// never completes must not stop an unrelated reader: if Await were inside the
// commit lock, this test would hang.
func TestAwaitHappensOutsideTheLock(t *testing.T) {
	f := &recordingFile{gate: make(chan struct{})}
	e, _, _ := newLoggedEngine(t, f, aof.Always)

	// Every Write is held open, so this Set reaches Await and stays there. It
	// must run in its own goroutine: called directly it would block the test
	// rather than demonstrate anything.
	stalled := make(chan struct{})
	go func() { defer close(stalled); _ = e.Set("k", "v", NoExpiry()) }()

	// Give it time to get past the lock and into Await. If Await were inside
	// the lock, the mutex would still be held when the reader arrives below.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-stalled:
		t.Fatal("the write completed; the gate did not hold and this test measures nothing")
	default:
	}

	// The reader must not be behind the stalled writer.
	read := make(chan struct{})
	go func() { defer close(read); e.Get("anything"); e.Exists([]string{"x"}) }()

	select {
	case <-read:
	case <-time.After(2 * time.Second):
		t.Fatal("a reader was blocked behind a stalled disk write; Await is holding the lock")
	}

	close(f.gate)
	select {
	case <-stalled:
	case <-time.After(2 * time.Second):
		t.Fatal("the write never completed after the gate opened")
	}
}

// TestAlreadyFailedLogRefusesBeforeMemory: the one failure mode where memory is
// provably untouched, which is why the check happens before the apply.
func TestAlreadyFailedLogRefusesBeforeMemory(t *testing.T) {
	f := &failingFile{err: errors.New("disk full")}
	e, _, fatal := newLoggedEngine(t, f, aof.EverySec)

	// First mutation breaks the log.
	_ = e.Set("k1", "v1", NoExpiry())
	select {
	case <-fatal:
	case <-time.After(2 * time.Second):
		t.Fatal("the write failure was never reported")
	}

	err := e.Set("k2", "v2", NoExpiry())
	if !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Set against a failed log = %v, want ErrPersistenceUnavailable", err)
	}
	if _, ok := e.Get("k2"); ok {
		t.Fatal("a refused mutation reached memory")
	}
}

type failingFile struct{ err error }

func (f *failingFile) Write(p []byte) (int, error) { return 0, f.err }
func (f *failingFile) Sync() error                 { return nil }
func (f *failingFile) Close() error                { return nil }

// TestPersistedOrderMatchesAppliedOrder: the append and the apply happen under
// one acquisition of the lock, so concurrent writers cannot interleave one
// against the other.
func TestPersistedOrderMatchesAppliedOrder(t *testing.T) {
	f := &recordingFile{}
	e, _, _ := newLoggedEngine(t, f, aof.EverySec)

	const writers, each = 8, 40
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				if err := e.Set("k"+strconv.Itoa(w), strconv.Itoa(i), NoExpiry()); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Per key, the values must appear in the order that key's writer produced
	// them. A gap or a reversal means an append and an apply crossed.
	last := map[string]int{}
	for _, rec := range f.records(t) {
		v, err := strconv.Atoi(rec.Value)
		if err != nil {
			t.Fatalf("unexpected value %q", rec.Value)
		}
		prev, seen := last[rec.Key]
		if seen && v != prev+1 {
			t.Fatalf("key %s: recorded %d after %d; persisted order diverged from applied order",
				rec.Key, v, prev)
		}
		last[rec.Key] = v
	}
	if len(last) != writers {
		t.Fatalf("saw %d keys, want %d", len(last), writers)
	}
}

// TestMGetReadsInRequestOrder covers the three answers MGET has to keep apart:
// a value, an empty value, and no key at all. The empty one is the case a
// `Value != ""` check gets wrong, so it is here rather than left to conformance.
func TestMGetReadsInRequestOrder(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Set("a", "1", NoExpiry())
	_ = e.Set("empty", "", NoExpiry())

	got := e.MGet([]string{"a", "empty", "missing", "a"})
	want := []Optional{
		{Value: "1", Found: true},
		{Value: "", Found: true},
		{Value: "", Found: false},
		{Value: "1", Found: true},
	}
	if len(got) != len(want) {
		t.Fatalf("MGet returned %d results, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("result %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestMGetOnNoKeysReturnsEmpty(t *testing.T) {
	e := newTestEngine(t)
	if got := e.MGet(nil); len(got) != 0 {
		t.Fatalf("MGet(nil) = %+v, want empty", got)
	}
}

// TestMGetHidesExpiredKeysWithoutReclaiming is TestLazyExpirationHidesKeyWithout
// Reclamation for the multi-key read: expiry is observed on the read path and
// the entry is still physically there afterwards.
func TestMGetHidesExpiredKeysWithoutReclaiming(t *testing.T) {
	e, clock := newClockedEngine(t)
	_ = e.Set("live", "1", NoExpiry())
	_ = e.Set("doomed", "2", ExpiresIn(10*time.Second))

	clock.advance(10 * time.Second)

	got := e.MGet([]string{"live", "doomed"})
	if !got[0].Found || got[0].Value != "1" {
		t.Errorf("live key = %+v, want found with value 1", got[0])
	}
	if got[1].Found {
		t.Errorf("expired key = %+v, want not found", got[1])
	}
	if n := e.physicalLen(); n != 2 {
		t.Fatalf("a read reclaimed an entry: physical length = %d, want 2", n)
	}
}

// MGET is a read, and reads keep working after the mutation gate closes.
func TestMGetWorksWhileDraining(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Set("a", "1", NoExpiry())
	e.BeginDrain()

	if got := e.MGet([]string{"a"}); !got[0].Found || got[0].Value != "1" {
		t.Fatalf("MGet during drain = %+v, want found with value 1", got[0])
	}
}
