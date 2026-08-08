package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

// TestInjectedClockIsUsed pins the seam itself. The engine supplies the clock
// that store's expiry checks are made against, so a read path that called
// time.Now directly, or skipped the clock entirely, would leave every later TTL
// test measuring nothing. Counting the calls is the only way to see that from
// outside, since v0.2 has no TTL API on the engine yet.
func TestInjectedClockIsUsed(t *testing.T) {
	var calls int
	fixed := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	e := NewWithClock(
		func(err error) { t.Errorf("unexpected fatal: %v", err) },
		func() time.Time { calls++; return fixed },
	)

	if err := e.Set("k", "v", NoExpiry()); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := e.Get("k"); !ok {
		t.Fatal("Get did not find the key")
	}
	if calls == 0 {
		t.Fatal("Get did not consult the injected clock; expiry would be judged against the wrong time")
	}

	before := calls
	if n := e.Exists([]string{"k", "k", "absent"}); n != 2 {
		t.Fatalf("Exists = %d, want 2", n)
	}
	// One read for the whole call, not one per key: keys in a single EXISTS
	// must be judged against the same instant.
	if got := calls - before; got != 1 {
		t.Fatalf("Exists consulted the clock %d times, want 1 for the whole call", got)
	}
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
