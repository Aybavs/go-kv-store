package engine

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	return New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
}

func TestSetGetDeleteExists(t *testing.T) {
	e := newTestEngine(t)

	if err := e.Set("a", "1"); err != nil {
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
	if err := e.Set("a", "1"); err != nil {
		t.Fatal(err)
	}

	e.BeginDrain()

	if err := e.Set("b", "2"); !errors.Is(err, ErrDraining) {
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
				if err := e.Set("shared", "v"); err != nil {
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

// TestBeginDrainUnderContention exercises the admission gate the way shutdown
// actually hits it: many in-flight writers while one goroutine drains. The
// sequential test cannot show race-freedom, and this one is the -race target
// for the gate.
//
// It deliberately does not assert "nothing was admitted after BeginDrain
// returned" by sampling a flag from the writers — that check would itself race,
// since a Set admitted before the drain could read the flag after it. It
// asserts instead that every outcome is consistent with what the engine
// reported, and that the gate is final once writers stop.
func TestBeginDrainUnderContention(t *testing.T) {
	e := newTestEngine(t)

	const (
		writers   = 8
		perWriter = 200
	)

	type outcome struct {
		key      string
		admitted bool
	}

	started := make(chan struct{}, writers)
	results := make(chan outcome, writers*perWriter)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			started <- struct{}{}
			for j := 0; j < perWriter; j++ {
				key := fmt.Sprintf("k%d-%d", i, j)
				err := e.Set(key, "v")
				switch {
				case err == nil:
					results <- outcome{key: key, admitted: true}
				case errors.Is(err, ErrDraining):
					results <- outcome{key: key, admitted: false}
				default:
					t.Errorf("unexpected error from Set: %v", err)
					return
				}
			}
		}(i)
	}

	// Drain only once every writer is actually running, so the gate is exercised
	// against in-flight mutations rather than before any have begun.
	for i := 0; i < writers; i++ {
		<-started
	}
	e.BeginDrain()

	wg.Wait()
	close(results)

	admitted, refused := 0, 0
	for r := range results {
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

	if refused == 0 {
		t.Fatal("no mutation was refused; the drain never overlapped the writers")
	}

	// Finality: the gate does not reopen.
	if err := e.Set("after", "v"); !errors.Is(err, ErrDraining) {
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
	if err := e.Set("a", "1"); err != nil {
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
