package engine

import (
	"errors"
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
