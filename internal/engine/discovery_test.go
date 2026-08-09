package engine

import (
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

func seedDiscovery(t *testing.T, e *Engine, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if err := e.Set(key, "v", NoExpiry()); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}
}

func TestKeysAndDBSizeExposeOnlyLiveKeys(t *testing.T) {
	e, clock := newClockedEngine(t)
	seedDiscovery(t, e, "beta", "alpha", "binary\x00\xff")
	if err := e.Set("expired", "v", ExpiresIn(time.Second)); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Second)
	if got, want := e.Keys("*"), []string{"alpha", "beta", "binary\x00\xff"}; !slices.Equal(got, want) {
		t.Fatalf("Keys = %q, want %q", got, want)
	}
	if got := e.DBSize(); got != 3 {
		t.Fatalf("DBSize = %d, want 3", got)
	}
	if got := e.physicalLen(); got != 4 {
		t.Fatalf("discovery reclaimed data: physicalLen = %d, want 4", got)
	}
}

func TestScanStableDatasetReturnsEachMatchExactlyOnce(t *testing.T) {
	e := newTestEngine(t)
	for i := 0; i < 37; i++ {
		seedDiscovery(t, e, "user:"+strconv.Itoa(i))
	}
	seedDiscovery(t, e, "other")
	seen := map[string]int{}
	cursor := uint64(0)
	for first := true; first || cursor != 0; first = false {
		page := e.Scan(cursor, "user:*", 4)
		if page.Cursor != 0 && page.Cursor <= cursor {
			t.Fatalf("cursor did not progress: %d -> %d", cursor, page.Cursor)
		}
		for _, key := range page.Keys {
			seen[key]++
		}
		cursor = page.Cursor
	}
	if len(seen) != 37 {
		t.Fatalf("full traversal returned %d matches, want 37", len(seen))
	}
	for key, count := range seen {
		if count != 1 {
			t.Errorf("%q returned %d times", key, count)
		}
	}
}

func TestScanMatchAppliesAfterCount(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "a", "b", "match")
	page := e.Scan(0, "match", 1)
	if len(page.Keys) != 0 || page.Cursor == 0 {
		t.Fatalf("first sparse page = %+v, want empty with nonzero cursor", page)
	}
}

func TestScanCursorBeyondSnapshotCompletes(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "a", "b")
	if got := e.Scan(99, "*", 10); got.Cursor != 0 || len(got.Keys) != 0 {
		t.Fatalf("Scan beyond snapshot = %+v", got)
	}
}

func TestDiscoveryReadsClockOncePerCommandWhenNeeded(t *testing.T) {
	var calls int
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	e := NewWithClock(func(error) {}, func() time.Time { calls++; return now })
	seedDiscovery(t, e, "plain")
	_ = e.Set("timed", "v", ExpiresIn(time.Hour))
	for name, call := range map[string]func(){
		"Keys":   func() { e.Keys("*") },
		"Scan":   func() { e.Scan(0, "*", 10) },
		"DBSize": func() { e.DBSize() },
	} {
		t.Run(name, func(t *testing.T) {
			calls = 0
			call()
			if calls != 1 {
				t.Fatalf("clock calls = %d, want 1", calls)
			}
		})
	}
}

func TestDiscoveryWorksWhileDraining(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "a")
	e.BeginDrain()
	if e.DBSize() != 1 || !slices.Equal(e.Keys("*"), []string{"a"}) {
		t.Fatal("discovery stopped working while draining")
	}
	if got := e.Scan(0, "*", 10); !slices.Equal(got.Keys, []string{"a"}) {
		t.Fatalf("Scan during drain = %+v", got)
	}
}

func TestScanUnderConcurrentMutationMakesSafeProgress(t *testing.T) {
	e := newTestEngine(t)
	for i := 0; i < 200; i++ {
		seedDiscovery(t, e, "key:"+strconv.Itoa(i))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key := "key:" + strconv.Itoa((i+worker)%200)
				if i%2 == 0 {
					_ = e.Set(key, "v", NoExpiry())
				} else {
					_, _ = e.Delete([]string{key})
				}
			}
		}(worker)
	}
	deadline := time.After(3 * time.Second)
	for iteration := 0; iteration < 100; iteration++ {
		cursor := uint64(0)
		for first := true; first || cursor != 0; first = false {
			select {
			case <-deadline:
				close(stop)
				wg.Wait()
				t.Fatal("SCAN did not terminate against a bounded keyspace")
			default:
			}
			page := e.Scan(cursor, "*", 7)
			if page.Cursor != 0 && page.Cursor <= cursor {
				t.Fatalf("cursor did not progress: %d -> %d", cursor, page.Cursor)
			}
			cursor = page.Cursor
		}
	}
	close(stop)
	wg.Wait()
}
