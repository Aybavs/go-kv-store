package engine

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
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

func TestScanCursorZeroCapturesFilteredSortedSnapshot(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "user:z", "other", "user:a", "user:m")

	first, err := e.Scan(ScanRequest{Pattern: "user:*", PatternSet: true, Count: 2})
	if err != nil {
		t.Fatalf("initial Scan: %v", err)
	}
	if first.Cursor == 0 || !slices.Equal(first.Keys, []string{"user:a", "user:m"}) {
		t.Fatalf("initial page = %+v, want nonzero cursor and [user:a user:m]", first)
	}
	last, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: 2})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if last.Cursor != 0 || !slices.Equal(last.Keys, []string{"user:z"}) {
		t.Fatalf("last page = %+v, want cursor 0 and [user:z]", last)
	}
}

func TestScanStableDatasetReturnsEachCapturedMatchExactlyOnce(t *testing.T) {
	e := newTestEngine(t)
	wantKeys := make(map[string]struct{}, 37)
	for i := 0; i < 37; i++ {
		key := "user:" + strconv.Itoa(i)
		seedDiscovery(t, e, key)
		wantKeys[key] = struct{}{}
	}
	seedDiscovery(t, e, "other")

	seenKeys := map[string]int{}
	seenCursors := map[uint64]struct{}{}
	req := ScanRequest{Pattern: "user:*", PatternSet: true, Count: 4}
	for {
		page, err := e.Scan(req)
		if err != nil {
			t.Fatalf("Scan(%+v): %v", req, err)
		}
		for _, key := range page.Keys {
			seenKeys[key]++
		}
		if page.Cursor == 0 {
			break
		}
		if _, exists := seenCursors[page.Cursor]; exists {
			t.Fatalf("cursor %d repeated before completion", page.Cursor)
		}
		seenCursors[page.Cursor] = struct{}{}
		req = ScanRequest{Cursor: page.Cursor, Count: 4}
	}
	if len(seenKeys) != 37 {
		t.Fatalf("full traversal returned %d matches, want 37", len(seenKeys))
	}
	for key, count := range seenKeys {
		if count != 1 {
			t.Errorf("%q returned %d times", key, count)
		}
		if _, wanted := wantKeys[key]; !wanted {
			t.Errorf("SCAN returned uncaptured or nonmatching key %q", key)
		}
	}
	for key := range wantKeys {
		if seenKeys[key] != 1 {
			t.Errorf("captured matching key %q returned %d times, want once", key, seenKeys[key])
		}
	}
}

func TestScanSnapshotMembershipAndCurrentValuesSurviveMutation(t *testing.T) {
	e, clock := newClockedEngine(t)
	seedDiscovery(t, e, "alpha", "bravo", "charlie", "delta")

	first, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatalf("initial Scan: %v", err)
	}
	if first.Cursor == 0 || !slices.Equal(first.Keys, []string{"alpha"}) {
		t.Fatalf("first page = %+v, want nonzero cursor and [alpha]", first)
	}

	seedDiscovery(t, e, "aardvark")
	if _, err := e.Delete([]string{"bravo"}); err != nil {
		t.Fatalf("Delete(bravo): %v", err)
	}
	if applied, err := e.Expire("charlie", time.Second); err != nil || !applied {
		t.Fatalf("Expire(charlie) = (%v, %v), want (true, nil)", applied, err)
	}
	clock.advance(time.Second)
	if err := e.Set("delta", "new-value", NoExpiry()); err != nil {
		t.Fatalf("Set(delta): %v", err)
	}

	if value, ok := e.Get("delta"); !ok || value != "new-value" {
		t.Fatalf("GET delta = (%q, %v), want current value", value, ok)
	}
	for _, key := range []string{"bravo", "charlie"} {
		if value, ok := e.Get(key); ok {
			t.Errorf("GET %s = (%q, true), want missing after mutation", key, value)
		}
	}

	keys := slices.Clone(first.Keys)
	cursor := first.Cursor
	for cursor != 0 {
		page, err := e.Scan(ScanRequest{Cursor: cursor, Count: 2})
		if err != nil {
			t.Fatalf("continuation %d: %v", cursor, err)
		}
		keys = append(keys, page.Keys...)
		cursor = page.Cursor
	}
	if want := []string{"alpha", "bravo", "charlie", "delta"}; !slices.Equal(keys, want) {
		t.Fatalf("captured traversal = %q, want %q; inserts must be absent and captured deleted/expired names retained", keys, want)
	}
}

func TestScanContinuationDoesNotRepeatSnapshotWorkOrOverlapLocks(t *testing.T) {
	baseTime := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	phase := "setup"
	clockCalls := 0
	var e *Engine
	var manager *scanSessionManager
	clock := func() time.Time {
		clockCalls++
		switch phase {
		case "initial":
			if e.mu.TryLock() {
				e.mu.Unlock()
				t.Error("initial SCAN read its capture clock outside the data read lock")
			}
			if !manager.mu.TryLock() {
				t.Error("initial SCAN read its capture clock while holding the manager lock")
			} else {
				manager.mu.Unlock()
			}
		case "continuation":
			if !e.mu.TryLock() {
				t.Error("continuation read its session clock while holding the data lock")
			} else {
				e.mu.Unlock()
			}
			if !manager.mu.TryLock() {
				t.Error("continuation read its session clock while holding the manager lock")
			} else {
				manager.mu.Unlock()
			}
		}
		return baseTime
	}
	e = NewWithClock(func(err error) { t.Errorf("unexpected fatal: %v", err) }, clock)
	seedDiscovery(t, e, "keep:c", "drop", "keep:a", "keep:b")

	var token uint64 = 100
	manager = newScanSessionManager(clock, func() (uint64, error) {
		if manager.mu.TryLock() {
			manager.mu.Unlock()
			t.Error("token generation ran without the manager lock")
		}
		if !e.mu.TryLock() {
			t.Error("manager token generation overlapped the data lock")
		} else {
			e.mu.Unlock()
		}
		token++
		return token, nil
	}, defaultScanSessionLimits())
	e.scanSessions = manager

	liveCalls, filterCalls, sortCalls := 0, 0, 0
	filterCompleted := false
	liveKeys := e.scanLiveKeys
	filter := e.scanFilter
	sorter := e.scanSort
	e.scanLiveKeys = func(now time.Time) []string {
		liveCalls++
		if e.mu.TryLock() {
			e.mu.Unlock()
			t.Error("LiveKeys ran outside the data read lock")
		}
		if !manager.mu.TryLock() {
			t.Error("LiveKeys overlapped the manager lock")
		} else {
			manager.mu.Unlock()
		}
		return liveKeys(now)
	}
	e.scanFilter = func(keys []string, pattern string) []string {
		filterCalls++
		if !e.mu.TryLock() {
			t.Error("filtering overlapped the data lock")
		} else {
			e.mu.Unlock()
		}
		if !manager.mu.TryLock() {
			t.Error("filtering overlapped the manager lock")
		} else {
			manager.mu.Unlock()
		}
		filtered := filter(keys, pattern)
		filterCompleted = true
		return filtered
	}
	e.scanSort = func(keys []string) {
		sortCalls++
		if !filterCompleted {
			t.Error("sorting ran before MATCH filtering completed")
		}
		if !e.mu.TryLock() {
			t.Error("sorting overlapped the data lock")
		} else {
			e.mu.Unlock()
		}
		if !manager.mu.TryLock() {
			t.Error("sorting overlapped the manager lock")
		} else {
			manager.mu.Unlock()
		}
		sorter(keys)
	}

	phase = "initial"
	clockCalls = 0
	first, err := e.Scan(ScanRequest{Pattern: "keep:*", PatternSet: true, Count: 1})
	if err != nil {
		t.Fatalf("initial Scan: %v", err)
	}
	if first.Cursor == 0 || !slices.Equal(first.Keys, []string{"keep:a"}) {
		t.Fatalf("first page = %+v, want nonzero cursor and [keep:a]", first)
	}
	if clockCalls != 1 || liveCalls != 1 || filterCalls != 1 || sortCalls != 1 {
		t.Fatalf("initial work = clock:%d live:%d filter:%d sort:%d, want one each", clockCalls, liveCalls, filterCalls, sortCalls)
	}

	phase = "continuation"
	clockCalls = 0
	second, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: 1})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if second.Cursor == 0 || !slices.Equal(second.Keys, []string{"keep:b"}) {
		t.Fatalf("second page = %+v, want replacement cursor and [keep:b]", second)
	}
	if clockCalls != 1 || liveCalls != 1 || filterCalls != 1 || sortCalls != 1 {
		t.Fatalf("work after continuation = clock:%d live:%d filter:%d sort:%d, want only one new session clock read", clockCalls, liveCalls, filterCalls, sortCalls)
	}
}

func TestScanCountMayChangeBetweenPages(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "a", "b", "c", "d", "e", "f")

	first, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	last, err := e.Scan(ScanRequest{Cursor: second.Cursor, Count: 99})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first.Keys, []string{"a"}) || !slices.Equal(second.Keys, []string{"b", "c", "d"}) ||
		last.Cursor != 0 || !slices.Equal(last.Keys, []string{"e", "f"}) {
		t.Fatalf("COUNT-changing pages = first:%+v second:%+v last:%+v", first, second, last)
	}
}

func TestScanRejectsUnknownExpiredCompletedAndConsumedTokens(t *testing.T) {
	e, clock := newClockedEngine(t)
	seedDiscovery(t, e, "a", "b", "c")

	if _, err := e.Scan(ScanRequest{Cursor: 999, Count: 1}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("unknown cursor error = %v, want %v", err, ErrInvalidCursor)
	}

	first, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: 1}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("consumed cursor error = %v, want %v", err, ErrInvalidCursor)
	}
	if _, err := e.Scan(ScanRequest{Cursor: second.Cursor, Count: 1}); err != nil {
		t.Fatalf("terminal continuation: %v", err)
	}
	if _, err := e.Scan(ScanRequest{Cursor: second.Cursor, Count: 1}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("completed cursor error = %v, want %v", err, ErrInvalidCursor)
	}

	expiring, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(scanSessionTTL)
	if _, err := e.Scan(ScanRequest{Cursor: expiring.Cursor, Count: 1}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired cursor error = %v, want %v", err, ErrInvalidCursor)
	}
}

func TestScanPropagatesUnexpectedSessionManagerErrors(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "a", "b")
	tokenErr := errors.New("token source failed")
	e.scanSessions = newScanSessionManager(e.now, func() (uint64, error) { return 0, tokenErr }, defaultScanSessionLimits())

	if _, err := e.Scan(ScanRequest{Count: 1}); !errors.Is(err, tokenErr) {
		t.Fatalf("Scan error = %v, want token-source error", err)
	}
}

func TestClearScanSessionsReleasesManagerResources(t *testing.T) {
	e := newTestEngine(t)
	seedDiscovery(t, e, "a", "b", "c")
	a, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.scanSessions.stats(); got.active != 2 || got.retainedBytes == 0 {
		t.Fatalf("stats before clear = %+v, want two retained sessions", got)
	}

	e.ClearScanSessions()

	if got := e.scanSessions.stats(); got.active != 0 || got.retainedBytes != 0 {
		t.Fatalf("stats after clear = %+v, want zero", got)
	}
	for _, cursor := range []uint64{a.Cursor, b.Cursor} {
		if _, err := e.Scan(ScanRequest{Cursor: cursor, Count: 1}); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("cleared cursor %d error = %v, want %v", cursor, err, ErrInvalidCursor)
		}
	}
}

func TestFilterSnapshotClearsDiscardedTrailingReferences(t *testing.T) {
	keys := []string{"drop:first", "keep", "drop:last"}
	filtered := filterSnapshot(keys, "keep")
	if !slices.Equal(filtered, []string{"keep"}) {
		t.Fatalf("filtered keys = %q, want [keep]", filtered)
	}
	for i, key := range keys[len(filtered):] {
		if key != "" {
			t.Errorf("discarded backing slot %d retains %q, want empty string", i+len(filtered), key)
		}
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
		"Scan":   func() { _, _ = e.Scan(ScanRequest{Count: 10}) },
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
	got, err := e.Scan(ScanRequest{Count: 10})
	if err != nil {
		t.Fatalf("Scan during drain: %v", err)
	}
	if !slices.Equal(got.Keys, []string{"a"}) {
		t.Fatalf("Scan during drain = %+v", got)
	}
}

func TestDiscoveryDoesNotAppendAOFRecords(t *testing.T) {
	f := &recordingFile{}
	e, _, _ := newLoggedEngine(t, f, aof.EverySec)
	seedDiscovery(t, e, "a", "b")
	before := len(f.records(t))
	_ = e.Keys("*")
	first, err := e.Scan(ScanRequest{Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.Cursor == 0 {
		t.Fatal("SCAN did not retain a session; AOF assertion measured only a one-page read")
	}
	if _, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: 1}); err != nil {
		t.Fatal(err)
	}
	_ = e.DBSize()
	after := len(f.records(t))
	if after != before {
		t.Fatalf("discovery appended %d records, want 0", after-before)
	}
}

func TestConcurrentScanSessionsAndGetSetRemainIsolated(t *testing.T) {
	e := newTestEngine(t)
	wantA := make([]string, 0, 40)
	wantB := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		a := fmt.Sprintf("a:%02d", i)
		b := fmt.Sprintf("b:%02d", i)
		wantA = append(wantA, a)
		wantB = append(wantB, b)
		seedDiscovery(t, e, a, b)
	}

	firstA, err := e.Scan(ScanRequest{Pattern: "a:*", PatternSet: true, Count: 1})
	if err != nil {
		t.Fatal(err)
	}
	firstB, err := e.Scan(ScanRequest{Pattern: "b:*", PatternSet: true, Count: 1})
	if err != nil {
		t.Fatal(err)
	}

	type traversal struct {
		keys []string
		err  error
	}
	collect := func(first ScanResult) traversal {
		keys := slices.Clone(first.Keys)
		cursor := first.Cursor
		for cursor != 0 {
			page, err := e.Scan(ScanRequest{Cursor: cursor, Count: 3})
			if err != nil {
				return traversal{err: err}
			}
			keys = append(keys, page.Keys...)
			cursor = page.Cursor
		}
		return traversal{keys: keys}
	}

	start := make(chan struct{})
	traversals := make(chan traversal, 2)
	mutatorErrors := make(chan error, 4)
	var wg sync.WaitGroup
	for _, first := range []ScanResult{firstA, firstB} {
		wg.Add(1)
		go func(first ScanResult) {
			defer wg.Done()
			<-start
			traversals <- collect(first)
		}(first)
	}
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("a:%02d", (i+worker)%len(wantA))
				value := strconv.Itoa(worker*500 + i)
				if err := e.Set(key, value, NoExpiry()); err != nil {
					mutatorErrors <- err
					return
				}
				if _, ok := e.Get(key); !ok {
					mutatorErrors <- fmt.Errorf("GET %q became missing", key)
					return
				}
			}
			mutatorErrors <- nil
		}(worker)
	}
	close(start)
	wg.Wait()

	for i := 0; i < 4; i++ {
		if err := <-mutatorErrors; err != nil {
			t.Fatal(err)
		}
	}
	gotA, gotB := <-traversals, <-traversals
	if gotA.err != nil || gotB.err != nil {
		t.Fatalf("concurrent traversals failed: %v, %v", gotA.err, gotB.err)
	}
	if slices.Equal(gotA.keys, wantB) && slices.Equal(gotB.keys, wantA) {
		gotA, gotB = gotB, gotA
	}
	if !slices.Equal(gotA.keys, wantA) || !slices.Equal(gotB.keys, wantB) {
		t.Fatalf("isolated traversals = A:%q B:%q", gotA.keys, gotB.keys)
	}
}
