package engine

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var discoverySizes = []int{1_000, 10_000, 100_000}
var discoveryCounts = []uint64{10, 100, 1_000}

type discoveryTraversalResult struct {
	keys      int
	pages     int
	snapshots int64
	filters   int64
	sorts     int64
}

type discoveryPhaseCounter struct {
	snapshots atomic.Int64
	filters   atomic.Int64
	sorts     atomic.Int64
}

func installBenchDiscoveryPhaseCounter(e *Engine) (*discoveryPhaseCounter, func()) {
	counter := &discoveryPhaseCounter{}
	liveKeys := e.scanLiveKeys
	filter := e.scanFilter
	sorter := e.scanSort
	e.scanLiveKeys = func(now time.Time) []string {
		counter.snapshots.Add(1)
		return liveKeys(now)
	}
	e.scanFilter = func(keys []string, pattern string) []string {
		counter.filters.Add(1)
		return filter(keys, pattern)
	}
	e.scanSort = func(keys []string) {
		counter.sorts.Add(1)
		sorter(keys)
	}
	return counter, func() {
		e.scanLiveKeys = liveKeys
		e.scanFilter = filter
		e.scanSort = sorter
	}
}

func (c *discoveryPhaseCounter) result(keys, pages int) discoveryTraversalResult {
	return discoveryTraversalResult{
		keys:      keys,
		pages:     pages,
		snapshots: c.snapshots.Load(),
		filters:   c.filters.Load(),
		sorts:     c.sorts.Load(),
	}
}

func scanBenchDiscoveryTraversal(tb testing.TB, e *Engine, pattern string, count uint64) (int, int) {
	tb.Helper()
	keys := 0
	pages := 0
	cursor := uint64(0)
	for first := true; first || cursor != 0; first = false {
		page, err := e.Scan(ScanRequest{Cursor: cursor, Pattern: pattern, PatternSet: true, Count: count})
		if err != nil {
			tb.Fatalf("Scan(cursor=%d): %v", cursor, err)
		}
		keys += len(page.Keys)
		pages++
		cursor = page.Cursor
	}
	return keys, pages
}

// runBenchDiscoveryTraversal is shared by the benchmark and its ordinary
// harness-invariant test, so a benchmark-only fast path cannot bypass Scan.
func runBenchDiscoveryTraversal(tb testing.TB, e *Engine, pattern string, count uint64) discoveryTraversalResult {
	tb.Helper()
	counter, restore := installBenchDiscoveryPhaseCounter(e)
	defer restore()
	keys, pages := scanBenchDiscoveryTraversal(tb, e, pattern, count)
	return counter.result(keys, pages)
}

func TestBenchScanTraversalCountsInitialWorkOncePerTraversal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     uint64
		wantPages int
	}{
		{name: "count-1", count: 1, wantPages: 11},
		{name: "count-4", count: 4, wantPages: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := benchDiscoveryEngine(t, 11)
			got := runBenchDiscoveryTraversal(t, e, "key:*", tc.count)
			if got.keys != 11 || got.pages != tc.wantPages {
				t.Fatalf("traversal = %d keys over %d pages, want 11 over %d", got.keys, got.pages, tc.wantPages)
			}
			if got.snapshots != 1 || got.filters != 1 || got.sorts != 1 {
				t.Fatalf("initial phases = snapshot:%d filter:%d sort:%d, want one each", got.snapshots, got.filters, got.sorts)
			}
			if stats := e.scanSessions.stats(); stats.active != 0 || stats.retainedBytes != 0 {
				t.Fatalf("manager stats after traversal = %+v, want zero", stats)
			}
		})
	}
}

func benchDiscoveryEngine(tb testing.TB, size int) *Engine {
	tb.Helper()
	return benchDiscoveryEngineWithClock(tb, size, time.Now)
}

func benchDiscoveryEngineWithClock(tb testing.TB, size int, now func() time.Time) *Engine {
	tb.Helper()
	e := NewWithClock(func(err error) { tb.Errorf("unexpected fatal: %v", err) }, now)
	for i := 0; i < size; i++ {
		if err := e.Set("key:"+strconv.Itoa(i), "v", NoExpiry()); err != nil {
			tb.Fatal(err)
		}
	}
	return e
}

func requireBenchSessionStats(tb testing.TB, e *Engine, wantActive int) scanSessionStats {
	tb.Helper()
	stats := e.scanSessions.stats()
	if stats.active != wantActive {
		tb.Fatalf("active scan sessions = %d, want %d (retained bytes %d)", stats.active, wantActive, stats.retainedBytes)
	}
	if wantActive == 0 && stats.retainedBytes != 0 {
		tb.Fatalf("retained scan-session bytes = %d with no active sessions, want 0", stats.retainedBytes)
	}
	if wantActive > 0 && stats.retainedBytes == 0 {
		tb.Fatalf("retained scan-session bytes = 0 with %d active sessions", wantActive)
	}
	return stats
}

func BenchmarkScanSessionCreation(b *testing.B) {
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		name := "keys-" + strconv.Itoa(size)
		b.Run(name+"/snapshot-under-rlock", func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				keys, _ := e.captureLiveKeys()
				b.StopTimer()
				if len(keys) != size {
					b.Fatalf("captured keys = %d, want %d", len(keys), size)
				}
				requireBenchSessionStats(b, e, 0)
			}
		})

		base, _ := e.captureLiveKeys()
		b.Run(name+"/filter-full-match", func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				keys := slices.Clone(base)
				b.StartTimer()
				keys = e.scanFilter(keys, "key:*")
				b.StopTimer()
				if len(keys) != size {
					b.Fatalf("full MATCH retained %d keys, want %d", len(keys), size)
				}
				requireBenchSessionStats(b, e, 0)
				b.ReportMetric(float64(len(keys)), "matching-keys")
			}
		})

		retained := filterSnapshot(slices.Clone(base), "key:*")
		b.Run(name+"/sort-retained-matches", func(b *testing.B) {
			b.ReportAllocs()
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				keys := slices.Clone(retained)
				b.StartTimer()
				e.scanSort(keys)
				b.StopTimer()
				if !sort.StringsAreSorted(keys) {
					b.Fatal("retained MATCH names are not sorted")
				}
				requireBenchSessionStats(b, e, 0)
			}
			b.ReportMetric(float64(len(retained)), "matching-keys")
		})

		for _, count := range discoveryCounts {
			count := count
			b.Run(name+"/count-"+strconv.FormatUint(count, 10)+"/first-page", func(b *testing.B) {
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				terminal := count >= uint64(size)
				var retainedBytes uint64
				for i := 0; i < b.N; i++ {
					e.ClearScanSessions()
					b.StartTimer()
					page, err := e.Scan(ScanRequest{Pattern: "key:*", PatternSet: true, Count: count})
					b.StopTimer()
					if err != nil {
						b.Fatalf("initial Scan: %v", err)
					}
					if len(page.Keys) != min(int(count), size) || (page.Cursor == 0) != terminal {
						b.Fatalf("first page = %d keys, cursor %d; terminal=%v", len(page.Keys), page.Cursor, terminal)
					}
					stats := requireBenchSessionStats(b, e, map[bool]int{true: 0, false: 1}[terminal])
					if i == 0 {
						retainedBytes = stats.retainedBytes
					} else if stats.retainedBytes != retainedBytes {
						b.Fatalf("retained bytes changed from %d to %d", retainedBytes, stats.retainedBytes)
					}
					e.ClearScanSessions()
					requireBenchSessionStats(b, e, 0)
				}
				b.ReportMetric(float64(retainedBytes), "retained-B/session")
				b.ReportMetric(map[bool]float64{true: 1, false: 0}[terminal], "terminal")
			})
		}
	}
}

// Continuation and cleanup setup is intentionally outside the timer: those
// rows isolate one continuation/cleanup operation. Task 13 must use documented
// fixed iteration counts with three repetitions; adaptive calibration would
// mix a variable number of excluded setup operations into otherwise comparable
// rows.
func BenchmarkScanSessionContinuation(b *testing.B) {
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		for _, count := range discoveryCounts {
			count := count
			b.Run("keys-"+strconv.Itoa(size)+"/count-"+strconv.FormatUint(count, 10), func(b *testing.B) {
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				terminal := count >= uint64(size-1)
				var retainedBytes uint64
				for i := 0; i < b.N; i++ {
					e.ClearScanSessions()
					first, err := e.Scan(ScanRequest{Pattern: "key:*", PatternSet: true, Count: 1})
					if err != nil || first.Cursor == 0 {
						b.Fatalf("continuation setup = %+v, %v; want nonterminal page", first, err)
					}
					before := requireBenchSessionStats(b, e, 1)
					b.StartTimer()
					page, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: count})
					b.StopTimer()
					if err != nil {
						b.Fatalf("continuation Scan: %v", err)
					}
					if len(page.Keys) != min(int(count), size-1) || (page.Cursor == 0) != terminal {
						b.Fatalf("continuation = %d keys, cursor %d; terminal=%v", len(page.Keys), page.Cursor, terminal)
					}
					after := requireBenchSessionStats(b, e, map[bool]int{true: 0, false: 1}[terminal])
					if !terminal && after.retainedBytes != before.retainedBytes {
						b.Fatalf("continuation retained bytes = %d, want %d", after.retainedBytes, before.retainedBytes)
					}
					if i == 0 {
						retainedBytes = after.retainedBytes
					}
					e.ClearScanSessions()
					requireBenchSessionStats(b, e, 0)
				}
				b.ReportMetric(float64(retainedBytes), "retained-B/session")
				b.ReportMetric(map[bool]float64{true: 1, false: 0}[terminal], "terminal")
			})
		}
	}
}

func BenchmarkScanSessionTraversal(b *testing.B) {
	if os.Getenv("KV_ENUM_BENCH") != "1" {
		b.Skip("set KV_ENUM_BENCH=1 to run the complete session traversal matrix")
	}
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		for _, count := range discoveryCounts {
			count := count
			b.Run("keys-"+strconv.Itoa(size)+"/count-"+strconv.FormatUint(count, 10), func(b *testing.B) {
				counter, restore := installBenchDiscoveryPhaseCounter(e)
				defer restore()
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				wantPages := (size + int(count) - 1) / int(count)
				lastPages := 0
				for i := 0; i < b.N; i++ {
					e.ClearScanSessions()
					before := counter.result(0, 0)
					b.StartTimer()
					keys, pages := scanBenchDiscoveryTraversal(b, e, "key:*", count)
					b.StopTimer()
					after := counter.result(0, 0)
					if keys != size || pages != wantPages {
						b.Fatalf("traversal = %d keys over %d pages, want %d over %d", keys, pages, size, wantPages)
					}
					if after.snapshots-before.snapshots != 1 || after.filters-before.filters != 1 || after.sorts-before.sorts != 1 {
						b.Fatalf("traversal phases = snapshot:%d filter:%d sort:%d, want one each",
							after.snapshots-before.snapshots, after.filters-before.filters, after.sorts-before.sorts)
					}
					requireBenchSessionStats(b, e, 0)
					lastPages = pages
				}
				b.ReportMetric(float64(lastPages), "pages/op")
				b.ReportMetric(1, "snapshots/op")
				b.ReportMetric(1, "filters/op")
				b.ReportMetric(1, "sorts/op")
			})
		}
	}
}

func BenchmarkScanSessionCleanup(b *testing.B) {
	for _, size := range discoverySizes {
		for _, count := range discoveryCounts {
			count := count
			name := "keys-" + strconv.Itoa(size) + "/count-" + strconv.FormatUint(count, 10)
			b.Run(name+"/completed", func(b *testing.B) {
				e := benchDiscoveryEngine(b, size)
				setupCount := uint64(size) - count
				if setupCount == 0 {
					setupCount = 1
				}
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				var releasedBytes uint64
				for i := 0; i < b.N; i++ {
					e.ClearScanSessions()
					first, err := e.Scan(ScanRequest{Pattern: "key:*", PatternSet: true, Count: setupCount})
					if err != nil || first.Cursor == 0 {
						b.Fatalf("completed-cleanup setup = %+v, %v; want nonterminal page", first, err)
					}
					before := requireBenchSessionStats(b, e, 1)
					b.StartTimer()
					last, err := e.Scan(ScanRequest{Cursor: first.Cursor, Count: count})
					b.StopTimer()
					if err != nil {
						b.Fatalf("completed-cleanup continuation: %v", err)
					}
					if last.Cursor != 0 {
						b.Fatalf("completed-cleanup cursor = %d, want terminal cursor", last.Cursor)
					}
					requireBenchSessionStats(b, e, 0)
					releasedBytes = before.retainedBytes
				}
				b.ReportMetric(float64(releasedBytes), "released-B/session")
			})

			b.Run(name+"/abandoned-exact-ttl", func(b *testing.B) {
				clock := newTestClock()
				e := benchDiscoveryEngineWithClock(b, size, clock.now)
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				var releasedBytes uint64
				for i := 0; i < b.N; i++ {
					e.ClearScanSessions()
					first, err := e.Scan(ScanRequest{Pattern: "key:*", PatternSet: true, Count: 1})
					if err != nil || first.Cursor == 0 {
						b.Fatalf("abandoned-cleanup setup = %+v, %v; want nonterminal page", first, err)
					}
					before := requireBenchSessionStats(b, e, 1)
					clock.advance(scanSessionTTL)
					b.StartTimer()
					_, err = e.Scan(ScanRequest{Cursor: first.Cursor, Count: count})
					b.StopTimer()
					if !errors.Is(err, ErrInvalidCursor) {
						b.Fatalf("continuation at exact TTL error = %v, want %v", err, ErrInvalidCursor)
					}
					requireBenchSessionStats(b, e, 0)
					releasedBytes = before.retainedBytes
				}
				b.ReportMetric(float64(releasedBytes), "released-B/session")
			})
		}
	}
}

func BenchmarkScanSessionConcurrent(b *testing.B) {
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		for _, sessions := range []int{1, 8, 16} {
			sessions := sessions
			b.Run("keys-"+strconv.Itoa(size)+"/sessions-"+strconv.Itoa(sessions), func(b *testing.B) {
				b.ReportAllocs()
				b.StopTimer()
				b.ResetTimer()
				var retainedBytes uint64
				for i := 0; i < b.N; i++ {
					e.ClearScanSessions()
					b.StartTimer()
					for session := 0; session < sessions; session++ {
						page, err := e.Scan(ScanRequest{Pattern: "key:*", PatternSet: true, Count: 1})
						if err != nil {
							b.Fatalf("create active session %d/%d: %v", session+1, sessions, err)
						}
						if page.Cursor == 0 {
							b.Fatalf("create active session %d/%d returned terminal page", session+1, sessions)
						}
					}
					b.StopTimer()
					stats := requireBenchSessionStats(b, e, sessions)
					retainedBytes = stats.retainedBytes
					e.ClearScanSessions()
					requireBenchSessionStats(b, e, 0)
				}
				b.ReportMetric(float64(sessions), "active-sessions/op")
				b.ReportMetric(float64(retainedBytes), "retained-B/op")
				b.ReportMetric(float64(retainedBytes)/float64(sessions), "retained-B/session")
			})
		}
	}
}

type latencyStats struct {
	p50, p95, p99 time.Duration
}

type discoveryLatencyMeasurement struct {
	latencyStats
	samples int
}

func summarizeLatencies(values []time.Duration) latencyStats {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(q float64) time.Duration {
		return values[int(q*float64(len(values)-1))]
	}
	return latencyStats{p50: at(.50), p95: at(.95), p99: at(.99)}
}

func benchDiscoveryLatencyKeys() []string {
	keys := make([]string, 1_000)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	return keys
}

func TestBenchDiscoveryLatencyKeysArePrecomputed(t *testing.T) {
	keys := benchDiscoveryLatencyKeys()
	if len(keys) != 1_000 || keys[0] != "key:0" || keys[999] != "key:999" {
		t.Fatalf("latency keys = len %d, endpoints %q/%q; want 1000 precomputed seeded keys",
			len(keys), firstString(keys), lastString(keys))
	}
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func lastString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[len(values)-1]
}

func measureDiscoveryLatencyFor(
	e *Engine,
	set bool,
	keys []string,
	duration time.Duration,
	onStart func(),
) (discoveryLatencyMeasurement, error) {
	if len(keys) == 0 {
		return discoveryLatencyMeasurement{}, errors.New("latency arm requires precomputed keys")
	}
	values := make([]time.Duration, 0, 1<<20)
	started := time.Now()
	if onStart != nil {
		onStart()
	}
	deadline := started.Add(duration)
	for time.Now().Before(deadline) {
		key := keys[len(values)%len(keys)]
		begin := time.Now()
		if set {
			if err := e.Set(key, "v", NoExpiry()); err != nil {
				return discoveryLatencyMeasurement{}, err
			}
		} else {
			e.Get(key)
		}
		values = append(values, time.Since(begin))
	}
	if len(values) == 0 {
		return discoveryLatencyMeasurement{}, errors.New("latency arm recorded no samples")
	}
	return discoveryLatencyMeasurement{
		latencyStats: summarizeLatencies(values),
		samples:      len(values),
	}, nil
}

func TestBenchDiscoveryLatencyArmRunsForFixedDuration(t *testing.T) {
	e := benchDiscoveryEngine(t, 23)
	keys := benchDiscoveryLatencyKeys()
	const duration = 50 * time.Millisecond
	started := time.Now()
	measurement, err := measureDiscoveryLatencyFor(e, false, keys, duration, nil)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < duration {
		t.Fatalf("latency arm ran for %s, want at least %s", elapsed, duration)
	}
	if measurement.samples == 0 {
		t.Fatal("latency arm recorded no samples")
	}
}

func runBenchDiscoveryInitialCapture(e *Engine, count uint64) error {
	page, err := e.Scan(ScanRequest{Pattern: "key:*", PatternSet: true, Count: count})
	if err != nil {
		e.ClearScanSessions()
		return err
	}
	if page.Cursor == 0 {
		e.ClearScanSessions()
		return fmt.Errorf("initial capture returned terminal page for COUNT %d", count)
	}
	e.ClearScanSessions()
	if stats := e.scanSessions.stats(); stats.active != 0 || stats.retainedBytes != 0 {
		return fmt.Errorf("manager stats after clearing initial capture = %+v, want zero", stats)
	}
	return nil
}

func TestBenchDiscoveryLoadRepeatsInitialCapture(t *testing.T) {
	e := benchDiscoveryEngine(t, 23)
	counter, restore := installBenchDiscoveryPhaseCounter(e)
	defer restore()
	for i := 0; i < 2; i++ {
		if err := runBenchDiscoveryInitialCapture(e, 10); err != nil {
			t.Fatalf("initial capture %d: %v", i+1, err)
		}
	}
	got := counter.result(0, 0)
	if got.snapshots != 2 || got.filters != 2 || got.sorts != 2 {
		t.Fatalf("two background captures = snapshot:%d filter:%d sort:%d, want two each", got.snapshots, got.filters, got.sorts)
	}
	requireBenchSessionStats(t, e, 0)
}

type discoveryCaptureLoad struct {
	start     chan struct{}
	stop      chan struct{}
	done      chan struct{}
	err       chan error
	startOnce sync.Once
	completed atomic.Int64
}

func TestBenchDiscoveryLoadWaitsAtCaptureStart(t *testing.T) {
	e := benchDiscoveryEngine(t, 23)
	liveKeys := e.scanLiveKeys
	captureEntered := make(chan struct{})
	releaseCapture := make(chan struct{})
	var entered sync.Once
	e.scanLiveKeys = func(now time.Time) []string {
		entered.Do(func() { close(captureEntered) })
		<-releaseCapture
		return liveKeys(now)
	}

	type startResult struct {
		load *discoveryCaptureLoad
		err  error
	}
	returned := make(chan startResult, 1)
	go func() {
		load, err := startBenchDiscoveryCaptureLoad(e, 10)
		returned <- startResult{load: load, err: err}
	}()

	select {
	case result := <-returned:
		if result.err != nil {
			t.Fatalf("prepare capture load: %v", result.err)
		}
		if err := result.load.stopAndWait(); err != nil {
			t.Fatalf("stop prepared capture load: %v", err)
		}
	case <-captureEntered:
		close(releaseCapture)
		result := <-returned
		if result.err == nil {
			if err := result.load.stopAndWait(); err != nil {
				t.Errorf("stop capture load after failed invariant: %v", err)
			}
		}
		t.Fatal("capture began before load preparation returned; foreground cannot start at capture onset")
	}
}

func TestBenchDiscoveryLoadCountsCompletedCapturesAndStopsCleanly(t *testing.T) {
	e := benchDiscoveryEngine(t, 23)
	liveKeys := e.scanLiveKeys
	captureStarted := make(chan struct{}, 3)
	e.scanLiveKeys = func(now time.Time) []string {
		select {
		case captureStarted <- struct{}{}:
		default:
		}
		return liveKeys(now)
	}

	load, err := startBenchDiscoveryCaptureLoad(e, 10)
	if err != nil {
		t.Fatalf("prepare capture load: %v", err)
	}
	load.begin()
	for range 3 {
		<-captureStarted
	}
	if err := load.stopAndWait(); err != nil {
		t.Fatalf("stop capture load: %v", err)
	}
	if got := load.completed.Load(); got < 3 {
		t.Fatalf("completed captures = %d, want at least 3", got)
	}
	requireBenchSessionStats(t, e, 0)
}

func startBenchDiscoveryCaptureLoad(e *Engine, count uint64) (*discoveryCaptureLoad, error) {
	load := &discoveryCaptureLoad{
		start: make(chan struct{}),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		err:   make(chan error, 1),
	}
	ready := make(chan struct{})
	go func() {
		defer close(load.done)
		defer e.ClearScanSessions()
		close(ready)
		select {
		case <-load.start:
		case <-load.stop:
			return
		}
		for {
			err := runBenchDiscoveryInitialCapture(e, count)
			if err != nil {
				load.err <- err
				return
			}
			load.completed.Add(1)
			select {
			case <-load.stop:
				return
			default:
			}
		}
	}()
	<-ready
	return load, nil
}

func (l *discoveryCaptureLoad) begin() {
	l.startOnce.Do(func() { close(l.start) })
}

func (l *discoveryCaptureLoad) stopAndWait() error {
	close(l.stop)
	<-l.done
	select {
	case err := <-l.err:
		return err
	default:
		return nil
	}
}

const (
	benchDiscoveryContentionDuration = 150 * time.Millisecond
	benchDiscoveryMinCaptures        = int64(3)
)

type discoveryContentionArm struct {
	discoveryLatencyMeasurement
	completedCaptures int64
}

func measureBenchDiscoveryContentionArm(
	e *Engine,
	count uint64,
	set bool,
	keys []string,
	loaded bool,
) (discoveryContentionArm, error) {
	if !loaded {
		measurement, err := measureDiscoveryLatencyFor(e, set, keys, benchDiscoveryContentionDuration, nil)
		return discoveryContentionArm{discoveryLatencyMeasurement: measurement}, err
	}

	load, err := startBenchDiscoveryCaptureLoad(e, count)
	if err != nil {
		return discoveryContentionArm{}, err
	}
	measurement, operationErr := measureDiscoveryLatencyFor(e, set, keys, benchDiscoveryContentionDuration, load.begin)
	loadErr := load.stopAndWait()
	if operationErr != nil || loadErr != nil {
		return discoveryContentionArm{}, errors.Join(operationErr, loadErr)
	}
	return discoveryContentionArm{
		discoveryLatencyMeasurement: measurement,
		completedCaptures:           load.completed.Load(),
	}, nil
}

func TestBenchDiscoveryContention(t *testing.T) {
	if os.Getenv("KV_ENUM_BENCH") != "1" {
		t.Skip("set KV_ENUM_BENCH=1 to measure GET/SET latency during repeated initial snapshot capture")
	}
	e := benchDiscoveryEngine(t, 100_000)
	keys := benchDiscoveryLatencyKeys()
	for _, count := range discoveryCounts {
		for _, operation := range []struct {
			name string
			set  bool
		}{
			{"GET", false},
			{"SET", true},
		} {
			for repetition := 1; repetition <= 5; repetition++ {
				order := "baseline-first"
				arms := []bool{false, true}
				if repetition%2 == 0 {
					order = "load-first"
					arms = []bool{true, false}
				}
				for _, loaded := range arms {
					requireBenchSessionStats(t, e, 0)
					result, err := measureBenchDiscoveryContentionArm(e, count, operation.set, keys, loaded)
					if err != nil {
						t.Fatalf("rep=%d operation=%s COUNT=%d loaded=%v: %v", repetition, operation.name, count, loaded, err)
					}
					requireBenchSessionStats(t, e, 0)
					if loaded {
						if result.completedCaptures < benchDiscoveryMinCaptures {
							t.Fatalf("rep=%d operation=%s COUNT=%d completed captures = %d, want at least %d",
								repetition, operation.name, count, result.completedCaptures, benchDiscoveryMinCaptures)
						}
						t.Logf("rep=%d order=%s operation=%s COUNT=%d arm=load background=repeated-initial-capture samples=%d completed-captures=%d p50=%s p95=%s p99=%s",
							repetition, order, operation.name, count, result.samples, result.completedCaptures,
							result.p50, result.p95, result.p99)
						continue
					}
					t.Logf("rep=%d order=%s operation=%s COUNT=%d arm=baseline background=none samples=%d completed-captures=0 p50=%s p95=%s p99=%s",
						repetition, order, operation.name, count, result.samples, result.p50, result.p95, result.p99)
				}
			}
		}
	}
}
