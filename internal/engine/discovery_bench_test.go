package engine

import (
	"os"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

var discoverySizes = []int{1_000, 10_000, 100_000}
var discoveryCounts = []uint64{10, 100, 1_000}

func benchDiscoveryEngine(tb testing.TB, size int) *Engine {
	tb.Helper()
	e := New(func(err error) { tb.Fatalf("unexpected fatal: %v", err) })
	for i := 0; i < size; i++ {
		if err := e.Set("key:"+strconv.Itoa(i), "v", NoExpiry()); err != nil {
			tb.Fatal(err)
		}
	}
	return e
}

func BenchmarkDiscoveryPhases(b *testing.B) {
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		b.Run(strconv.Itoa(size)+"/snapshot-under-rlock", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := e.snapshotLiveKeys(); len(got) != size {
					b.Fatalf("snapshot = %d, want %d", len(got), size)
				}
			}
		})

		base := e.snapshotLiveKeys()
		b.Run(strconv.Itoa(size)+"/sort", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				keys := slices.Clone(base)
				b.StartTimer()
				sortSnapshot(keys)
			}
		})

		sort.Strings(base)
		for _, count := range discoveryCounts {
			count := count
			b.Run(strconv.Itoa(size)+"/filter-"+strconv.FormatUint(count, 10), func(b *testing.B) {
				end := min(int(count), len(base))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					candidates := slices.Clone(base[:end])
					b.StartTimer()
					if got := filterSnapshot(candidates, "key:*7*"); len(got) > end {
						b.Fatal("filter grew the candidate page")
					}
				}
			})
		}
	}
}

func BenchmarkEngineScanPage(b *testing.B) {
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		for _, count := range discoveryCounts {
			b.Run(strconv.Itoa(size)+"/count-"+strconv.FormatUint(count, 10), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = e.Scan(0, "key:*", count)
				}
			})
		}
	}
}

func BenchmarkEngineScanTraversal(b *testing.B) {
	if os.Getenv("KV_ENUM_BENCH") != "1" {
		b.Skip("set KV_ENUM_BENCH=1; complete 100k/count-10 traversal is intentionally expensive")
	}
	for _, size := range discoverySizes {
		e := benchDiscoveryEngine(b, size)
		for _, count := range discoveryCounts {
			b.Run(strconv.Itoa(size)+"/count-"+strconv.FormatUint(count, 10), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cursor := uint64(0)
					for first := true; first || cursor != 0; first = false {
						cursor = e.Scan(cursor, "key:*", count).Cursor
					}
				}
			})
		}
	}
}

type latencyStats struct {
	p50, p95, p99 time.Duration
}

func summarizeLatencies(values []time.Duration) latencyStats {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(q float64) time.Duration {
		return values[int(q*float64(len(values)-1))]
	}
	return latencyStats{p50: at(.50), p95: at(.95), p99: at(.99)}
}

func measureDiscoveryLatency(t *testing.T, e *Engine, count uint64, scan bool, set bool) latencyStats {
	t.Helper()
	stop := make(chan struct{})
	started := make(chan struct{})
	var wg sync.WaitGroup
	if scan {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var once sync.Once
			for {
				cursor := uint64(0)
				for first := true; first || cursor != 0; first = false {
					page := e.Scan(cursor, "*", count)
					once.Do(func() { close(started) })
					cursor = page.Cursor
					select {
					case <-stop:
						return
					default:
					}
				}
			}
		}()
		<-started
	} else {
		close(started)
	}

	const operations = 5_000
	values := make([]time.Duration, operations)
	for i := 0; i < operations; i++ {
		begin := time.Now()
		key := "key:" + strconv.Itoa(i%1_000)
		if set {
			if err := e.Set(key, "v", NoExpiry()); err != nil {
				t.Fatal(err)
			}
		} else {
			e.Get(key)
		}
		values[i] = time.Since(begin)
	}
	close(stop)
	wg.Wait()
	return summarizeLatencies(values)
}

func TestBenchDiscoveryContention(t *testing.T) {
	if os.Getenv("KV_ENUM_BENCH") != "1" {
		t.Skip("set KV_ENUM_BENCH=1 to measure GET/SET latency during complete scans")
	}
	e := benchDiscoveryEngine(t, 100_000)
	for _, operation := range []struct {
		name string
		set  bool
	}{
		{"GET", false},
		{"SET", true},
	} {
		for repetition := 1; repetition <= 5; repetition++ {
			for _, count := range discoveryCounts {
				baseline := measureDiscoveryLatency(t, e, count, false, operation.set)
				t.Logf("rep=%d %s COUNT %d baseline: p50=%s p95=%s p99=%s",
					repetition, operation.name, count, baseline.p50, baseline.p95, baseline.p99)
				got := measureDiscoveryLatency(t, e, count, true, operation.set)
				t.Logf("rep=%d %s COUNT %d with SCAN: p50=%s p95=%s p99=%s",
					repetition, operation.name, count, got.p50, got.p95, got.p99)
			}
		}
	}
}
