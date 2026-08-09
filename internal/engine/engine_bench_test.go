package engine

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
)

func benchEngine(b *testing.B) *Engine {
	b.Helper()
	return New(func(err error) { b.Fatalf("unexpected fatal: %v", err) })
}

func BenchmarkEngineSet(b *testing.B) {
	e := benchEngine(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Set("key", "value", NoExpiry()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEngineGet(b *testing.B) {
	e := benchEngine(b)
	_ = e.Set("key", "value", NoExpiry())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get("key")
	}
}

// benchKeys returns keys built once, outside the measured loop.
//
// The parallel benchmarks used to build their keys inside it, which put
// strconv.Itoa and an allocation into the number the sharding decision is
// supposed to be made against. Measured at roughly 17 ns of 121 — not the
// dominant cost, but not nothing either, and the wrong thing entirely.
func benchKeys(b *testing.B, e *Engine, n int) []string {
	b.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key" + strconv.Itoa(i)
		if err := e.Set(keys[i], "value", NoExpiry()); err != nil {
			b.Fatal(err)
		}
	}
	return keys
}

// BenchmarkEngineParallelReads measures RWMutex read scaling, and it is the
// number ADR-0001's sharding question turns on.
//
// Run it across -cpu values rather than reading the single figure: reads do not
// scale, they invert. RLock increments a counter shared by every reader, so the
// cache line holding it ping-pongs between cores, and total throughput falls as
// cores are added. On this machine 1 core sustains ~44 M ops/s and 10 cores
// sustain ~10 M.
//
// That is a property of sync.RWMutex, not a defect here, and it is not on its
// own an argument for sharding: this loop does nothing but lock, read and
// unlock, while a real request also parses a command and makes syscalls. What
// fraction of a real request the lock actually is remains unmeasured until the
// end-to-end work in v0.5.
func BenchmarkEngineParallelReads(b *testing.B) {
	e := benchEngine(b)
	keys := benchKeys(b, e, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.Get(keys[i%len(keys)])
			i++
		}
	})
}

// BenchmarkEngineParallelMixed is the write-contention case: 1 writer per 9
// readers, all on the same lock.
func BenchmarkEngineParallelMixed(b *testing.B) {
	e := benchEngine(b)
	keys := benchKeys(b, e, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := keys[i%len(keys)]
			if i%10 == 0 {
				if err := e.Set(k, "value", NoExpiry()); err != nil {
					b.Error(err)
					return
				}
			} else {
				e.Get(k)
			}
			i++
		}
	})
}

// discardFile is the cheapest possible log target: it accepts everything and
// syncs instantly. It isolates what the commit path itself costs from what the
// disk costs, which are different questions and answered by different
// benchmarks — this one and BenchmarkEngineSetLoggedToDisk below.
type discardFile struct{}

func (discardFile) Write(p []byte) (int, error) { return len(p), nil }
func (discardFile) Sync() error                 { return nil }
func (discardFile) Close() error                { return nil }

func benchLoggedEngine(b *testing.B, f aof.File, p aof.Policy) *Engine {
	b.Helper()
	e := New(func(err error) { b.Fatalf("unexpected fatal: %v", err) })
	l := aof.Open(f, p, func(err error) { b.Fatalf("unexpected fatal: %v", err) })
	e.AttachLog(l, p)
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = l.Close(ctx)
	})
	return e
}

// BenchmarkEngineSetLoggedEverysec measures the commit path with a log attached
// but no real device underneath: derive the effect, append it to the buffer,
// apply to memory, then wait for the writer to have written. The wait is what
// separates this from the unlogged case.
func BenchmarkEngineSetLoggedEverysec(b *testing.B) {
	e := benchLoggedEngine(b, discardFile{}, aof.EverySec)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Set("key", "value", NoExpiry()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEngineSetLoggedAlways adds waiting for a Sync. Against discardFile
// that Sync is free, so the difference from the everysec figure above is the
// cost of the extra round trip through the writer, not the cost of fsync.
func BenchmarkEngineSetLoggedAlways(b *testing.B) {
	e := benchLoggedEngine(b, discardFile{}, aof.Always)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Set("key", "value", NoExpiry()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEngineSetLoggedToDisk is the honest one, and the slowest by orders
// of magnitude: a real file with a real fsync per batch. It measures the
// device, not this code, and is here so the other figures cannot be mistaken
// for what durability actually costs.
func BenchmarkEngineSetLoggedToDisk(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "bench.aof")
	if err != nil {
		b.Fatalf("temp file: %v", err)
	}
	e := benchLoggedEngine(b, f, aof.Always)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Set("key", "value", NoExpiry()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEngineSetLoggedToDiskParallel is the measurement group commit exists
// for. Its serial counterpart pays one fsync per write; here every writer
// waiting on the same Sync shares that syscall, so the per-operation cost falls
// by roughly the number of writers in flight. Comparing the two is the only way
// to see that the claim is real rather than architectural decoration.
func BenchmarkEngineSetLoggedToDiskParallel(b *testing.B) {
	f, err := os.CreateTemp(b.TempDir(), "bench.aof")
	if err != nil {
		b.Fatalf("temp file: %v", err)
	}
	e := benchLoggedEngine(b, f, aof.Always)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := e.Set("key"+strconv.Itoa(i%64), "value", NoExpiry()); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

// BenchmarkEngineMGet measures the multi-key read against four present keys.
// The keys are built outside the loop for the reason benchKeys documents.
func BenchmarkEngineMGet(b *testing.B) {
	e := benchEngine(b)
	keys := benchKeys(b, e, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.MGet(keys)
	}
}

// BenchmarkEngineIncr is the first read-modify-write path in the project: it
// reads the value, parses it, formats the result and writes it back, all under
// one lock acquisition. Compare against BenchmarkEngineSet, which does the same
// write with none of the read side.
func BenchmarkEngineIncr(b *testing.B) {
	e := benchEngine(b)
	if err := e.Set("counter", "0", NoExpiry()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.IncrBy("counter", 1); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEngineIncrWithTTL is the same path with a deadline to carry, which
// adds the expires-map lookup that produces the record's PXAT.
func BenchmarkEngineIncrWithTTL(b *testing.B) {
	e := benchEngine(b)
	if err := e.Set("counter", "0", ExpiresIn(time.Hour)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.IncrBy("counter", 1); err != nil {
			b.Fatal(err)
		}
	}
}
