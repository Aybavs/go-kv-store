package engine

import (
	"strconv"
	"testing"
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

// BenchmarkEngineParallelReads measures RWMutex read scaling — the number the
// v0.5 sharding decision will be made against.
func BenchmarkEngineParallelReads(b *testing.B) {
	e := benchEngine(b)
	for i := 0; i < 1000; i++ {
		_ = e.Set("key"+strconv.Itoa(i), "value", NoExpiry())
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.Get("key" + strconv.Itoa(i%1000))
			i++
		}
	})
}

// BenchmarkEngineParallelMixed is the write-contention case: 1 writer per 9
// readers, all on the same lock.
func BenchmarkEngineParallelMixed(b *testing.B) {
	e := benchEngine(b)
	for i := 0; i < 1000; i++ {
		_ = e.Set("key"+strconv.Itoa(i), "value", NoExpiry())
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := "key" + strconv.Itoa(i%1000)
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
