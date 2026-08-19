package store

import (
	"strconv"
	"testing"
	"time"
)

// A fixed instant: the benchmarks measure map work, and calling time.Now in the
// loop would measure the clock as well.
var benchNow = time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

func BenchmarkStoreSet(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key", "value", time.Time{}, false)
	}
}

func BenchmarkStoreSetWithTTL(b *testing.B) {
	s := New()
	deadline := benchNow.Add(time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key", "value", deadline, true)
	}
}

// BenchmarkStoreGetHit is the number to compare against the v0.1.0 baseline.
// A key with no TTL still consults expires, so this is where the second map
// lookup would show up if it costs the common path anything.
func BenchmarkStoreGetHit(b *testing.B) {
	s := New()
	s.Set("key", "value", time.Time{}, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key", benchNow)
	}
}

func BenchmarkStoreGetHitWithTTL(b *testing.B) {
	s := New()
	s.Set("key", "value", benchNow.Add(time.Hour), true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key", benchNow)
	}
}

func BenchmarkStoreGetMiss(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("absent", benchNow)
	}
}

func BenchmarkStoreLiveKeys(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			s := New()
			for i := 0; i < size; i++ {
				s.Set("key:"+strconv.Itoa(i), "v", time.Time{}, false)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := s.LiveKeys(benchNow); len(got) != size {
					b.Fatalf("LiveKeys returned %d keys, want %d", len(got), size)
				}
			}
		})
	}
}
