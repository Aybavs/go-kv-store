package store

import "testing"

func BenchmarkStoreSet(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set("key", "value")
	}
}

func BenchmarkStoreGetHit(b *testing.B) {
	s := New()
	s.Set("key", "value")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

func BenchmarkStoreGetMiss(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("absent")
	}
}
