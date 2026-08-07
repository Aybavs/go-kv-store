package store

import "testing"

func TestSetGet(t *testing.T) {
	s := New()
	if _, ok := s.Get("missing"); ok {
		t.Fatal("empty store returned a value")
	}
	s.Set("k", "v")
	got, ok := s.Get("k")
	if !ok || got != "v" {
		t.Fatalf("got (%q, %v), want (\"v\", true)", got, ok)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New()
	s.Set("k", "old")
	s.Set("k", "new")
	if got, _ := s.Get("k"); got != "new" {
		t.Fatalf("got %q, want %q", got, "new")
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Set("k", "v")
	if !s.Delete("k") {
		t.Fatal("Delete on existing key returned false")
	}
	if s.Delete("k") {
		t.Fatal("Delete on missing key returned true")
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

func TestBinarySafeKeysAndValues(t *testing.T) {
	s := New()
	key := "k\x00\r\n"
	val := "v\x00\xff\r\n"
	s.Set(key, val)
	got, ok := s.Get(key)
	if !ok || got != val {
		t.Fatalf("got (%q, %v), want (%q, true)", got, ok, val)
	}
}
