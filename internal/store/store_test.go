package store

import (
	"testing"
	"time"
)

// Every test here runs against a fixed instant and explicit offsets. Nothing
// sleeps and nothing reads the clock, which is the whole reason time is a
// parameter of this package rather than something it fetches.
var base = time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return base.Add(d) }

// noTTL and withTTL keep the Set call sites readable; the boolean alone at a
// call site says nothing about which way round it goes.
func setNoTTL(s *Store, key, value string) {
	s.Set(key, value, time.Time{}, false)
}

func setTTL(s *Store, key, value string, deadline time.Time) {
	s.Set(key, value, deadline, true)
}

func TestSetGet(t *testing.T) {
	s := New()
	if _, ok := s.Get("missing", base); ok {
		t.Fatal("empty store returned a value")
	}
	setNoTTL(s, "k", "v")
	got, ok := s.Get("k", base)
	if !ok || got != "v" {
		t.Fatalf("got (%q, %v), want (\"v\", true)", got, ok)
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New()
	setNoTTL(s, "k", "old")
	setNoTTL(s, "k", "new")
	if got, _ := s.Get("k", base); got != "new" {
		t.Fatalf("got %q, want %q", got, "new")
	}
	if s.Len(base) != 1 {
		t.Fatalf("Len = %d, want 1", s.Len(base))
	}
}

func TestDelete(t *testing.T) {
	s := New()
	setNoTTL(s, "k", "v")
	if !s.Delete("k") {
		t.Fatal("Delete on existing key returned false")
	}
	if s.Delete("k") {
		t.Fatal("Delete on missing key returned true")
	}
	if s.Len(base) != 0 {
		t.Fatalf("Len = %d, want 0", s.Len(base))
	}
}

func TestBinarySafeKeysAndValues(t *testing.T) {
	s := New()
	key := "k\x00\r\n"
	val := "v\x00\xff\r\n"
	setNoTTL(s, key, val)
	got, ok := s.Get(key, base)
	if !ok || got != val {
		t.Fatalf("got (%q, %v), want (%q, true)", got, ok, val)
	}
}

// TestExpiryBoundary pins the deadline instant itself as expired. An off-by-one
// here is invisible in ordinary use and would show up only as a key living one
// tick too long.
func TestExpiryBoundary(t *testing.T) {
	s := New()
	setTTL(s, "k", "v", at(10*time.Second))

	cases := []struct {
		name string
		now  time.Time
		want bool
	}{
		{"before the deadline", at(9 * time.Second), true},
		{"one nanosecond before", at(10*time.Second - 1), true},
		{"exactly at the deadline", at(10 * time.Second), false},
		{"after the deadline", at(11 * time.Second), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := s.Get("k", tc.now); ok != tc.want {
				t.Fatalf("Get present = %v, want %v", ok, tc.want)
			}
		})
	}
}

// TestSetClearsExistingTTL is Redis's rule and an easy one to get wrong: a SET
// with no expiry option means "this key has no expiry", not "leave the old one".
func TestSetClearsExistingTTL(t *testing.T) {
	s := New()
	setTTL(s, "k", "v", at(10*time.Second))
	setNoTTL(s, "k", "v2")

	if _, st := s.TTL("k", base); st != NoTTL {
		t.Fatalf("TTL status = %v, want NoTTL", st)
	}
	if _, ok := s.Get("k", at(time.Hour)); !ok {
		t.Fatal("key expired despite the TTL having been cleared")
	}
	if n := len(s.expires); n != 0 {
		t.Fatalf("expires holds %d entries, want 0", n)
	}
}

func TestSetReplacesExistingTTL(t *testing.T) {
	s := New()
	setTTL(s, "k", "v", at(10*time.Second))
	setTTL(s, "k", "v", at(60*time.Second))

	if _, ok := s.Get("k", at(30*time.Second)); !ok {
		t.Fatal("key expired at the old deadline; the new one did not replace it")
	}
	d, st := s.TTL("k", at(30*time.Second))
	if st != HasTTL || d != 30*time.Second {
		t.Fatalf("TTL = (%v, %v), want (30s, HasTTL)", d, st)
	}
}

func TestTTLStatuses(t *testing.T) {
	s := New()
	setNoTTL(s, "plain", "v")
	setTTL(s, "timed", "v", at(10*time.Second))

	if _, st := s.TTL("absent", base); st != NoKey {
		t.Fatalf("missing key: status %v, want NoKey", st)
	}
	if _, st := s.TTL("plain", base); st != NoTTL {
		t.Fatalf("key without a TTL: status %v, want NoTTL", st)
	}
	d, st := s.TTL("timed", at(3*time.Second))
	if st != HasTTL || d != 7*time.Second {
		t.Fatalf("TTL = (%v, %v), want (7s, HasTTL)", d, st)
	}
	// An expired key is indistinguishable from a missing one.
	if _, st := s.TTL("timed", at(20*time.Second)); st != NoKey {
		t.Fatalf("expired key: status %v, want NoKey", st)
	}
}

func TestExpire(t *testing.T) {
	s := New()
	setNoTTL(s, "k", "v")

	if !s.Expire("k", at(10*time.Second), base) {
		t.Fatal("Expire on a live key returned false")
	}
	if _, ok := s.Get("k", at(11*time.Second)); ok {
		t.Fatal("key survived the deadline Expire set")
	}
	if s.Expire("absent", at(10*time.Second), base) {
		t.Fatal("Expire on a missing key returned true")
	}
}

// TestExpireDoesNotResurrect covers the window between a key expiring and the
// worker reclaiming it. During that window the key is still in the map but is
// gone as far as callers are concerned, and giving it a fresh deadline would
// make reclamation timing observable.
func TestExpireDoesNotResurrect(t *testing.T) {
	s := New()
	setTTL(s, "k", "v", at(10*time.Second))
	now := at(20 * time.Second) // expired, not yet reclaimed

	if s.Expire("k", at(time.Hour), now) {
		t.Fatal("Expire revived an expired key")
	}
	if _, ok := s.Get("k", now); ok {
		t.Fatal("key came back")
	}
	if s.Persist("k", now) {
		t.Fatal("Persist revived an expired key")
	}
}

func TestPersist(t *testing.T) {
	s := New()
	setTTL(s, "timed", "v", at(10*time.Second))
	setNoTTL(s, "plain", "v")

	if !s.Persist("timed", base) {
		t.Fatal("Persist on a key with a TTL returned false")
	}
	if _, ok := s.Get("timed", at(time.Hour)); !ok {
		t.Fatal("key expired after its TTL was removed")
	}
	if s.Persist("plain", base) {
		t.Fatal("Persist on a key without a TTL returned true")
	}
	if s.Persist("absent", base) {
		t.Fatal("Persist on a missing key returned true")
	}
}

// TestLenIgnoresExpiredKeys keeps reclamation invisible: a count that included
// expired-but-unreclaimed keys would let callers see when the worker last ran.
func TestLenIgnoresExpiredKeys(t *testing.T) {
	s := New()
	setNoTTL(s, "plain", "v")
	setTTL(s, "short", "v", at(5*time.Second))
	setTTL(s, "long", "v", at(time.Hour))

	if got := s.Len(base); got != 3 {
		t.Fatalf("Len before any deadline = %d, want 3", got)
	}
	if got := s.Len(at(10 * time.Second)); got != 2 {
		t.Fatalf("Len after the short deadline = %d, want 2", got)
	}
	if got := s.Len(at(2 * time.Hour)); got != 1 {
		t.Fatalf("Len after both deadlines = %d, want 1", got)
	}
}

func TestDeleteClearsTTL(t *testing.T) {
	s := New()
	setTTL(s, "k", "v", at(10*time.Second))
	s.Delete("k")
	if n := len(s.expires); n != 0 {
		t.Fatalf("expires holds %d entries after Delete, want 0", n)
	}
	// Recreating the key must not inherit the old deadline.
	setNoTTL(s, "k", "v")
	if _, ok := s.Get("k", at(time.Hour)); !ok {
		t.Fatal("recreated key inherited the deleted key's deadline")
	}
}

// TestDeleteOnExpiredKeyReportsAbsent: the key is already gone to callers, so
// DEL must report 0 even though it does reclaim the entry.
func TestDeleteOnExpiredKeyReportsAbsent(t *testing.T) {
	s := New()
	setTTL(s, "k", "v", at(10*time.Second))
	// Delete has no clock, so the caller is responsible for not deleting what
	// it has already observed as absent. Engine does that; this pins the
	// reclamation half.
	if !s.Delete("k") {
		t.Fatal("Delete did not find the entry to reclaim")
	}
	if len(s.data) != 0 || len(s.expires) != 0 {
		t.Fatalf("entry survived: data=%d expires=%d", len(s.data), len(s.expires))
	}
}

func TestReclaimExpired(t *testing.T) {
	t.Run("removes only expired entries", func(t *testing.T) {
		s := New()
		setTTL(s, "a", "1", at(5*time.Second))
		setTTL(s, "b", "2", at(5*time.Second))
		setTTL(s, "c", "3", at(time.Hour))
		setNoTTL(s, "plain", "4")

		removed, examined := s.ReclaimExpired(at(10*time.Second), 100)
		if removed != 2 {
			t.Fatalf("removed = %d, want 2", removed)
		}
		if examined != 3 {
			t.Fatalf("examined = %d, want 3 (only TTL-bearing keys)", examined)
		}
		if _, ok := s.Get("c", at(10*time.Second)); !ok {
			t.Fatal("a live key was reclaimed")
		}
		if _, ok := s.Get("plain", at(10*time.Second)); !ok {
			t.Fatal("a key without a TTL was reclaimed")
		}
	})

	t.Run("work per call is bounded", func(t *testing.T) {
		s := New()
		for i := 0; i < 50; i++ {
			setTTL(s, string(rune('a'+i%26))+string(rune('a'+i/26)), "v", at(time.Second))
		}
		_, examined := s.ReclaimExpired(at(time.Hour), 10)
		if examined != 10 {
			t.Fatalf("examined = %d, want exactly the limit of 10", examined)
		}
	})

	t.Run("repeated calls eventually reclaim everything", func(t *testing.T) {
		s := New()
		for i := 0; i < 30; i++ {
			setTTL(s, string(rune('a'+i)), "v", at(time.Second))
		}
		now := at(time.Hour)
		for range 100 {
			if _, examined := s.ReclaimExpired(now, 4); examined == 0 {
				break
			}
		}
		if n := len(s.data); n != 0 {
			t.Fatalf("%d keys survived repeated reclamation", n)
		}
	})

	t.Run("empty and non-positive limits", func(t *testing.T) {
		s := New()
		if r, e := s.ReclaimExpired(base, 10); r != 0 || e != 0 {
			t.Fatalf("empty store: got (%d, %d), want (0, 0)", r, e)
		}
		setTTL(s, "k", "v", at(time.Second))
		if r, e := s.ReclaimExpired(at(time.Hour), 0); r != 0 || e != 0 {
			t.Fatalf("zero limit: got (%d, %d), want (0, 0)", r, e)
		}
		if _, ok := s.Get("k", base); !ok {
			t.Fatal("a zero limit removed something")
		}
	})
}

// TestExpiresIsSubsetOfData is the package's one structural invariant. It runs
// a deterministic but interleaved sequence of every mutating operation, since
// the invariant can only break where two maps are updated together.
func TestExpiresIsSubsetOfData(t *testing.T) {
	s := New()
	keys := []string{"a", "b", "c", "d"}

	check := func(step int) {
		t.Helper()
		for key := range s.expires {
			if _, ok := s.data[key]; !ok {
				t.Fatalf("step %d: %q is in expires but not in data", step, key)
			}
		}
	}

	for i := range 200 {
		key := keys[i%len(keys)]
		now := at(time.Duration(i) * time.Second)
		switch i % 7 {
		case 0:
			setNoTTL(s, key, "v")
		case 1:
			setTTL(s, key, "v", now.Add(3*time.Second))
		case 2:
			s.Delete(key)
		case 3:
			s.Expire(key, now.Add(2*time.Second), now)
		case 4:
			s.Persist(key, now)
		case 5:
			s.ReclaimExpired(now, 2)
		case 6:
			s.Get(key, now)
			s.TTL(key, now)
			s.Len(now)
		}
		check(i)
	}
}
