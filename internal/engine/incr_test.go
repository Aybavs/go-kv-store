package engine

import (
	"errors"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
)

// TestParseStoredInt is a table of what real Redis accepts, measured against
// 8.10.0. The four rows marked below are the reason this function exists at
// all: strconv.ParseInt accepts every one of them and Redis accepts none, so
// using the standard library here would diverge from the oracle four times over
// a value the client stored earlier and does not think of as input.
func TestParseStoredInt(t *testing.T) {
	tests := []struct {
		in    string
		want  int64
		valid bool
	}{
		{"0", 0, true},
		{"5", 5, true},
		{"-3", -3, true},
		{"9223372036854775807", math.MaxInt64, true},
		{"-9223372036854775808", math.MinInt64, true},

		{"+5", 0, false},  // ParseInt accepts
		{"07", 0, false},  // ParseInt accepts
		{"00", 0, false},  // ParseInt accepts
		{"-0", 0, false},  // ParseInt accepts
		{"-00", 0, false}, // ParseInt accepts

		{"", 0, false},
		{" 5", 0, false},
		{"5 ", 0, false},
		{"abc", 0, false},
		{"3.0", 0, false},
		{"5\r\n", 0, false},
		{"-", 0, false},
		{"--5", 0, false},
		{"5-", 0, false},
		{"1e3", 0, false},

		// Beyond int64 in both directions: well-formed digits, wrong range.
		{"9223372036854775808", 0, false},
		{"-9223372036854775809", 0, false},
		{"92233720368547758080", 0, false},
	}

	for _, tc := range tests {
		got, ok := parseStoredInt(tc.in)
		if ok != tc.valid {
			t.Errorf("parseStoredInt(%q) valid = %v, want %v", tc.in, ok, tc.valid)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("parseStoredInt(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestIncrByOnMissingKey(t *testing.T) {
	e := newTestEngine(t)

	if got, err := e.IncrBy("up", 1); err != nil || got != 1 {
		t.Fatalf("IncrBy(+1) on a missing key = (%d, %v), want (1, nil)", got, err)
	}
	if got, err := e.IncrBy("down", -1); err != nil || got != -1 {
		t.Fatalf("IncrBy(-1) on a missing key = (%d, %v), want (-1, nil)", got, err)
	}
	// The result is stored as its decimal text, readable by an ordinary GET.
	if v, ok := e.Get("up"); !ok || v != "1" {
		t.Fatalf("stored value = %q, %v; want \"1\", true", v, ok)
	}
}

func TestIncrByOnExistingValue(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Set("k", "5", NoExpiry())

	if got, err := e.IncrBy("k", 1); err != nil || got != 6 {
		t.Fatalf("IncrBy = (%d, %v), want (6, nil)", got, err)
	}
	if got, err := e.IncrBy("k", -1); err != nil || got != 5 {
		t.Fatalf("IncrBy = (%d, %v), want (5, nil)", got, err)
	}
	if v, _ := e.Get("k"); v != "5" {
		t.Fatalf("stored value = %q, want \"5\"", v)
	}
}

// A rejected INCR must leave the value exactly as it was. Redis does, and a
// half-applied read-modify-write would be worse than the error.
func TestIncrByRejectsNonIntegerAndLeavesTheValue(t *testing.T) {
	e, _ := newClockedEngine(t)
	_ = e.Set("k", "abc", ExpiresIn(time.Minute))

	if _, err := e.IncrBy("k", 1); !errors.Is(err, ErrNotAnInteger) {
		t.Fatalf("IncrBy on %q = %v, want ErrNotAnInteger", "abc", err)
	}
	if v, ok := e.Get("k"); !ok || v != "abc" {
		t.Fatalf("value after a rejected INCR = %q, %v; want \"abc\", true", v, ok)
	}
	if d, st := e.TTL("k"); st != HasTTL || d != time.Minute {
		t.Fatalf("TTL after a rejected INCR = (%v, %v), want (1m, HasTTL)", d, st)
	}
}

func TestIncrByOverflowAtBothBoundaries(t *testing.T) {
	e := newTestEngine(t)

	_ = e.Set("max", strconv.FormatInt(math.MaxInt64, 10), NoExpiry())
	if _, err := e.IncrBy("max", 1); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("IncrBy at MaxInt64 = %v, want ErrIncrOverflow", err)
	}
	if v, _ := e.Get("max"); v != strconv.FormatInt(math.MaxInt64, 10) {
		t.Fatalf("value changed on overflow: %q", v)
	}

	_ = e.Set("min", strconv.FormatInt(math.MinInt64, 10), NoExpiry())
	if _, err := e.IncrBy("min", -1); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("IncrBy at MinInt64 = %v, want ErrIncrOverflow", err)
	}
	if v, _ := e.Get("min"); v != strconv.FormatInt(math.MinInt64, 10) {
		t.Fatalf("value changed on overflow: %q", v)
	}

	// The boundary is reachable from the other side, so the check is not simply
	// refusing everything near the limit.
	if got, err := e.IncrBy("max", -1); err != nil || got != math.MaxInt64-1 {
		t.Fatalf("IncrBy(-1) at MaxInt64 = (%d, %v), want (%d, nil)", got, err, math.MaxInt64-1)
	}
}

// An expired key is absent to callers, so INCR starts again from zero rather
// than continuing a count nobody could have read — and the new key must not
// inherit the dead entry's deadline.
func TestIncrByOnExpiredKeyStartsOver(t *testing.T) {
	e, clock := newClockedEngine(t)
	_ = e.Set("k", "41", ExpiresIn(10*time.Second))
	clock.advance(10 * time.Second)

	got, err := e.IncrBy("k", 1)
	if err != nil || got != 1 {
		t.Fatalf("IncrBy on an expired key = (%d, %v), want (1, nil)", got, err)
	}
	if _, st := e.TTL("k"); st != NoTTL {
		t.Fatalf("TTL status = %v, want NoTTL; the dead entry's deadline was carried forward", st)
	}
}

func TestIncrByRefusedWhileDraining(t *testing.T) {
	e := newTestEngine(t)
	_ = e.Set("k", "1", NoExpiry())
	e.BeginDrain()

	if _, err := e.IncrBy("k", 1); !errors.Is(err, ErrDraining) {
		t.Fatalf("IncrBy while draining = %v, want ErrDraining", err)
	}
}

// TestIncrByPreservesTTLInMemoryAndInTheLog is the milestone's load-bearing
// test, and it is deliberately two assertions rather than one.
//
// The in-memory half can be right while the logged half is wrong, and that
// combination survives every test until a crash: recovery would restore the key
// with no expiry at all. ADR-0004's counterexample is exactly this shape, so
// the record is inspected directly rather than inferred from what Get and TTL
// report afterwards.
func TestIncrByPreservesTTLInMemoryAndInTheLog(t *testing.T) {
	f := &recordingFile{}
	e, clock, _ := newLoggedEngine(t, f, aof.EverySec)

	if err := e.Set("k", "5", ExpiresIn(30*time.Second)); err != nil {
		t.Fatalf("Set: %v", err)
	}
	deadline := clock.now().Add(30 * time.Second)

	clock.advance(10 * time.Second)

	if got, err := e.IncrBy("k", 1); err != nil || got != 6 {
		t.Fatalf("IncrBy = (%d, %v), want (6, nil)", got, err)
	}

	// In memory: the deadline did not move with the increment.
	if d, st := e.TTL("k"); st != HasTTL || d != 20*time.Second {
		t.Fatalf("TTL after INCR = (%v, %v), want (20s, HasTTL)", d, st)
	}

	// In the log: a complete SET carrying the result and the same absolute
	// instant the original SET recorded, not one measured from now.
	got := f.records(t)
	if len(got) != 2 {
		t.Fatalf("wrote %d records, want 2: %+v", len(got), got)
	}
	rec := got[1]
	if rec.Kind != aof.KindSet || rec.Key != "k" || rec.Value != "6" {
		t.Fatalf("INCR logged as %+v, want a SET carrying the result", rec)
	}
	if !rec.HasExpiry {
		t.Fatal("INCR logged a SET with no expiry; recovery would resurrect the key without its TTL")
	}
	if rec.ExpireAtMS != deadline.UnixMilli() {
		t.Fatalf("INCR logged PXAT %d, want %d (the deadline the key already had)",
			rec.ExpireAtMS, deadline.UnixMilli())
	}
}

// A key created by INCR carries no expiry, and the record must say so too.
func TestIncrByOnMissingKeyLogsNoExpiry(t *testing.T) {
	f := &recordingFile{}
	e, _, _ := newLoggedEngine(t, f, aof.EverySec)

	if _, err := e.IncrBy("fresh", 1); err != nil {
		t.Fatalf("IncrBy: %v", err)
	}

	got := f.records(t)
	if len(got) != 1 {
		t.Fatalf("wrote %d records, want 1: %+v", len(got), got)
	}
	if got[0].Kind != aof.KindSet || got[0].Value != "1" || got[0].HasExpiry {
		t.Fatalf("logged %+v, want a SET of 1 with no expiry", got[0])
	}
}

// A refused INCR must not append anything. Otherwise a client hammering INCR on
// a non-numeric key grows the log without changing any state.
func TestRejectedIncrLogsNothing(t *testing.T) {
	f := &recordingFile{}
	e, _, _ := newLoggedEngine(t, f, aof.EverySec)

	if err := e.Set("k", "abc", NoExpiry()); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := e.IncrBy("k", 1); !errors.Is(err, ErrNotAnInteger) {
		t.Fatalf("IncrBy = %v, want ErrNotAnInteger", err)
	}
	_ = e.Set("big", strconv.FormatInt(math.MaxInt64, 10), NoExpiry())
	if _, err := e.IncrBy("big", 1); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("IncrBy = %v, want ErrIncrOverflow", err)
	}

	// Two SETs, and nothing from either refused INCR.
	got := f.records(t)
	if len(got) != 2 {
		t.Fatalf("wrote %d records, want 2 (the SETs only): %+v", len(got), got)
	}
}
