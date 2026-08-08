package aof

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/resp"
	"github.com/aybavs/go-kv-store/internal/store"
)

// store.Store is the real Applier. Using it rather than a fake keeps the rules
// tested against the semantics they actually have to produce — in particular
// that a Set with no TTL clears one, which a hand-written fake would be free to
// get wrong in the same direction as the code.
var _ Applier = (*store.Store)(nil)

func fileWith(t *testing.T, records ...Record) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(EncodeHeader())
	w := resp.NewWriter(&buf)
	for _, r := range records {
		if err := Encode(w, r); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func replayInto(t *testing.T, data []byte, now time.Time) (*store.Store, Result, error) {
	t.Helper()
	s := store.New()
	res, err := Replay(bytes.NewReader(data), now, s)
	return s, res, err
}

func TestReplayAppliesEffects(t *testing.T) {
	now := epoch
	data := fileWith(t,
		DeriveSet("plain", "v", time.Time{}, false),
		DeriveSet("timed", "v", now.Add(time.Hour), true),
		DeriveSet("gone", "v", time.Time{}, false),
		DeriveDel([]string{"gone"}),
	)

	s, res, err := replayInto(t, data, now)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Records != 4 {
		t.Fatalf("replayed %d records, want 4", res.Records)
	}
	if res.Truncated {
		t.Fatal("a complete file was reported as truncated")
	}
	if res.LastValidOffset != int64(len(data)) {
		t.Fatalf("LastValidOffset = %d, want %d (the whole file)", res.LastValidOffset, len(data))
	}

	if v, ok := s.Get("plain", now); !ok || v != "v" {
		t.Fatalf("plain = (%q, %v)", v, ok)
	}
	if _, st := s.TTL("timed", now); st != store.HasTTL {
		t.Fatalf("timed has status %v, want HasTTL", st)
	}
	if _, ok := s.Get("gone", now); ok {
		t.Fatal("a deleted key came back")
	}
}

// TestExpiredRecordEnsuresAbsence is rule 3, and the reason it is written down
// as a rule at all. Skipping an expired record is the natural implementation
// and it is wrong: it resurrects the value that record replaced.
func TestExpiredRecordEnsuresAbsence(t *testing.T) {
	now := epoch
	data := fileWith(t,
		DeriveSet("k", "old", time.Time{}, false),
		DeriveSet("k", "new", now.Add(-time.Second), true), // already expired at replay
	)

	s, _, err := replayInto(t, data, now)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	v, ok := s.Get("k", now)
	if ok {
		t.Fatalf("k = %q; an expired record must leave the key absent, not skipped past", v)
	}
}

func TestExpiredRecordAtTheDeadlineIsAbsent(t *testing.T) {
	now := epoch
	data := fileWith(t, DeriveSet("k", "v", now, true)) // deadline == replayNow

	s, _, err := replayInto(t, data, now)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, ok := s.Get("k", now); ok {
		t.Fatal("a record whose deadline is exactly replayNow must be treated as expired")
	}
}

// TestOneReplayNowThroughout: a long replay must not have a key expire part-way
// through it, or the reconstructed state would depend on how long recovery took.
func TestOneReplayNowThroughout(t *testing.T) {
	now := epoch
	deadline := now.Add(time.Millisecond) // in the future, but barely

	records := []Record{DeriveSet("k", "v", deadline, true)}
	for i := 0; i < 5000; i++ {
		records = append(records, DeriveSet("filler", "v", time.Time{}, false))
	}
	data := fileWith(t, records...)

	s, _, err := replayInto(t, data, now)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, ok := s.Get("k", now); !ok {
		t.Fatal("a key alive at replayNow expired during the replay itself")
	}
}

// TestTornTailTruncatesAtEveryOffset is the crash case. A good file cut at any
// point inside its final record must recover to the state the earlier records
// describe, and report where to truncate.
func TestTornTailTruncatesAtEveryOffset(t *testing.T) {
	now := epoch
	complete := fileWith(t,
		DeriveSet("a", "1", time.Time{}, false),
		DeriveSet("b", "2", time.Time{}, false),
	)
	prefix := fileWith(t, DeriveSet("a", "1", time.Time{}, false))

	for cut := len(prefix) + 1; cut < len(complete); cut++ {
		s, res, err := replayInto(t, complete[:cut], now)
		if err != nil {
			t.Fatalf("cut at %d: %v; a torn tail must not refuse to start", cut, err)
		}
		if !res.Truncated {
			t.Fatalf("cut at %d was not reported as truncated", cut)
		}
		if res.LastValidOffset != int64(len(prefix)) {
			t.Fatalf("cut at %d: LastValidOffset = %d, want %d", cut, res.LastValidOffset, len(prefix))
		}
		if v, ok := s.Get("a", now); !ok || v != "1" {
			t.Fatalf("cut at %d lost the complete record before the tear", cut)
		}
		if _, ok := s.Get("b", now); ok {
			t.Fatal("a half-written record was applied")
		}
	}
}

// TestStructuralCorruptionRefuses: silently skipping past this would load a
// state we cannot justify, so it stops the server instead.
func TestStructuralCorruptionRefuses(t *testing.T) {
	now := epoch
	good := fileWith(t, DeriveSet("a", "1", time.Time{}, false))

	cases := []struct {
		name string
		tail string
	}{
		{"unknown verb", "*3\r\n$4\r\nNOPE\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"not an array", "+OK\r\n"},
		{"empty array", "*0\r\n"},
		{"bad bulk prefix", "*3\r\n#3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"PXAT not an integer", "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPXAT\r\n$1\r\nx\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := append(append([]byte(nil), good...), tc.tail...)
			_, res, err := replayInto(t, data, now)

			var ce *CorruptionError
			if !errors.As(err, &ce) {
				t.Fatalf("got %v, want a CorruptionError", err)
			}
			if res.Truncated {
				t.Fatal("corruption was reported as a torn tail; the file would be truncated instead of refused")
			}
			// The offset must name where the trouble starts, not the start of
			// the file, or it tells an operator nothing.
			if ce.Offset != int64(len(good)) {
				t.Fatalf("corruption reported at offset %d, want %d", ce.Offset, len(good))
			}
		})
	}
}

func TestReplayRejectsBadHeaders(t *testing.T) {
	now := epoch

	t.Run("empty file is a new log", func(t *testing.T) {
		s, res, err := replayInto(t, nil, now)
		if err != nil {
			t.Fatalf("an empty file must be a new log, not a failure: %v", err)
		}
		if res.Records != 0 || res.LastValidOffset != 0 {
			t.Fatalf("got %+v, want a zero result", res)
		}
		if s.PhysicalLen() != 0 {
			t.Fatal("an empty file produced state")
		}
	})

	t.Run("foreign file", func(t *testing.T) {
		if _, _, err := replayInto(t, []byte("REDIS0011.......>"), now); !errors.Is(err, ErrNotAnAOF) {
			t.Fatalf("got %v, want ErrNotAnAOF", err)
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		if _, _, err := replayInto(t, EncodeHeader()[:4], now); !errors.Is(err, ErrTruncatedHeader) {
			t.Fatalf("got %v, want ErrTruncatedHeader", err)
		}
	})

	t.Run("header only is a valid empty log", func(t *testing.T) {
		_, res, err := replayInto(t, EncodeHeader(), now)
		if err != nil {
			t.Fatalf("a header with no records is a valid empty log: %v", err)
		}
		if res.Records != 0 {
			t.Fatalf("replayed %d records from a header-only file", res.Records)
		}
		if res.LastValidOffset != HeaderSize {
			t.Fatalf("LastValidOffset = %d, want %d", res.LastValidOffset, HeaderSize)
		}
	})
}

// TestSetWithoutExpiryClearsATTLOnReplay: the replayed rules must produce the
// same semantics the live path does, or a restart would silently change state.
func TestSetWithoutExpiryClearsATTLOnReplay(t *testing.T) {
	now := epoch
	data := fileWith(t,
		DeriveSet("k", "v", now.Add(time.Hour), true),
		DeriveSet("k", "v2", time.Time{}, false),
	)

	s, _, err := replayInto(t, data, now)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if _, st := s.TTL("k", now); st != store.NoTTL {
		t.Fatalf("TTL status = %v, want NoTTL; the second record did not clear the deadline", st)
	}
}

// TestRoundTripThroughReplay is the property the whole file format exists for:
// what the engine writes, replay reads back into the same state.
func TestRoundTripThroughReplay(t *testing.T) {
	now := epoch
	data := fileWith(t,
		DeriveSet("bin\x00\r\nkey", "bin\x00\xff\r\nvalue", time.Time{}, false),
		DeriveSet("", "", time.Time{}, false),
		DeriveSet("timed", "v", now.Add(time.Hour), true),
		DeriveDel([]string{"absent", "also-absent"}),
	)

	s, res, err := replayInto(t, data, now)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.Records != 4 {
		t.Fatalf("replayed %d records, want 4", res.Records)
	}
	if v, ok := s.Get("bin\x00\r\nkey", now); !ok || v != "bin\x00\xff\r\nvalue" {
		t.Fatalf("binary key/value did not survive: (%q, %v)", v, ok)
	}
	if _, ok := s.Get("", now); !ok {
		t.Fatal("the empty key did not survive")
	}
}

// TestOpenFileTruncatesTheTornTail: the file must be left in a state where the
// next append lands after the last complete record, not after the debris.
func TestOpenFileTruncatesTheTornTail(t *testing.T) {
	now := epoch
	complete := fileWith(t,
		DeriveSet("a", "1", time.Time{}, false),
		DeriveSet("b", "2", time.Time{}, false),
	)
	prefix := fileWith(t, DeriveSet("a", "1", time.Time{}, false))

	path := t.TempDir() + "/torn.aof"
	// Cut inside the final record.
	if err := os.WriteFile(path, complete[:len(complete)-4], 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := store.New()
	l, res, err := OpenFile(path, EverySec, now, s, func(err error) { t.Errorf("unexpected fatal: %v", err) })
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if !res.Truncated {
		t.Fatal("the tear was not reported")
	}

	if _, err := l.Append(DeriveSet("c", "3", time.Time{}, false)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reading it back must find exactly two records and no debris.
	s2 := store.New()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	res2, err := Replay(bytes.NewReader(data), now, s2)
	if err != nil {
		t.Fatalf("replaying the reopened file: %v", err)
	}
	if res2.Truncated {
		t.Fatal("the reopened file still ends in a torn record")
	}
	if res2.Records != 2 {
		t.Fatalf("reopened file holds %d records, want 2", res2.Records)
	}
	if len(data) != len(prefix)+(len(fileWith(t, DeriveSet("c", "3", time.Time{}, false)))-HeaderSize) {
		t.Fatalf("file is %d bytes; the append did not land at the truncation point", len(data))
	}
	if v, ok := s2.Get("c", now); !ok || v != "3" {
		t.Fatalf("the appended record did not survive: (%q, %v)", v, ok)
	}
}

// TestOpenFileWritesAHeaderForANewLog: without it, the next start would see a
// file that does not begin with our magic and refuse to run.
func TestOpenFileWritesAHeaderForANewLog(t *testing.T) {
	path := t.TempDir() + "/new.aof"
	s := store.New()

	l, res, err := OpenFile(path, EverySec, epoch, s, func(err error) { t.Errorf("unexpected fatal: %v", err) })
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if res.Records != 0 {
		t.Fatalf("a new file replayed %d records", res.Records)
	}
	if _, err := l.Append(DeriveSet("k", "v", time.Time{}, false)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if empty, err := ReadHeader(bytes.NewReader(data)); err != nil || empty {
		t.Fatalf("the new log does not start with a valid header: (empty=%v, %v)", empty, err)
	}
}

// TestOpenFileRefusesCorruption leaves the file untouched: a server that
// rewrote a corrupt log on its way to refusing it would destroy the evidence.
func TestOpenFileRefusesCorruption(t *testing.T) {
	path := t.TempDir() + "/corrupt.aof"
	data := append(fileWith(t, DeriveSet("a", "1", time.Time{}, false)),
		[]byte("*3\r\n$4\r\nNOPE\r\n$1\r\nk\r\n$1\r\nv\r\n")...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := store.New()
	_, _, err := OpenFile(path, EverySec, epoch, s, func(err error) { t.Errorf("unexpected fatal: %v", err) })
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("got %v, want a CorruptionError", err)
	}

	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read back: %v", readErr)
	}
	if len(after) != len(data) {
		t.Fatalf("the file was modified on the way to refusing it: %d bytes, was %d", len(after), len(data))
	}
}
