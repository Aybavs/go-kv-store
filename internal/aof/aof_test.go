package aof

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/resp"
)

var epoch = time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

func encodeAll(t *testing.T, records ...Record) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	for _, r := range records {
		if err := Encode(w, r); err != nil {
			t.Fatalf("Encode(%+v): %v", r, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func decodeAll(t *testing.T, data []byte) []Record {
	t.Helper()
	r := resp.NewReader(bytes.NewReader(data), resp.DefaultLimits())
	var out []Record
	for {
		rec, err := Decode(r)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		out = append(out, rec)
	}
}

func TestRoundTrip(t *testing.T) {
	deadline := epoch.Add(30 * time.Second)
	records := []Record{
		DeriveSet("k", "v", time.Time{}, false),
		DeriveSet("k", "v", deadline, true),
		DeriveSet("", "", time.Time{}, false),
		DeriveSet("bin\x00\r\nkey", "bin\x00\xff\r\nvalue", deadline, true),
		DeriveDel([]string{"a"}),
		DeriveDel([]string{"a", "b", "c"}),
		DeriveDel([]string{"bin\x00\r\n"}),
	}

	got := decodeAll(t, encodeAll(t, records...))
	if len(got) != len(records) {
		t.Fatalf("decoded %d records, want %d", len(got), len(records))
	}
	for i, want := range records {
		if !recordsEqual(got[i], want) {
			t.Fatalf("record %d: got %+v, want %+v", i, got[i], want)
		}
	}
}

func recordsEqual(a, b Record) bool {
	if a.Kind != b.Kind {
		return false
	}
	if a.Kind == KindDel {
		if len(a.Keys) != len(b.Keys) {
			return false
		}
		for i := range a.Keys {
			if a.Keys[i] != b.Keys[i] {
				return false
			}
		}
		return true
	}
	return a.Key == b.Key && a.Value == b.Value &&
		a.HasExpiry == b.HasExpiry && a.ExpireAtMS == b.ExpireAtMS
}

// TestDelIsOneRecord pins the atomicity invariant at the level where it is
// decided. Recovery restores a prefix and stops at the first incomplete record,
// so a DEL split into one record per key would let a crash restore a partial
// multi-key delete. The vocabulary being variadic is what prevents that, and
// this asserts the encoding actually is.
func TestDelIsOneRecord(t *testing.T) {
	data := encodeAll(t, DeriveDel([]string{"a", "b", "c"}))

	if got := decodeAll(t, data); len(got) != 1 {
		t.Fatalf("three keys encoded as %d records, want 1", len(got))
	}
	// Stated against the bytes too, so a decoder that merged three records
	// would not satisfy this by accident.
	if n := bytes.Count(data, []byte("DEL")); n != 1 {
		t.Fatalf("the verb appears %d times, want 1", n)
	}
	if !bytes.HasPrefix(data, []byte("*4\r\n")) {
		t.Fatalf("record does not start with a 4-element array header: %q", data[:8])
	}
}

// TestDeriveCarriesValueForward is the ADR-0004 counterexample expressed at the
// derivation. A command log would record "EXPIRE k 30", which on replay depends
// on a prior record having created k. The effect record carries the value, so
// it stands alone.
func TestDeriveCarriesValueForward(t *testing.T) {
	deadline := epoch.Add(30 * time.Second)
	rec := DeriveSet("k", "5", deadline, true)

	if rec.Value != "5" {
		t.Fatalf("value = %q, want the value the key holds", rec.Value)
	}
	if !rec.HasExpiry || rec.ExpireAtMS != deadline.UnixMilli() {
		t.Fatalf("deadline = %d, want %d", rec.ExpireAtMS, deadline.UnixMilli())
	}

	round := decodeAll(t, encodeAll(t, rec))[0]
	if round.Value != "5" || round.ExpireAtMS != deadline.UnixMilli() {
		t.Fatalf("round trip lost the effect: %+v", round)
	}
}

// TestDeriveDelCopiesKeys: the caller's slice belongs to the command layer and
// does not outlive the request, while a record can sit in the write buffer well
// past it.
func TestDeriveDelCopiesKeys(t *testing.T) {
	keys := []string{"a", "b"}
	rec := DeriveDel(keys)
	keys[0] = "mutated"

	if rec.Keys[0] != "a" {
		t.Fatalf("record aliases the caller's slice: %q", rec.Keys[0])
	}
}

// TestDecodeRejectsStructuralViolations. Each of these must be distinguishable
// from a torn tail, because recovery truncates one and refuses to start on the
// other.
func TestDecodeRejectsStructuralViolations(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"unknown verb", "*3\r\n$4\r\nNOPE\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"SET with two parts", "*2\r\n$3\r\nSET\r\n$1\r\nk\r\n"},
		{"SET with four parts", "*4\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPXAT\r\n"},
		{"unknown SET option", "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nAT\r\n$1\r\n1\r\n"},
		{"PXAT not an integer", "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$4\r\nPXAT\r\n$3\r\nabc\r\n"},
		{"DEL with no keys", "*1\r\n$3\r\nDEL\r\n"},
		{"lowercase verb", "*3\r\n$3\r\nset\r\n$1\r\nk\r\n$1\r\nv\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := resp.NewReader(strings.NewReader(tc.data), resp.DefaultLimits())
			_, err := Decode(r)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v, want ErrCorrupt", err)
			}
			if errors.Is(err, io.EOF) {
				t.Fatal("corruption must not be reported as a clean end of stream")
			}
			if errors.Is(err, resp.ErrIncompleteFrame) {
				t.Fatal("corruption must not be reported as a torn tail; recovery would truncate the file instead of refusing to start")
			}
		})
	}
}

// TestTornRecordIsNotCorruption is the other half, and the distinction the
// whole recovery policy rests on: a record that simply ran out is expected
// after a crash, and must not look like corruption.
func TestTornRecordIsNotCorruption(t *testing.T) {
	full := encodeAll(t, DeriveSet("key", "value", epoch, true))

	for cut := 1; cut < len(full); cut++ {
		r := resp.NewReader(bytes.NewReader(full[:cut]), resp.DefaultLimits())
		_, err := Decode(r)
		if err == nil {
			t.Fatalf("cut at %d decoded a complete record from a partial one", cut)
		}
		if errors.Is(err, ErrCorrupt) {
			t.Fatalf("cut at %d reported corruption; a torn tail must not refuse to start:\n  %v", cut, err)
		}
		// The positive assertion, not just the absence of the negative one:
		// recovery must be able to recognise this as a torn tail rather than
		// merely fail to recognise it as corruption.
		if !errors.Is(err, resp.ErrIncompleteFrame) {
			t.Fatalf("cut at %d: %v is not identifiable as a torn tail", cut, err)
		}
	}
}

func TestEmptyStreamIsCleanEOF(t *testing.T) {
	r := resp.NewReader(bytes.NewReader(nil), resp.DefaultLimits())
	if _, err := Decode(r); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestEncodeRefusesEmptyDel(t *testing.T) {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	if err := Encode(w, Record{Kind: KindDel}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

func TestHeader(t *testing.T) {
	t.Run("round trip", func(t *testing.T) {
		empty, err := ReadHeader(bytes.NewReader(EncodeHeader()))
		if err != nil || empty {
			t.Fatalf("got (empty=%v, %v), want (false, nil)", empty, err)
		}
	})

	t.Run("an empty file is a new log, not a damaged one", func(t *testing.T) {
		empty, err := ReadHeader(bytes.NewReader(nil))
		if err != nil || !empty {
			t.Fatalf("got (empty=%v, %v), want (true, nil)", empty, err)
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		for cut := 1; cut < HeaderSize; cut++ {
			_, err := ReadHeader(bytes.NewReader(EncodeHeader()[:cut]))
			if !errors.Is(err, ErrTruncatedHeader) {
				t.Fatalf("cut at %d: got %v, want ErrTruncatedHeader", cut, err)
			}
		}
	})

	t.Run("foreign file", func(t *testing.T) {
		foreign := []byte("REDIS0011\x00\x00\x00\x00\x00\x00\x00")
		if _, err := ReadHeader(bytes.NewReader(foreign)); !errors.Is(err, ErrNotAnAOF) {
			t.Fatalf("got %v, want ErrNotAnAOF", err)
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		h := EncodeHeader()
		h[11] = byte(FormatVersion + 1)
		var ve *UnsupportedVersionError
		_, err := ReadHeader(bytes.NewReader(h))
		if !errors.As(err, &ve) {
			t.Fatalf("got %v, want *UnsupportedVersionError", err)
		}
		if ve.Found != FormatVersion+1 || ve.Supported != FormatVersion {
			t.Fatalf("error reports found=%d supported=%d", ve.Found, ve.Supported)
		}
	})

	t.Run("header size is fixed", func(t *testing.T) {
		if got := len(EncodeHeader()); got != HeaderSize {
			t.Fatalf("header is %d bytes, want %d; recovery reports offsets against this", got, HeaderSize)
		}
	})
}
