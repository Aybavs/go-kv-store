package command

import (
	"testing"

	"github.com/aybavs/go-kv-store/internal/engine"
)

func newTestRegistry(t *testing.T) (*Registry, *engine.Engine) {
	t.Helper()
	e := engine.New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
	return New(e), e
}

func args(parts ...string) [][]byte {
	out := make([][]byte, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}

func TestPing(t *testing.T) {
	r, _ := newTestRegistry(t)
	got := r.Dispatch(args("PING"))
	if got.Kind != ReplySimple || got.Str != "PONG" {
		t.Fatalf("got %+v, want simple PONG", got)
	}
}

func TestCommandNameIsCaseInsensitive(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, name := range []string{"ping", "Ping", "PING", "pInG"} {
		if got := r.Dispatch(args(name)); got.Kind != ReplySimple {
			t.Fatalf("%q: got %+v, want simple reply", name, got)
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	r, _ := newTestRegistry(t)
	got := r.Dispatch(args("NOPE", "x"))
	if got.Kind != ReplyError {
		t.Fatalf("got %+v, want error reply", got)
	}
	if want := "ERR unknown command 'NOPE'"; got.Str != want {
		t.Fatalf("got %q, want %q", got.Str, want)
	}
}

func TestWrongArity(t *testing.T) {
	r, _ := newTestRegistry(t)
	got := r.Dispatch(args("PING", "extra"))
	if got.Kind != ReplyError {
		t.Fatalf("got %+v, want error reply", got)
	}
	if want := "ERR wrong number of arguments for 'PING' command"; got.Str != want {
		t.Fatalf("got %q, want %q", got.Str, want)
	}
}

func TestEmptyArgs(t *testing.T) {
	r, _ := newTestRegistry(t)
	if got := r.Dispatch(nil); got.Kind != ReplyError {
		t.Fatalf("got %+v, want error reply", got)
	}
}

func TestSetAndGet(t *testing.T) {
	r, _ := newTestRegistry(t)

	if got := r.Dispatch(args("SET", "k", "v")); got.Kind != ReplySimple || got.Str != "OK" {
		t.Fatalf("SET got %+v, want simple OK", got)
	}
	got := r.Dispatch(args("GET", "k"))
	if got.Kind != ReplyBulk || got.Str != "v" {
		t.Fatalf("GET got %+v, want bulk \"v\"", got)
	}
}

func TestGetMissingReturnsNullBulk(t *testing.T) {
	r, _ := newTestRegistry(t)
	if got := r.Dispatch(args("GET", "nope")); got.Kind != ReplyNullBulk {
		t.Fatalf("got %+v, want null bulk", got)
	}
}

func TestSetBinarySafe(t *testing.T) {
	r, _ := newTestRegistry(t)
	key, val := "k\x00\r\n", "v\xff\x00\r\n"
	r.Dispatch(args("SET", key, val))
	got := r.Dispatch(args("GET", key))
	if got.Kind != ReplyBulk || got.Str != val {
		t.Fatalf("got %+v, want bulk %q", got, val)
	}
}

func TestSetOverwrites(t *testing.T) {
	r, _ := newTestRegistry(t)
	r.Dispatch(args("SET", "k", "old"))
	r.Dispatch(args("SET", "k", "new"))
	if got := r.Dispatch(args("GET", "k")); got.Str != "new" {
		t.Fatalf("got %q, want %q", got.Str, "new")
	}
}

func TestSetArity(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, a := range [][][]byte{args("SET"), args("SET", "k"), args("SET", "k", "v", "extra")} {
		if got := r.Dispatch(a); got.Kind != ReplyError {
			t.Fatalf("%q: got %+v, want error", a, got)
		}
	}
}

// TestMutationRejectedWhileDraining proves the engine's admission gate surfaces
// as a client-visible error rather than a silent success.
func TestMutationRejectedWhileDraining(t *testing.T) {
	r, e := newTestRegistry(t)
	r.Dispatch(args("SET", "k", "v"))
	e.BeginDrain()

	got := r.Dispatch(args("SET", "k2", "v2"))
	if got.Kind != ReplyError || got.Str != "ERR server is shutting down" {
		t.Fatalf("got %+v, want shutdown error", got)
	}
	if read := r.Dispatch(args("GET", "k")); read.Kind != ReplyBulk || read.Str != "v" {
		t.Fatalf("reads must continue while draining, got %+v", read)
	}
}

func TestDel(t *testing.T) {
	r, _ := newTestRegistry(t)
	r.Dispatch(args("SET", "a", "1"))
	r.Dispatch(args("SET", "b", "2"))

	got := r.Dispatch(args("DEL", "a", "missing", "b"))
	if got.Kind != ReplyInt || got.Int != 2 {
		t.Fatalf("got %+v, want integer 2", got)
	}
	if after := r.Dispatch(args("GET", "a")); after.Kind != ReplyNullBulk {
		t.Fatalf("key survived DEL: %+v", after)
	}
}

func TestDelSingleKey(t *testing.T) {
	r, _ := newTestRegistry(t)
	r.Dispatch(args("SET", "k", "v"))
	if got := r.Dispatch(args("DEL", "k")); got.Kind != ReplyInt || got.Int != 1 {
		t.Fatalf("got %+v, want integer 1", got)
	}
}

func TestExistsCountsDuplicates(t *testing.T) {
	r, _ := newTestRegistry(t)
	r.Dispatch(args("SET", "a", "1"))

	if got := r.Dispatch(args("EXISTS", "a")); got.Kind != ReplyInt || got.Int != 1 {
		t.Fatalf("got %+v, want integer 1", got)
	}
	if got := r.Dispatch(args("EXISTS", "a", "a", "missing")); got.Int != 2 {
		t.Fatalf("got %+v, want integer 2 (duplicates counted)", got)
	}
	if got := r.Dispatch(args("EXISTS", "missing")); got.Int != 0 {
		t.Fatalf("got %+v, want integer 0", got)
	}
}

func TestDelExistsArity(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, a := range [][][]byte{args("DEL"), args("EXISTS")} {
		if got := r.Dispatch(a); got.Kind != ReplyError {
			t.Fatalf("%q: got %+v, want error", a, got)
		}
	}
}

func TestDelRejectedWhileDraining(t *testing.T) {
	r, e := newTestRegistry(t)
	r.Dispatch(args("SET", "k", "v"))
	e.BeginDrain()
	if got := r.Dispatch(args("DEL", "k")); got.Kind != ReplyError {
		t.Fatalf("got %+v, want error", got)
	}
}

// TestReadsContinueWhileDraining pins that draining refuses mutations only.
// Both read commands must keep answering: a client that is mid-read when
// shutdown begins should get its answer, not an error.
func TestReadsContinueWhileDraining(t *testing.T) {
	r, e := newTestRegistry(t)
	r.Dispatch(args("SET", "a", "1"))
	r.Dispatch(args("SET", "b", "2"))

	e.BeginDrain()

	if got := r.Dispatch(args("GET", "a")); got.Kind != ReplyBulk || got.Str != "1" {
		t.Fatalf("GET while draining = %+v, want bulk \"1\"", got)
	}
	if got := r.Dispatch(args("EXISTS", "a", "b", "missing")); got.Kind != ReplyInt || got.Int != 2 {
		t.Fatalf("EXISTS while draining = %+v, want integer 2", got)
	}
	if got := r.Dispatch(args("GET", "missing")); got.Kind != ReplyNullBulk {
		t.Fatalf("GET of a missing key while draining = %+v, want null bulk", got)
	}
	if got := r.Dispatch(args("PING")); got.Kind != ReplySimple || got.Str != "PONG" {
		t.Fatalf("PING while draining = %+v, want simple PONG", got)
	}
}

// TestEmptyValueIsNotAMissingKey pins the distinction between a key holding an
// empty string and a key that does not exist. They encode differently on the
// wire ($0\r\n\r\n versus $-1\r\n), so Kind — not Str — must discriminate them.
func TestEmptyValueIsNotAMissingKey(t *testing.T) {
	r, _ := newTestRegistry(t)

	if got := r.Dispatch(args("SET", "empty", "")); got.Kind != ReplySimple {
		t.Fatalf("SET with an empty value = %+v, want simple OK", got)
	}

	stored := r.Dispatch(args("GET", "empty"))
	if stored.Kind != ReplyBulk {
		t.Fatalf("GET of an empty value = %+v, want a bulk reply, not null bulk", stored)
	}
	if stored.Str != "" {
		t.Fatalf("GET of an empty value returned %q, want the empty string", stored.Str)
	}

	missing := r.Dispatch(args("GET", "absent"))
	if missing.Kind != ReplyNullBulk {
		t.Fatalf("GET of a missing key = %+v, want null bulk", missing)
	}

	if stored.Kind == missing.Kind {
		t.Fatal("an empty stored value and a missing key produced the same reply kind")
	}

	if got := r.Dispatch(args("EXISTS", "empty")); got.Kind != ReplyInt || got.Int != 1 {
		t.Fatalf("EXISTS on a key holding an empty value = %+v, want integer 1", got)
	}
}
