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
