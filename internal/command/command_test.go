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
