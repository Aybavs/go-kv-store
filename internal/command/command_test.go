package command

import (
	"strings"
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

// Redis answers a bare PING with the status PONG and PING <msg> with a bulk
// string. The two reply types are not interchangeable to a client.
func TestPingWithMessage(t *testing.T) {
	r, _ := newTestRegistry(t)

	got := r.Dispatch(args("PING", "hello"))
	if got.Kind != ReplyBulk {
		t.Fatalf("got %+v, want bulk reply", got)
	}
	if got.Str != "hello" {
		t.Fatalf("got %q, want %q", got.Str, "hello")
	}

	// The message is binary-safe and is not interpreted.
	if got := r.Dispatch(args("PING", "a\x00\r\nb")); got.Str != "a\x00\r\nb" {
		t.Fatalf("binary message: got %q", got.Str)
	}

	// Two arguments is still wrong arity: the message is optional, not variadic.
	if got := r.Dispatch(args("PING", "a", "b")); got.Kind != ReplyError {
		t.Fatalf("PING a b = %+v, want error reply", got)
	}
}

// The argument count for "SET k v EX 10" is legal by Redis's rules, so the
// rejection must name the option rather than blame the count.
func TestSetRejectsOptions(t *testing.T) {
	r, e := newTestRegistry(t)

	for _, extra := range [][]string{{"extra"}, {"EX", "10"}, {"NX"}, {"KEEPTTL"}} {
		call := append([]string{"SET", "k", "v"}, extra...)
		got := r.Dispatch(args(call...))
		if got.Kind != ReplyError {
			t.Fatalf("SET with %v: got %+v, want error reply", extra, got)
		}
		if want := "ERR syntax error"; got.Str != want {
			t.Fatalf("SET with %v: got %q, want %q", extra, got.Str, want)
		}
	}

	// A rejected SET must not have written anything.
	if _, ok := e.Get("k"); ok {
		t.Fatal("a rejected SET stored the key anyway")
	}

	// Two arguments is still wrong arity, not a syntax error: nothing has been
	// supplied to misinterpret as an option.
	got := r.Dispatch(args("SET", "k"))
	if want := "ERR wrong number of arguments for 'set' command"; got.Str != want {
		t.Fatalf("SET k: got %q, want %q", got.Str, want)
	}
}

// An unknown name is echoed exactly as sent: there is no canonical form for a
// command we do not know. See docs/protocol.md.
func TestUnknownCommand(t *testing.T) {
	r, _ := newTestRegistry(t)
	for _, sent := range []string{"NOPE", "nope", "NoPe"} {
		t.Run(sent, func(t *testing.T) {
			got := r.Dispatch(args(sent, "x"))
			if got.Kind != ReplyError {
				t.Fatalf("got %+v, want error reply", got)
			}
			if want := "ERR unknown command '" + sent + "'"; got.Str != want {
				t.Fatalf("got %q, want %q", got.Str, want)
			}
		})
	}
}

// A name may be as long as the bulk-string limit; only a bounded prefix returns.
func TestUnknownCommandNameIsBounded(t *testing.T) {
	r, _ := newTestRegistry(t)
	long := strings.Repeat("Z", maxEchoedName*4)

	got := r.Dispatch(args(long))
	if got.Kind != ReplyError {
		t.Fatalf("got %+v, want error reply", got)
	}
	want := "ERR unknown command '" + strings.Repeat("Z", maxEchoedName) + "...'"
	if got.Str != want {
		t.Fatalf("got %d bytes %q, want %d bytes", len(got.Str), got.Str, len(want))
	}

	// A name exactly at the bound is quoted whole, with no ellipsis.
	exact := strings.Repeat("Z", maxEchoedName)
	if got := r.Dispatch(args(exact)); got.Str != "ERR unknown command '"+exact+"'" {
		t.Fatalf("name of exactly maxEchoedName bytes was altered: %q", got.Str)
	}
}

// The other half: a known command has a canonical form, reported lowercased.
func TestWrongArity(t *testing.T) {
	r, _ := newTestRegistry(t)
	// PING takes an optional message, so two arguments is a legal call; three
	// is not.
	for _, sent := range []string{"PING", "ping", "PiNg"} {
		t.Run(sent, func(t *testing.T) {
			got := r.Dispatch(args(sent, "a", "b"))
			if got.Kind != ReplyError {
				t.Fatalf("got %+v, want error reply", got)
			}
			if want := "ERR wrong number of arguments for 'ping' command"; got.Str != want {
				t.Fatalf("got %q, want %q", got.Str, want)
			}
		})
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
