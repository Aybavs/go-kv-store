package command

import (
	"bytes"
	"testing"

	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/resp"
)

// Scribbles over the exact bytes the parser handed us after SET returns: if the
// store kept those slices, GET returns corruption. Single-goroutine aliasing, so
// the race detector cannot catch it.
func TestStoredDataDoesNotAliasParserBuffer(t *testing.T) {
	e := engine.New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
	r := New(e)

	frame := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	reader := resp.NewReader(bytes.NewReader([]byte(frame)), resp.DefaultLimits())

	parsed, err := reader.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if reply := r.Dispatch(parsed); reply.Kind != ReplySimple {
		t.Fatalf("SET failed: %+v", reply)
	}

	// The parser's slices are borrowed. Overwrite them.
	for i := range parsed {
		for j := range parsed[i] {
			parsed[i][j] = 'X'
		}
	}

	got, ok := e.Get("key")
	if !ok {
		t.Fatal("key vanished after the parser buffer was overwritten")
	}
	if got != "value" {
		t.Fatalf("stored value aliased the parser buffer: got %q, want %q", got, "value")
	}
}

// TestBufferReuseAcrossCommands drives two commands through one Reader so the
// second parse reuses the buffer the first one wrote into.
func TestBufferReuseAcrossCommands(t *testing.T) {
	e := engine.New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
	r := New(e)

	frames := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nfirst\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$40\r\n" + string(bytes.Repeat([]byte("Z"), 40)) + "\r\n"
	reader := resp.NewReader(bytes.NewReader([]byte(frames)), resp.DefaultLimits())

	for i := 0; i < 2; i++ {
		parsed, err := reader.ReadCommand()
		if err != nil {
			t.Fatalf("ReadCommand %d: %v", i, err)
		}
		if reply := r.Dispatch(parsed); reply.Kind != ReplySimple {
			t.Fatalf("SET %d failed: %+v", i, reply)
		}
	}

	if got, _ := e.Get("a"); got != "first" {
		t.Fatalf("first value corrupted by the second parse: got %q", got)
	}
	if got, _ := e.Get("b"); got != string(bytes.Repeat([]byte("Z"), 40)) {
		t.Fatalf("second value wrong: got %q", got)
	}
}

// TestMGetRepliesDoNotAliasParserBuffer scribbles over the parser's bytes after
// MGET returns but before the reply is read. MGET is the first command whose
// reply is built from several stored values at once, and a reply that borrowed
// the request buffer would be corrupted between dispatch and encoding — the
// server does exactly that in between, on the same goroutine, where the race
// detector sees nothing.
func TestMGetRepliesDoNotAliasParserBuffer(t *testing.T) {
	e := engine.New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
	r := New(e)

	if reply := r.Dispatch(args("SET", "key", "value")); reply.Kind != ReplySimple {
		t.Fatalf("SET failed: %+v", reply)
	}

	frame := "*3\r\n$4\r\nMGET\r\n$3\r\nkey\r\n$4\r\nnope\r\n"
	reader := resp.NewReader(bytes.NewReader([]byte(frame)), resp.DefaultLimits())
	parsed, err := reader.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}

	reply := r.Dispatch(parsed)

	for i := range parsed {
		for j := range parsed[i] {
			parsed[i][j] = 'X'
		}
	}

	if len(reply.Array) != 2 {
		t.Fatalf("MGET returned %d elements, want 2", len(reply.Array))
	}
	if reply.Array[0].Str != "value" {
		t.Fatalf("reply aliased the parser buffer: got %q, want %q", reply.Array[0].Str, "value")
	}
	if reply.Array[1].Kind != ReplyNullBulk {
		t.Fatalf("missing key returned %+v, want a null bulk", reply.Array[1])
	}

	// The lookup itself must have used an owned copy too: overwriting the key
	// bytes cannot make the stored key unreachable.
	if got, ok := e.Get("key"); !ok || got != "value" {
		t.Fatalf("stored key was disturbed by MGET: %q, %v", got, ok)
	}
}

// TestOwnedKeysCopies pins the copy, which nothing else does: aliasing the
// parser buffer here leaves the rest of the suite green.
//
// It is not harmless. engine.Delete passes the live keys to aof.DeriveDel, which
// copies the slice but not the strings in it, so an aliased key could be encoded
// by the writer goroutine after the connection reused those bytes — recording a
// key nobody deleted. Pinned on the function because that is exact, not timed.
func TestOwnedKeysCopies(t *testing.T) {
	buf := []byte("DELkeyakeyb")
	parsed := [][]byte{buf[0:3], buf[3:7], buf[7:11]}

	keys := ownedKeys(parsed)

	for i := range buf {
		buf[i] = 'X'
	}

	want := []string{"keya", "keyb"}
	if len(keys) != len(want) {
		t.Fatalf("ownedKeys returned %d keys, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("key %d aliased the parser buffer: got %q, want %q", i, keys[i], want[i])
		}
	}
}

// INCR is the second command that stores a key taken from the request, and the
// first whose value the engine builds rather than the client. Both halves are
// checked: the key must survive the buffer being reused, and the stored value
// must not alias anything the parser owns.
func TestIncrDoesNotAliasParserBuffer(t *testing.T) {
	e := engine.New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
	r := New(e)

	frame := "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"
	reader := resp.NewReader(bytes.NewReader([]byte(frame)), resp.DefaultLimits())
	parsed, err := reader.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if reply := r.Dispatch(parsed); reply.Kind != ReplyInt || reply.Int != 1 {
		t.Fatalf("INCR failed: %+v", reply)
	}

	for i := range parsed {
		for j := range parsed[i] {
			parsed[i][j] = 'X'
		}
	}

	got, ok := e.Get("counter")
	if !ok {
		t.Fatal("the key vanished after the parser buffer was overwritten")
	}
	if got != "1" {
		t.Fatalf("stored value = %q, want %q", got, "1")
	}
}
