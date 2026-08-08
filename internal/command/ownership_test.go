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
