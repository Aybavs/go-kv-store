package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"testing/iotest"
)

func readAll(t *testing.T, input string) [][][]byte {
	t.Helper()
	r := NewReader(bytes.NewReader([]byte(input)), DefaultLimits())
	var out [][][]byte
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return out
		}
		// Copy, because the returned slices are borrowed.
		cp := make([][]byte, len(args))
		for i, a := range args {
			cp[i] = append([]byte(nil), a...)
		}
		out = append(out, cp)
	}
}

func TestReadCommandSingle(t *testing.T) {
	got := readAll(t, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
	want := [][][]byte{{[]byte("GET"), []byte("foo")}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReadCommandPipelined(t *testing.T) {
	got := readAll(t, "*1\r\n$4\r\nPING\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
	want := [][][]byte{
		{[]byte("PING")},
		{[]byte("SET"), []byte("a"), []byte("1")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReadCommandEmptyBulkAndBinarySafe(t *testing.T) {
	got := readAll(t, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\x00\r\x00\r\n")
	want := [][][]byte{{[]byte("SET"), []byte("k"), []byte("a\x00\r\x00")}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestReadCommandTruncatedHeaderIsProtocolError covers a stream that ends
// part-way through a header line. bufio.Reader.ReadSlice returns the partial
// bytes alongside io.EOF, so without an explicit check those bytes are dropped
// and a truncated frame is misreported as a clean disconnect.
func TestReadCommandTruncatedHeaderIsProtocolError(t *testing.T) {
	cases := map[string]string{
		"array header truncated after CR": "*2\r",
		"array header with no CRLF":       "*2",
		"bulk header truncated after CR":  "*1\r\n$5\r",
		"bulk header with no CRLF":        "*1\r\n$5",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewReader(bytes.NewReader([]byte(input)), DefaultLimits())
			_, err := r.ReadCommand()
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("want *ProtocolError, got %#v", err)
			}
		})
	}
}

// TestReadCommandEmptyStreamIsCleanEOF is the boundary case the fix must not
// break: nothing buffered at all is a legitimate disconnect between frames.
func TestReadCommandEmptyStreamIsCleanEOF(t *testing.T) {
	r := NewReader(bytes.NewReader(nil), DefaultLimits())
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF on an empty stream, got %#v", err)
	}
}

// TestReadCommandFragmentedAtEveryBoundary feeds the same frame one byte at a
// time. TCP may deliver a frame split at any offset, so the decoder must never
// depend on a whole frame arriving in one read.
func TestReadCommandFragmentedAtEveryBoundary(t *testing.T) {
	const frame = "*3\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
	r := NewReader(iotest.OneByteReader(bytes.NewReader([]byte(frame))), DefaultLimits())

	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	// Safe to compare args directly: ReadCommand is called once, so nothing
	// reuses the decoder's buffer before this comparison.
	want := [][]byte{[]byte("SET"), []byte("hello"), []byte("world")}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("got %q, want %q", args, want)
	}
}

func TestReadCommandCleanEOFBetweenFrames(t *testing.T) {
	r := NewReader(bytes.NewReader([]byte("*1\r\n$4\r\nPING\r\n")), DefaultLimits())
	if _, err := r.ReadCommand(); err != nil {
		t.Fatalf("first ReadCommand: %v", err)
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("want io.EOF between frames, got %v", err)
	}
}

func TestReadCommandMalformed(t *testing.T) {
	cases := map[string]string{
		"not an array":          "+OK\r\n",
		"inline command":        "PING\r\n",
		"negative multibulk":    "*-1\r\n",
		"zero multibulk":        "*0\r\n",
		"non-numeric multibulk": "*x\r\n",
		"element not bulk":      "*1\r\n+OK\r\n",
		"negative bulk length":  "*1\r\n$-1\r\n",
		"bad bulk terminator":   "*1\r\n$3\r\nabcXX",
		"truncated mid-bulk":    "*1\r\n$5\r\nab",
		"lf without cr":         "*1\n",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			r := NewReader(bytes.NewReader([]byte(input)), DefaultLimits())
			_, err := r.ReadCommand()
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("want *ProtocolError, got %#v", err)
			}
		})
	}
}

func TestReadCommandLimits(t *testing.T) {
	small := Limits{MaxArrayElements: 2, MaxBulkLength: 4}

	t.Run("too many elements", func(t *testing.T) {
		r := NewReader(bytes.NewReader([]byte("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")), small)
		_, err := r.ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("want *ProtocolError, got %#v", err)
		}
	})

	t.Run("bulk too long", func(t *testing.T) {
		r := NewReader(bytes.NewReader([]byte("*1\r\n$5\r\nabcde\r\n")), small)
		_, err := r.ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("want *ProtocolError, got %#v", err)
		}
	})

	t.Run("element count exactly at limit is accepted", func(t *testing.T) {
		r := NewReader(bytes.NewReader([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n")), small)
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("ReadCommand: %v", err)
		}
		want := [][]byte{[]byte("a"), []byte("b")}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("got %q, want %q", args, want)
		}
	})

	t.Run("bulk length exactly at limit is accepted", func(t *testing.T) {
		r := NewReader(bytes.NewReader([]byte("*1\r\n$4\r\nabcd\r\n")), small)
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("ReadCommand: %v", err)
		}
		want := [][]byte{[]byte("abcd")}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("got %q, want %q", args, want)
		}
	})
}

// FuzzReadCommand asserts two properties over arbitrary input: the decoder
// never panics, and every error it returns honours the package's contract —
// either a between-frames io.EOF or a *ProtocolError, never a raw stdlib
// error. The second property matters because leaking a raw io.EOF for a
// truncated frame is a real bug this package has already had once.
func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$0\r\n\r\n"))
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\x00\r\x00\r\n"))
	f.Add([]byte("*1\r\n$99999999999999999999\r\n"))
	f.Add([]byte("*x\r\n"))
	f.Add([]byte("$-1\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data), DefaultLimits())
		for i := 0; i < 32; i++ {
			_, err := r.ReadCommand()
			if err == nil {
				continue
			}
			var pe *ProtocolError
			if errors.Is(err, io.EOF) || errors.As(err, &pe) {
				return
			}
			t.Fatalf("ReadCommand returned %#v; want io.EOF or *ProtocolError", err)
		}
	})
}
