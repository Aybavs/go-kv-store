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

// failingReader delivers data and then fails with a transport error, the way a
// net.Conn does on a read deadline or a connection reset.
type failingReader struct {
	data []byte
	pos  int
	err  error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.pos < len(f.data) {
		n := copy(p, f.data[f.pos:])
		f.pos += n
		return n, nil
	}
	return 0, f.err
}

// TestReadCommandErrorClasses pins the three-way contract callers dispatch on:
// a clean io.EOF between frames, a *ProtocolError for unparseable input, and a
// pass-through transport error for a broken connection. Fuzzing cannot cover
// the third class, because bytes.Reader only ever fails with io.EOF.
func TestReadCommandErrorClasses(t *testing.T) {
	transport := errors.New("simulated connection reset")

	t.Run("clean EOF between frames", func(t *testing.T) {
		r := NewReader(bytes.NewReader([]byte("*1\r\n$4\r\nPING\r\n")), DefaultLimits())
		if _, err := r.ReadCommand(); err != nil {
			t.Fatalf("first frame: %v", err)
		}
		if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
			t.Fatalf("got %#v, want io.EOF", err)
		}
	})

	t.Run("protocol error for unparseable input", func(t *testing.T) {
		r := NewReader(bytes.NewReader([]byte("+OK\r\n")), DefaultLimits())
		_, err := r.ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("got %#v, want *ProtocolError", err)
		}
	})

	for name, input := range map[string]string{
		"during the array header": "*2\r",
		"between elements":        "*2\r\n$3\r\nGET",
		"inside a bulk payload":   "*2\r\n$3\r\nGET\r\n$5\r\nab",
	} {
		t.Run("transport error "+name, func(t *testing.T) {
			r := NewReader(&failingReader{data: []byte(input), err: transport}, DefaultLimits())
			_, err := r.ReadCommand()
			if !errors.Is(err, transport) {
				t.Fatalf("got %#v, want the transport error to pass through", err)
			}
			var pe *ProtocolError
			if errors.As(err, &pe) {
				t.Fatal("transport error was misclassified as a protocol error")
			}
		})
	}
}

// TestReadCommandLargeBulkSpansChunks covers a payload larger than one
// pre-allocation chunk, so the bounded read loop runs more than once.
func TestReadCommandLargeBulkSpansChunks(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 200*1024)
	frame := append([]byte("*2\r\n$3\r\nSET\r\n$204800\r\n"), payload...)
	frame = append(frame, '\r', '\n')

	r := NewReader(bytes.NewReader(frame), DefaultLimits())
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != 2 {
		t.Fatalf("got %d args, want 2", len(args))
	}
	if !bytes.Equal(args[1], payload) {
		t.Fatalf("payload corrupted: got %d bytes, want %d", len(args[1]), len(payload))
	}
}

// TestReadCommandSlicesAreBorrowed pins the ownership invariant the rest of the
// project depends on: slices returned by ReadCommand are only valid until the
// next call. Every other test copies before retaining, so without this one a
// regression that stopped reusing the buffer would go unnoticed.
func TestReadCommandSlicesAreBorrowed(t *testing.T) {
	frames := "*2\r\n$3\r\nGET\r\n$5\r\nfirst\r\n" +
		"*2\r\n$3\r\nGET\r\n$6\r\nsecond\r\n"
	r := NewReader(bytes.NewReader([]byte(frames)), DefaultLimits())

	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("first ReadCommand: %v", err)
	}
	retained := args[1]
	if string(retained) != "first" {
		t.Fatalf("got %q, want %q", retained, "first")
	}

	if _, err := r.ReadCommand(); err != nil {
		t.Fatalf("second ReadCommand: %v", err)
	}

	if string(retained) == "first" {
		t.Fatal("slice retained across ReadCommand still holds its original value; " +
			"the decoder is no longer reusing its buffer, so the documented " +
			"borrowed-slice contract no longer describes reality")
	}
}

// TestReadCommandOversizedHeaderLine covers the bufio.ErrBufferFull path: a peer
// that streams a header line with no CRLF must be rejected rather than allowed
// to grow the buffer without bound.
func TestReadCommandOversizedHeaderLine(t *testing.T) {
	huge := append([]byte{'*'}, bytes.Repeat([]byte("9"), 128*1024)...)
	r := NewReader(bytes.NewReader(huge), DefaultLimits())
	_, err := r.ReadCommand()
	var pe *ProtocolError
	if !errors.As(err, &pe) {
		t.Fatalf("got %#v, want *ProtocolError", err)
	}
}

// FuzzReadCommand asserts two properties over arbitrary input: the decoder
// never panics, and every error it returns is either io.EOF or a
// *ProtocolError. It covers parse errors only — bytes.Reader cannot produce a
// transport failure, so the third error class is pinned by
// TestReadCommandErrorClasses instead.
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
