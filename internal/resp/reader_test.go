package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
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

// TestReadCommandTotalSizeLimit covers the limit that bounds the product of the
// other two. Each element below is well inside MaxBulkLength and the element
// count is well inside MaxArrayElements, so nothing here is caught by the
// per-element limits — which is exactly the gap: without a total, 1024
// arguments of 64 MiB is a 64 GiB frame that both per-element limits accept.
func TestReadCommandTotalSizeLimit(t *testing.T) {
	// Room for 10 payload bytes in total, with neither per-element limit in
	// the way.
	total := Limits{MaxArrayElements: 16, MaxBulkLength: 64, MaxCommandBytes: 10}

	frame := func(args ...string) []byte {
		var b []byte
		b = append(b, []byte("*"+strconv.Itoa(len(args))+"\r\n")...)
		for _, a := range args {
			b = append(b, []byte("$"+strconv.Itoa(len(a))+"\r\n"+a+"\r\n")...)
		}
		return b
	}

	t.Run("sum over the limit is refused", func(t *testing.T) {
		// 4 x 3 bytes = 12 > 10, but every element is far below MaxBulkLength.
		r := NewReader(bytes.NewReader(frame("aaa", "bbb", "ccc", "ddd")), total)
		_, err := r.ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("want *ProtocolError, got %#v", err)
		}
	})

	t.Run("sum exactly at the limit is accepted", func(t *testing.T) {
		r := NewReader(bytes.NewReader(frame("aaaaa", "bbbbb")), total) // 10
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("ReadCommand: %v", err)
		}
		want := [][]byte{[]byte("aaaaa"), []byte("bbbbb")}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("got %q, want %q", args, want)
		}
	})

	t.Run("one byte over the limit is refused", func(t *testing.T) {
		r := NewReader(bytes.NewReader(frame("aaaaa", "bbbbbb")), total) // 11
		_, err := r.ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("want *ProtocolError, got %#v", err)
		}
	})

	// The rejection must happen on the declared length, before the payload is
	// read. Otherwise the limit bounds nothing: the bytes it is meant to keep
	// out have already been buffered by the time it fires. The reader is given
	// a header promising 1 MiB and nothing else at all, so a decoder that reads
	// first cannot return a protocol error — it can only block or report a
	// truncated stream.
	t.Run("refused before the payload is read", func(t *testing.T) {
		// MaxBulkLength is deliberately huge here so that it cannot be the
		// check that fires; only the total can reject this frame.
		roomy := Limits{MaxArrayElements: 16, MaxBulkLength: 1 << 30, MaxCommandBytes: 10}
		header := []byte("*1\r\n$1048576\r\n")
		r := NewReader(bytes.NewReader(header), roomy)
		_, err := r.ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Fatalf("want *ProtocolError, got %#v", err)
		}
		if !strings.Contains(pe.Msg, "command exceeds limit") {
			t.Fatalf("got %q, want the total-size limit to fire, not a truncation error", pe.Msg)
		}
	})

	t.Run("zero disables the check", func(t *testing.T) {
		off := Limits{MaxArrayElements: 16, MaxBulkLength: 64, MaxCommandBytes: 0}
		r := NewReader(bytes.NewReader(frame("aaa", "bbb", "ccc", "ddd")), off)
		if _, err := r.ReadCommand(); err != nil {
			t.Fatalf("ReadCommand with the check disabled: %v", err)
		}
	})
}

// TestReaderReleasesOversizedBuffer pins that a connection does not carry the
// peak of one large command for the rest of its life. MaxCommandBytes bounds
// what a single command may allocate; this bounds what a connection may park.
// Without it a client sends one maximum-size command, then sits idle holding
// that memory, and the limit bounds far less than it appears to.
func TestReaderReleasesOversizedBuffer(t *testing.T) {
	big := strings.Repeat("z", 4*maxRetainedBuffer)
	frame := "*1\r\n$" + strconv.Itoa(len(big)) + "\r\n" + big + "\r\n"
	small := "*1\r\n$4\r\nPING\r\n"

	r := NewReader(strings.NewReader(frame+small+small), Limits{
		MaxArrayElements: 16,
		MaxBulkLength:    1 << 30,
		MaxCommandBytes:  1 << 30,
	})

	if _, err := r.ReadCommand(); err != nil {
		t.Fatalf("large command: %v", err)
	}
	grown := cap(r.buf)
	if grown < len(big) {
		t.Fatalf("buffer capacity %d did not grow to hold the %d-byte argument", grown, len(big))
	}

	// The next decode is where the release happens: the slices handed out for
	// the command above stay valid until then.
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("following small command: %v", err)
	}
	if string(args[0]) != "PING" {
		t.Fatalf("got %q, want PING", args[0])
	}
	if cap(r.buf) > maxRetainedBuffer {
		t.Fatalf("buffer still holds %d bytes after a small command, want at most %d",
			cap(r.buf), maxRetainedBuffer)
	}

	// Decoding must still work after the release, not just be smaller.
	args, err = r.ReadCommand()
	if err != nil {
		t.Fatalf("second small command after release: %v", err)
	}
	if string(args[0]) != "PING" {
		t.Fatalf("got %q, want PING", args[0])
	}
}

// TestReaderKeepsOrdinaryBuffer is the other half: the release must not throw
// away the reuse that ordinary traffic depends on, or every command would
// allocate a fresh buffer.
func TestReaderKeepsOrdinaryBuffer(t *testing.T) {
	frames := strings.Repeat("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n", 3)
	r := NewReader(strings.NewReader(frames), DefaultLimits())

	if _, err := r.ReadCommand(); err != nil {
		t.Fatalf("first command: %v", err)
	}
	first := cap(r.buf)
	if first == 0 {
		t.Fatal("buffer was not allocated at all")
	}
	for i := 2; i <= 3; i++ {
		if _, err := r.ReadCommand(); err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if cap(r.buf) != first {
			t.Fatalf("command %d reallocated an ordinary-sized buffer: cap %d, want %d",
				i, cap(r.buf), first)
		}
	}
}

// TestDefaultLimitsBoundTheProduct pins the relationship between the defaults
// rather than their values: whatever they are, the total must not be reachable
// by multiplying the other two, and must still admit one maximum-size value.
func TestDefaultLimitsBoundTheProduct(t *testing.T) {
	l := DefaultLimits()

	if l.MaxCommandBytes <= 0 {
		t.Fatal("MaxCommandBytes must be set in DefaultLimits, or the server has no total bound")
	}
	if l.MaxCommandBytes >= l.MaxArrayElements*l.MaxBulkLength {
		t.Fatalf("MaxCommandBytes (%d) does not bound MaxArrayElements*MaxBulkLength (%d)",
			l.MaxCommandBytes, l.MaxArrayElements*l.MaxBulkLength)
	}
	if l.MaxCommandBytes <= l.MaxBulkLength {
		t.Fatalf("MaxCommandBytes (%d) leaves no room for a maximum-size bulk (%d) plus its key and command name",
			l.MaxCommandBytes, l.MaxBulkLength)
	}
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
