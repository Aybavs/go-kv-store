package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriterTypes(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*Writer) error
		want string
	}{
		{"simple string", func(w *Writer) error { return w.WriteSimpleString("OK") }, "+OK\r\n"},
		{"error", func(w *Writer) error { return w.WriteError("ERR nope") }, "-ERR nope\r\n"},
		{"integer", func(w *Writer) error { return w.WriteInt(42) }, ":42\r\n"},
		{"negative integer", func(w *Writer) error { return w.WriteInt(-2) }, ":-2\r\n"},
		{"bulk", func(w *Writer) error { return w.WriteBulk("foo") }, "$3\r\nfoo\r\n"},
		{"empty bulk", func(w *Writer) error { return w.WriteBulk("") }, "$0\r\n\r\n"},
		{"binary bulk", func(w *Writer) error { return w.WriteBulk("a\x00\r\n") }, "$4\r\na\x00\r\n\r\n"},
		{"null bulk", func(w *Writer) error { return w.WriteNullBulk() }, "$-1\r\n"},
		{"array header", func(w *Writer) error { return w.WriteArrayHeader(2) }, "*2\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := tc.fn(w); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// A status or error line ends at the first CRLF, so a CR or LF in the payload
// would terminate the frame early and turn the rest into replies the client
// never asked for. Each case asserts exactly one CRLF, at the end.
func TestWriterLineRepliesCannotSplitFrames(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*Writer) error
		want string
	}{
		{
			"error with CRLF",
			func(w *Writer) error { return w.WriteError("ERR unknown command 'A\r\n+INJECTED'") },
			"-ERR unknown command 'A  +INJECTED'\r\n",
		},
		{
			"error with lone LF",
			func(w *Writer) error { return w.WriteError("ERR a\nb") },
			"-ERR a b\r\n",
		},
		{
			"error with lone CR",
			func(w *Writer) error { return w.WriteError("ERR a\rb") },
			"-ERR a b\r\n",
		},
		{
			"error ending in CRLF",
			func(w *Writer) error { return w.WriteError("ERR trailing\r\n") },
			"-ERR trailing  \r\n",
		},
		{
			"status with CRLF",
			func(w *Writer) error { return w.WriteSimpleString("O\r\nK") },
			"+O  K\r\n",
		},
		{
			"consecutive newlines",
			func(w *Writer) error { return w.WriteError("a\n\n\nb") },
			"-a   b\r\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := tc.fn(w); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			got := buf.String()
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
			// The framing property itself, stated independently of the exact
			// bytes above: one terminator, and it is the last thing written.
			if n := strings.Count(got, "\r\n"); n != 1 {
				t.Fatalf("got %d CRLF terminators in %q, want exactly 1", n, got)
			}
			if !strings.HasSuffix(got, "\r\n") {
				t.Fatalf("%q does not end with CRLF", got)
			}
		})
	}
}

// The other half: bulk strings are length-prefixed, so CR and LF inside them
// are payload and must survive.
func TestWriterBulkPreservesCRLF(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteBulk("a\r\nb"); err != nil {
		t.Fatalf("WriteBulk: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got, want := buf.String(), "$4\r\na\r\nb\r\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestWriterArrayFraming pins the exact bytes of an MGET-shaped array reply
// against a hand-written expectation. It checks the encoder alone; see
// TestWriterDecoderRoundTrip for encoder/decoder agreement.
func TestWriterArrayFraming(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteArrayHeader(2); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("one"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("two"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestWriterNestedArrayFraming pins the recursive RESP2 shape used by SCAN:
// the cursor is a bulk string and the key page is an inner array of bulks.
func TestWriterNestedArrayFraming(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteArrayHeader(2); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("1"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteArrayHeader(2); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("a"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteBulk("b"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "*2\r\n$1\r\n1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got := buf.String(); got != want {
		t.Fatalf("nested array = %q, want %q", got, want)
	}
}

// TestWriterDecoderRoundTrip encodes an array of bulk strings and reads it back
// through this package's own decoder. This is the property the append-only file
// will rely on later: records written by the encoder must be parseable by the
// same decoder that reads the network, including for empty and binary payloads.
func TestWriterDecoderRoundTrip(t *testing.T) {
	values := []string{"one", "two", "", "bin\x00\r\nary"}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteArrayHeader(len(values)); err != nil {
		t.Fatalf("WriteArrayHeader: %v", err)
	}
	for _, v := range values {
		if err := w.WriteBulk(v); err != nil {
			t.Fatalf("WriteBulk(%q): %v", v, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	r := NewReader(bytes.NewReader(buf.Bytes()), DefaultLimits())
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand: %v", err)
	}
	if len(args) != len(values) {
		t.Fatalf("decoded %d elements, want %d", len(args), len(values))
	}
	for i, want := range values {
		if string(args[i]) != want {
			t.Fatalf("element %d: got %q, want %q", i, args[i], want)
		}
	}
}
