package resp

import (
	"bytes"
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
