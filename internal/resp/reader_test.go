package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
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
