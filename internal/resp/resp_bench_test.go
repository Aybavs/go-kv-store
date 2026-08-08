package resp

import (
	"io"
	"testing"
)

// repeatReader replays the same frame forever without allocating per iteration.
type repeatReader struct {
	data []byte
	pos  int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos == len(r.data) {
		r.pos = 0
	}
	return n, nil
}

func BenchmarkReadCommand(b *testing.B) {
	frame := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
	r := NewReader(&repeatReader{data: frame}, DefaultLimits())
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadCommand(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteBulk(b *testing.B) {
	w := NewWriter(io.Discard)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteBulk("myvalue"); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkWriteError covers the line-reply path, which scans its payload for
// CR and LF before writing. Error replies are on the failure path rather than
// the hot path, but the scan is the only per-byte work the encoder does, so its
// cost is worth having a number for rather than an assumption.
func BenchmarkWriteError(b *testing.B) {
	w := NewWriter(io.Discard)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.WriteError("ERR unknown command 'totallybogus'"); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		b.Fatal(err)
	}
}
