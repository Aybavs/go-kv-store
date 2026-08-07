// Package resp implements the subset of the RESP2 protocol used by go-kv-store,
// for both the network wire format and (from v0.3) the append-only file.
package resp

import (
	"bufio"
	"errors"
	"io"
	"slices"
)

// Limits bound what the decoder will accept from an untrusted peer.
type Limits struct {
	MaxArrayElements int
	MaxBulkLength    int
}

// DefaultLimits returns the limits used by the server unless overridden.
func DefaultLimits() Limits {
	return Limits{
		MaxArrayElements: 1024,
		MaxBulkLength:    64 << 20, // 64 MiB
	}
}

// ProtocolError means the stream cannot be resynchronised. The caller must
// close the connection; there is no reliable point to resume parsing from.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "protocol error: " + e.Msg }

func protoErr(msg string) error { return &ProtocolError{Msg: msg} }

// Reader decodes RESP2 request frames from a stream.
//
// The slices returned by ReadCommand are borrowed: they point into an internal
// buffer that is reused by the next call. Callers that retain data beyond the
// current command must copy it.
type Reader struct {
	br      *bufio.Reader
	limits  Limits
	buf     []byte
	offsets [][2]int
	args    [][]byte
}

func NewReader(r io.Reader, limits Limits) *Reader {
	return &Reader{
		br:     bufio.NewReader(r),
		limits: limits,
	}
}

// ReadCommand reads one request frame: a RESP2 array of bulk strings.
// It returns io.EOF when the peer closed the connection cleanly between frames.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, protoErr("expected '*' at start of request")
	}
	n, err := parseInt(line[1:])
	if err != nil {
		return nil, protoErr("invalid multibulk length")
	}
	if n <= 0 {
		return nil, protoErr("invalid multibulk length")
	}
	if n > r.limits.MaxArrayElements {
		return nil, protoErr("multibulk length exceeds limit")
	}

	r.buf = r.buf[:0]
	r.offsets = r.offsets[:0]

	for i := 0; i < n; i++ {
		if err := r.readBulk(); err != nil {
			return nil, err
		}
	}

	// Materialise slices only after the buffer has stopped growing, so that a
	// reallocation cannot leave earlier arguments pointing at a stale array.
	r.args = r.args[:0]
	for _, o := range r.offsets {
		r.args = append(r.args, r.buf[o[0]:o[1]])
	}
	return r.args, nil
}

func (r *Reader) readBulk() error {
	line, err := r.readLine()
	if err != nil {
		return bulkReadErr(err)
	}
	if len(line) == 0 || line[0] != '$' {
		return protoErr("expected '$' at start of bulk string")
	}
	ln, err := parseInt(line[1:])
	if err != nil || ln < 0 {
		return protoErr("invalid bulk length")
	}
	if ln > r.limits.MaxBulkLength {
		return protoErr("bulk length exceeds limit")
	}

	start := len(r.buf)
	r.buf = slices.Grow(r.buf, ln+2)
	r.buf = r.buf[:start+ln+2]
	if _, err := io.ReadFull(r.br, r.buf[start:start+ln+2]); err != nil {
		return bulkReadErr(err)
	}
	if r.buf[start+ln] != '\r' || r.buf[start+ln+1] != '\n' {
		return protoErr("bulk string not terminated by CRLF")
	}
	r.buf = r.buf[:start+ln] // drop the trailing CRLF
	r.offsets = append(r.offsets, [2]int{start, start + ln})
	return nil
}

// bulkReadErr maps a mid-frame EOF to a protocol error: a clean EOF is only
// legal between frames, never inside one.
func bulkReadErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return protoErr("unexpected end of stream inside frame")
	}
	return err
}

// readLine reads one CRLF-terminated header line. Header lines are short by
// construction, so exceeding the bufio buffer is itself a protocol violation.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErr("header line too long")
		}
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErr("header line not terminated by CRLF")
	}
	return line[:len(line)-2], nil
}

// parseInt parses a base-10 signed integer without allocating.
func parseInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty integer")
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, errors.New("lone minus sign")
		}
	}
	n := 0
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, errors.New("non-digit in integer")
		}
		n = n*10 + int(c-'0')
		if n > 1<<40 { // far above any legal limit; prevents overflow
			return 0, errors.New("integer too large")
		}
	}
	if neg {
		n = -n
	}
	return n, nil
}
