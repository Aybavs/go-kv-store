// Package resp implements the subset of the RESP2 protocol used by go-kv-store,
// for both the network wire format and (from v0.3) the append-only file.
package resp

import (
	"bufio"
	"errors"
	"io"
)

// Limits bound what the decoder will accept from an untrusted peer. The first
// two bound one dimension each and multiply; MaxCommandBytes bounds the product.
// See docs/protocol.md.
type Limits struct {
	MaxArrayElements int
	MaxBulkLength    int
	// Total argument bytes per frame. Zero disables the check.
	MaxCommandBytes int
}

// DefaultLimits returns the limits used by the server unless overridden.
func DefaultLimits() Limits {
	return Limits{
		MaxArrayElements: 1024,
		MaxBulkLength:    64 << 20,  // 64 MiB
		MaxCommandBytes:  128 << 20, // twice MaxBulkLength, so a maximum-size value still fits
	}
}

// A peer can declare a huge bulk length in a dozen bytes, so reserve only this
// much before the bytes actually arrive.
const maxPreallocation = 64 << 10 // 64 KiB

// Decode buffer a connection may keep between commands; anything larger is
// released so one big request cannot park memory for the life of the connection.
const maxRetainedBuffer = 1 << 20 // 1 MiB

// ProtocolError means the stream cannot be resynchronised. The caller must
// close the connection; there is no reliable point to resume parsing from.
type ProtocolError struct {
	Msg string
	// incomplete distinguishes a frame that ran out of bytes from one that was
	// malformed. Both break a connection identically, so the wire protocol has
	// no use for the difference — but a reader recovering a file does: a frame
	// that simply ended is the expected result of a crash, while a malformed
	// one means the file cannot be trusted. Exposed through ErrIncompleteFrame
	// rather than by matching on Msg, which would break the moment someone
	// rewords it.
	incomplete bool
}

func (e *ProtocolError) Error() string { return "protocol error: " + e.Msg }

// ErrIncompleteFrame reports that a frame ended part-way through, as opposed to
// being structurally wrong. Test with errors.Is.
var ErrIncompleteFrame = errors.New("incomplete frame")

func (e *ProtocolError) Is(target error) bool {
	return target == ErrIncompleteFrame && e.incomplete
}

func protoErr(msg string) error { return &ProtocolError{Msg: msg} }

func incompleteErr(msg string) error { return &ProtocolError{Msg: msg, incomplete: true} }

// Reader decodes RESP2 request frames from a stream.
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

// Buffered reports how many bytes have been read from the underlying reader but
// not yet turned into a decoded frame.
//
// A caller that also counts what the underlying reader delivered can subtract
// the two to get the exact offset of the last complete frame — which is what
// recovery needs to truncate a torn tail at the right place. Note this is a
// different question from the one Buffered cannot answer for the connection
// handler: "how many bytes are left over" is exact, while "is a complete next
// command pending" is not.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one request frame: a RESP2 array of bulk strings. The
// returned slices point into a buffer the next call reuses; copy what you keep.
//
// Callers must distinguish three error classes:
//
//   - io.EOF — clean close between frames. Close without replying. A stream that
//     ends mid-frame is a *ProtocolError, never this.
//   - *ProtocolError — unparseable; cannot resynchronise. Reply, then close.
//   - anything else — transport failure. Close without replying.
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

	// Reslicing to zero keeps the capacity, so an oversized buffer is dropped
	// instead. Done here, not after the previous call: the slices handed out
	// then are borrowed from it and stay valid until now.
	if cap(r.buf) > maxRetainedBuffer {
		r.buf = nil
	}
	r.buf = r.buf[:0]
	r.offsets = r.offsets[:0]

	for i := 0; i < n; i++ {
		if err := r.readBulk(); err != nil {
			return nil, err
		}
	}

	// Only now that the buffer has stopped growing: a reallocation would leave
	// earlier arguments pointing at a stale array.
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
	// Checked against the declared length before any payload is read, so an
	// oversized frame costs nothing to reject. Subtraction rather than
	// len(r.buf)+ln, which a large declared length could overflow.
	if r.limits.MaxCommandBytes > 0 && ln > r.limits.MaxCommandBytes-len(r.buf) {
		return protoErr("command exceeds limit")
	}

	start := len(r.buf)
	// Bounded chunks, so memory grows with data that arrived rather than with
	// the peer's claim about what is coming.
	for remaining := ln + 2; remaining > 0; {
		chunk := remaining
		if chunk > maxPreallocation {
			chunk = maxPreallocation
		}
		at := len(r.buf)
		r.growBuf(at + chunk)
		r.buf = r.buf[:at+chunk]
		if _, err := io.ReadFull(r.br, r.buf[at:at+chunk]); err != nil {
			return bulkReadErr(err)
		}
		remaining -= chunk
	}
	if r.buf[start+ln] != '\r' || r.buf[start+ln+1] != '\n' {
		return protoErr("bulk string not terminated by CRLF")
	}
	r.buf = r.buf[:start+ln] // drop the trailing CRLF
	r.offsets = append(r.offsets, [2]int{start, start + ln})
	return nil
}

// growBuf makes room for need bytes, doubling as slices.Grow would but never
// past MaxCommandBytes.
//
// The cap is about peak memory, not about the limit itself — that is already
// enforced against the declared length before any payload is read. During a
// grow both arrays are live while the copy runs, so an unbounded doubling makes
// the peak the old array plus twice the limit: measured at v0.1.1, a 128 MiB
// limit peaked at 417 MB resident, about three times the number the operator
// configured.
//
// A new array capped at the limit cannot be the larger of the two, so the peak
// becomes old plus limit instead. It costs one extra copy in the case where the
// doubling would have overshot, on a path that is already reading tens of
// megabytes off a socket.
func (r *Reader) growBuf(need int) {
	if cap(r.buf) >= need {
		return
	}
	size := 2 * cap(r.buf)
	if size < need {
		size = need
	}
	if limit := r.limits.MaxCommandBytes; limit > 0 && size > limit {
		// max(limit, need), not limit. The declared payload length is checked
		// against the limit before any byte is read, but the CRLF that
		// terminates it is read into the same buffer, so the room actually
		// needed can exceed the limit by those two bytes. Capping to the limit
		// flatly produced a slice shorter than the caller was about to index —
		// caught by TestReadCommandTotalSizeLimit rather than by reading.
		size = limit
		if need > size {
			size = need
		}
	}
	grown := make([]byte, len(r.buf), size)
	copy(grown, r.buf)
	r.buf = grown
}

// bulkReadErr maps a mid-frame EOF to a protocol error: a clean EOF is only
// legal between frames, never inside one.
func bulkReadErr(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return incompleteErr("unexpected end of stream inside frame")
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
		// ReadSlice hands back whatever it had buffered alongside the error.
		// Bytes present here mean the stream ended part-way through a header
		// line — a truncated frame, not a clean disconnect between frames.
		if len(line) > 0 && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)) {
			return nil, incompleteErr("unexpected end of stream inside frame")
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
		if n > 1<<30 { // far above any legal limit, and fits a 32-bit int
			return 0, errors.New("integer too large")
		}
	}
	if neg {
		n = -n
	}
	return n, nil
}
