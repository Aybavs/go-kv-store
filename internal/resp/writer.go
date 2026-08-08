package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Writer encodes RESP2 replies. It buffers; call Flush to deliver.
//
// A write that fails partway through a reply leaves the partial bytes buffered.
// Callers must not Flush after a write error — doing so emits a truncated frame.
// Close the connection instead.
type Writer struct {
	bw  *bufio.Writer
	num []byte // scratch for integer formatting
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w), num: make([]byte, 0, 24)}
}

func (w *Writer) WriteSimpleString(s string) error { return w.prefixed('+', s) }

func (w *Writer) WriteError(s string) error { return w.prefixed('-', s) }

// prefixed writes a single-line reply: a tag, the payload, then CRLF.
//
// The payload is written with CR and LF mapped to spaces. A status or error
// line has no length prefix, so its only terminator is the CRLF this function
// appends; an embedded CR or LF would end the frame early and every following
// byte would be read as a further reply. Since error text routinely carries
// client-supplied data — a command name, for one — that is a reply-splitting
// vector, not a cosmetic concern. Redis maps the same two bytes for the same
// reason.
//
// Bulk strings are deliberately exempt: they are length-prefixed, so CR and LF
// inside them are ordinary payload bytes and must survive untouched.
func (w *Writer) prefixed(tag byte, s string) error {
	if err := w.bw.WriteByte(tag); err != nil {
		return err
	}
	if err := w.writeLineSafe(s); err != nil {
		return err
	}
	return w.crlf()
}

// writeLineSafe writes s with every CR and LF replaced by a space. It copies
// whole segments between offending bytes, so a payload containing neither —
// the overwhelmingly common case — costs one scan and one write.
func (w *Writer) writeLineSafe(s string) error {
	for {
		i := strings.IndexAny(s, "\r\n")
		if i < 0 {
			_, err := w.bw.WriteString(s)
			return err
		}
		if _, err := w.bw.WriteString(s[:i]); err != nil {
			return err
		}
		if err := w.bw.WriteByte(' '); err != nil {
			return err
		}
		s = s[i+1:]
	}
}

func (w *Writer) WriteInt(n int64) error {
	if err := w.bw.WriteByte(':'); err != nil {
		return err
	}
	return w.numberLine(n)
}

func (w *Writer) WriteBulk(s string) error {
	if err := w.bw.WriteByte('$'); err != nil {
		return err
	}
	if err := w.numberLine(int64(len(s))); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.crlf()
}

func (w *Writer) WriteNullBulk() error {
	_, err := w.bw.WriteString("$-1\r\n")
	return err
}

// WriteArrayHeader writes an array header; the caller writes n elements after
// it. Note the asymmetry with this package's decoder: n below 1 encodes an
// empty or null array, which is valid on the reply side but is rejected by
// ReadCommand, which only decodes requests.
func (w *Writer) WriteArrayHeader(n int) error {
	if err := w.bw.WriteByte('*'); err != nil {
		return err
	}
	return w.numberLine(int64(n))
}

func (w *Writer) numberLine(n int64) error {
	w.num = strconv.AppendInt(w.num[:0], n, 10)
	if _, err := w.bw.Write(w.num); err != nil {
		return err
	}
	return w.crlf()
}

func (w *Writer) crlf() error {
	_, err := w.bw.WriteString("\r\n")
	return err
}

func (w *Writer) Flush() error { return w.bw.Flush() }
