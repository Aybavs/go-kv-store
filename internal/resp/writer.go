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

// prefixed writes a single-line reply, with CR and LF in the payload mapped to
// spaces. These lines have no length prefix, so an embedded CR or LF would end
// the frame early and split the reply. Bulk strings are exempt; see
// docs/architecture.md, "Reply framing".
func (w *Writer) prefixed(tag byte, s string) error {
	if err := w.bw.WriteByte(tag); err != nil {
		return err
	}
	if err := w.writeLineSafe(s); err != nil {
		return err
	}
	return w.crlf()
}

// writeLineSafe copies whole segments between offending bytes, so the common
// case of a payload with neither costs one scan and one write.
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
// it. n below 1 is a valid reply but is rejected by ReadCommand, which decodes
// requests only.
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
