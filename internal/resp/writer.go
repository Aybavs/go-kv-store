package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer encodes RESP2 replies. It buffers; call Flush to deliver.
type Writer struct {
	bw  *bufio.Writer
	num []byte // scratch for integer formatting
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w), num: make([]byte, 0, 24)}
}

func (w *Writer) WriteSimpleString(s string) error { return w.prefixed('+', s) }

func (w *Writer) WriteError(s string) error { return w.prefixed('-', s) }

func (w *Writer) prefixed(tag byte, s string) error {
	if err := w.bw.WriteByte(tag); err != nil {
		return err
	}
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.crlf()
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
