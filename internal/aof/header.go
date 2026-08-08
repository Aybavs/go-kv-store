package aof

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// The file opens with a fixed-size header so a server pointed at a foreign file
// refuses to start rather than interpreting its contents as data. Fixed size
// means recovery validates it with one read and can report a byte offset that
// means something.
//
// The reserved bytes exist so that a future field a reader could safely ignore
// does not force a format-version bump.
const (
	magic         = "GOKVAOF\x00"
	FormatVersion = uint32(1)
	HeaderSize    = 16 // 8 magic + 4 version + 4 reserved
)

var (
	// ErrNotAnAOF means the file does not begin with our magic. It is kept
	// distinct from a version mismatch because the operator response differs:
	// one is the wrong file, the other is the right file from another build.
	ErrNotAnAOF = errors.New("aof: file is not a go-kv-store append-only file")

	// ErrTruncatedHeader means the file is shorter than a header but not empty.
	// An empty file is a new log, not a damaged one, and that distinction
	// decides whether the server starts.
	ErrTruncatedHeader = errors.New("aof: header is incomplete")
)

// UnsupportedVersionError reports a file written by a different format version.
type UnsupportedVersionError struct{ Found, Supported uint32 }

func (e *UnsupportedVersionError) Error() string {
	return fmt.Sprintf("aof: format version %d is not supported (this build writes %d)", e.Found, e.Supported)
}

// EncodeHeader returns the bytes that begin every log file.
func EncodeHeader() []byte {
	buf := make([]byte, HeaderSize)
	copy(buf, magic)
	binary.BigEndian.PutUint32(buf[8:12], FormatVersion)
	return buf
}

// ReadHeader validates the start of a log. It reports whether the file was
// empty, which the caller treats as a new log rather than as a failure.
func ReadHeader(r io.Reader) (empty bool, err error) {
	buf := make([]byte, HeaderSize)
	n, err := io.ReadFull(r, buf)
	switch {
	case n == 0 && (errors.Is(err, io.EOF) || err == nil):
		return true, nil
	case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
		return false, ErrTruncatedHeader
	case err != nil:
		return false, err
	}

	if string(buf[:8]) != magic {
		return false, ErrNotAnAOF
	}
	if v := binary.BigEndian.Uint32(buf[8:12]); v != FormatVersion {
		return false, &UnsupportedVersionError{Found: v, Supported: FormatVersion}
	}
	return false, nil
}
