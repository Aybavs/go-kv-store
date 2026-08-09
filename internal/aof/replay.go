package aof

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aybavs/go-kv-store/internal/resp"
)

// Applier is what replay writes into. It is declared here, in terms of what
// replay actually needs, so this package still does not know that store or
// engine exist — the same reason the lock order holds.
type Applier interface {
	Set(key, value string, expiresAt time.Time, hasTTL bool)
	Delete(key string) bool
}

// Result describes what replay found.
type Result struct {
	Records int
	// LastValidOffset is where the last complete record ended. The file is
	// reopened for append here, so a torn tail is overwritten rather than left
	// in place.
	LastValidOffset int64
	// Truncated reports that the file ended part-way through a record. That is
	// the expected shape of a crash, not a fault.
	Truncated bool
}

// CorruptionError refuses the file. It carries the offset because "the log is
// corrupt" is not something an operator can act on and "the log is corrupt at
// byte 4096" is.
type CorruptionError struct {
	Offset int64
	Err    error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("aof: corrupt record at byte offset %d: %v", e.Offset, e.Err)
}
func (e *CorruptionError) Unwrap() error { return e.Err }

// countingReader tracks how much of the stream has been handed to the decoder,
// so an exact offset can be recovered by subtracting what is still buffered.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Replay reads the log and applies its effects. One now for every deadline
// comparison, so a key cannot expire part-way through a long replay.
//
// Three outcomes, deliberately different:
//
//   - clean end between records: fully replayed
//   - end part-way through a record: a torn tail, truncated to the last
//     complete boundary — that is what a crash produces
//   - anything structurally wrong: refuse, rather than load a state we cannot
//     justify
func Replay(src io.Reader, now time.Time, dst Applier) (Result, error) {
	counted := &countingReader{r: src}

	empty, err := ReadHeader(counted)
	if err != nil {
		return Result{}, err
	}
	if empty {
		return Result{}, nil
	}

	r := resp.NewReader(counted, resp.DefaultLimits())
	res := Result{LastValidOffset: HeaderSize}

	for {
		rec, err := Decode(r)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				// Between records: the log ended where it should.
				return res, nil
			case errors.Is(err, resp.ErrIncompleteFrame):
				res.Truncated = true
				return res, nil
			default:
				return res, &CorruptionError{Offset: res.LastValidOffset, Err: err}
			}
		}

		if err := apply(rec, now, dst); err != nil {
			return res, &CorruptionError{Offset: res.LastValidOffset, Err: err}
		}
		res.Records++
		res.LastValidOffset = counted.n - int64(r.Buffered())
	}
}

// apply is spec §6.8's four rules.
func apply(rec Record, now time.Time, dst Applier) error {
	switch rec.Kind {
	case KindSet:
		if !rec.HasExpiry {
			dst.Set(rec.Key, rec.Value, time.Time{}, false)
			return nil
		}
		deadline := time.UnixMilli(rec.ExpireAtMS)
		if now.Before(deadline) {
			dst.Set(rec.Key, rec.Value, deadline, true)
			return nil
		}
		// Rule 3, and the one that is natural to get wrong. An expired record
		// is not skipped — it must leave the key ABSENT. Skipping would
		// resurrect whatever value this record replaced: "SET k old" followed
		// by an expired "SET k new PXAT T" has to leave k gone, not holding
		// old.
		dst.Delete(rec.Key)
		return nil

	case KindDel:
		for _, k := range rec.Keys {
			dst.Delete(k)
		}
		return nil

	default:
		return corruptf("unknown record kind in a decoded record")
	}
}
