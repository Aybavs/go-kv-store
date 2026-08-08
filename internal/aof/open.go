package aof

import (
	"fmt"
	"os"
	"time"
)

// OpenFile recovers an existing log and returns a Log ready to append to.
//
// The order matters: replay first, then truncate to the last complete record,
// then open for append at that offset. A torn tail is overwritten rather than
// left in place, so the next crash cannot find two half-records.
func OpenFile(path string, policy Policy, now time.Time, dst Applier, onFatal func(error)) (*Log, Result, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, Result{}, err
	}

	res, err := Replay(f, now, dst)
	if err != nil {
		_ = f.Close()
		return nil, res, err
	}

	if res.LastValidOffset == 0 {
		// A new file: write the header before anything else, so the next start
		// can tell this file apart from a foreign one.
		if _, err := f.WriteAt(EncodeHeader(), 0); err != nil {
			_ = f.Close()
			return nil, res, err
		}
		res.LastValidOffset = HeaderSize
	}

	if err := f.Truncate(res.LastValidOffset); err != nil {
		_ = f.Close()
		return nil, res, fmt.Errorf("aof: truncating to the last complete record at %d: %w", res.LastValidOffset, err)
	}
	if _, err := f.Seek(res.LastValidOffset, 0); err != nil {
		_ = f.Close()
		return nil, res, err
	}

	return Open(f, policy, onFatal), res, nil
}
