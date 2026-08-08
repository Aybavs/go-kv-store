package aof

import (
	"errors"
	"strconv"

	"github.com/aybavs/go-kv-store/internal/resp"
)

// The codec is shared with the wire protocol; the semantics are not. Two
// decoders sit over one encoding — the client command decoder and this one —
// and neither may reach the other. Replay must never trigger client-only
// behaviour, and must never append records back into the log.
const (
	verbSet  = "SET"
	verbDel  = "DEL"
	optPXAT  = "PXAT"
	maxParts = 5 // SET key value PXAT ms
)

// ErrCorrupt marks a record that is structurally wrong rather than merely
// incomplete. Recovery treats the two completely differently: a torn tail is
// expected after a crash and is truncated, while corruption refuses to start
// the server. Anything returned from here that is not ErrCorrupt is the
// decoder's own error and keeps its meaning — in particular io.EOF still means
// the stream ended cleanly between records.
var ErrCorrupt = errors.New("aof: corrupt record")

func corruptf(msg string) error { return errors.Join(ErrCorrupt, errors.New(msg)) }

// Encode writes one record as a RESP2 array of bulk strings.
func Encode(w *resp.Writer, r Record) error {
	switch r.Kind {
	case KindSet:
		n := 3
		if r.HasExpiry {
			n = 5
		}
		if err := w.WriteArrayHeader(n); err != nil {
			return err
		}
		if err := w.WriteBulk(verbSet); err != nil {
			return err
		}
		if err := w.WriteBulk(r.Key); err != nil {
			return err
		}
		if err := w.WriteBulk(r.Value); err != nil {
			return err
		}
		if !r.HasExpiry {
			return nil
		}
		if err := w.WriteBulk(optPXAT); err != nil {
			return err
		}
		return w.WriteBulk(strconv.FormatInt(r.ExpireAtMS, 10))

	case KindDel:
		if len(r.Keys) == 0 {
			// Not reachable from DeriveDel with a real command, and an empty
			// DEL would decode as a structurally invalid record, so refuse to
			// write one rather than produce a file that cannot be read back.
			return corruptf("DEL record with no keys")
		}
		if err := w.WriteArrayHeader(len(r.Keys) + 1); err != nil {
			return err
		}
		if err := w.WriteBulk(verbDel); err != nil {
			return err
		}
		for _, k := range r.Keys {
			if err := w.WriteBulk(k); err != nil {
				return err
			}
		}
		return nil

	default:
		return corruptf("unknown record kind")
	}
}

// Decode reads one record. The error classes matter more than usual here:
//
//   - io.EOF — the stream ended cleanly between records; replay is done
//   - *resp.ProtocolError — the frame itself is malformed or ran out mid-record;
//     the caller decides whether that is a torn tail or corruption by where it
//     happened
//   - ErrCorrupt — the frame decoded but is not a record this vocabulary knows
func Decode(r *resp.Reader) (Record, error) {
	parts, err := r.ReadCommand()
	if err != nil {
		return Record{}, err
	}
	if len(parts) == 0 {
		return Record{}, corruptf("empty record")
	}

	switch verb := string(parts[0]); verb {
	case verbSet:
		switch len(parts) {
		case 3:
			return Record{Kind: KindSet, Key: string(parts[1]), Value: string(parts[2])}, nil
		case 5:
			if string(parts[3]) != optPXAT {
				return Record{}, corruptf("unknown SET option " + strconv.Quote(string(parts[3])))
			}
			ms, err := strconv.ParseInt(string(parts[4]), 10, 64)
			if err != nil {
				return Record{}, corruptf("PXAT is not an integer")
			}
			return Record{
				Kind:       KindSet,
				Key:        string(parts[1]),
				Value:      string(parts[2]),
				ExpireAtMS: ms,
				HasExpiry:  true,
			}, nil
		default:
			return Record{}, corruptf("SET record with " + strconv.Itoa(len(parts)) + " parts")
		}

	case verbDel:
		if len(parts) < 2 {
			return Record{}, corruptf("DEL record with no keys")
		}
		keys := make([]string, 0, len(parts)-1)
		for _, p := range parts[1:] {
			keys = append(keys, string(p))
		}
		return Record{Kind: KindDel, Keys: keys}, nil

	default:
		return Record{}, corruptf("unknown effect verb " + strconv.Quote(verb))
	}
}
