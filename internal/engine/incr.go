package engine

import (
	"errors"
	"math"
	"strconv"
	"time"
)

// ErrNotAnInteger reports that the key holds something that cannot be
// incremented. It is a value error, not a failure of the engine, so the command
// layer turns it into the same reply Redis gives.
var ErrNotAnInteger = errors.New("value is not an integer or out of range")

// ErrIncrOverflow reports that the result would not fit in an int64. Redis
// distinguishes this from ErrNotAnInteger with its own message, and so do we:
// collapsing them would tell a client its stored value was malformed when the
// value is fine and only the arithmetic is impossible.
var ErrIncrOverflow = errors.New("increment or decrement would overflow")

// parseStoredInt accepts exactly what Redis accepts as an incrementable value:
// "0", or an optional minus sign followed by a digit 1-9 and any further
// digits, within int64.
//
// strconv.ParseInt is deliberately not used. It accepts "+5", "07", "00" and
// "-0"; Redis rejects all four, measured against 8.10.0 rather than remembered.
// Reaching for the standard library here would produce four silent divergences
// from the oracle, in a value the client stored earlier and may not think of as
// input at all.
func parseStoredInt(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	if s == "0" {
		return 0, true
	}
	digits := s
	if s[0] == '-' {
		digits = s[1:]
	}
	// No leading zero, and therefore no "-0" and no bare "-" either.
	if digits == "" || digits[0] < '1' || digits[0] > '9' {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	// The grammar is settled above; ParseInt is left only the range check.
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// IncrBy adds delta to the integer held by key and returns the result. A key
// that is missing, or expired and not yet reclaimed, starts from zero.
//
// Any expiry the key already carries is preserved exactly. This is the case
// ADR-0004 was written about: the effect is a complete SET carrying the same
// absolute deadline, so replaying it cannot resurrect the key without its TTL.
func (e *Engine) IncrBy(key string, delta int64) (int64, error) {
	var result int64
	seq, err := func() (uint64, error) {
		defer e.guard()
		e.mu.Lock()
		defer e.mu.Unlock()
		if !e.acceptingMutations {
			return 0, ErrDraining
		}
		// readNow, not now: this path never computes a new deadline, it only
		// judges liveness and copies the deadline the key already has. So it
		// can skip the clock entirely when no key carries one, which is worth
		// about 50 ns on this hardware — the same measurement that made Get
		// five times faster in v0.2.
		now := e.readNow()

		var current int64
		// An expired key is absent to callers, so it is absent here too: the
		// count starts again rather than continuing from a value nobody could
		// have read.
		value, live := e.store.Get(key, now)
		if live {
			n, valid := parseStoredInt(value)
			if !valid {
				return 0, ErrNotAnInteger
			}
			current = n
		}

		if (delta > 0 && current > math.MaxInt64-delta) ||
			(delta < 0 && current < math.MinInt64-delta) {
			// Checked this way round because computing the sum first is exactly
			// the overflow being guarded against.
			return 0, ErrIncrOverflow
		}
		result = current + delta
		text := strconv.FormatInt(result, 10)

		// The deadline the key already holds, read as the absolute instant it
		// is, not rebuilt from the time remaining on it. The liveness check
		// matters: an expired entry may still be in the map with its old
		// deadline, and carrying that forward would create the key already
		// expired.
		var deadline time.Time
		var hasTTL bool
		if live {
			deadline, hasTTL = e.store.ExpiresAt(key)
		}

		return e.commitSet(key, text, deadline, hasTTL, func() {
			e.store.Set(key, text, deadline, hasTTL)
		})
	}()
	if err != nil {
		return 0, err
	}
	return result, e.await(seq)
}
