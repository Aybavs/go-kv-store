// Package store holds the in-memory key-value data structure.
//
// Store is deliberately passive: it has no mutex, no goroutines, no I/O, no
// clock reads and no protocol knowledge. All synchronisation is owned by
// package engine. Time enters as a parameter, which is what keeps every TTL
// test here deterministic — no sleeping, no clock abstraction.
package store

import "time"

// TTLStatus distinguishes the three answers a TTL query can have, which an
// error value or a sentinel duration would collapse.
type TTLStatus int

const (
	NoKey TTLStatus = iota
	NoTTL
	HasTTL
)

// Store maps keys to values. Values are strings because Go strings are
// immutable and binary-safe, which removes the aliasing bug class by
// construction.
//
// Keys carrying a deadline appear in expires as well as data. That index is
// what makes bounded reclamation viable: sampling the main map would be
// useless when TTL-bearing keys are sparse.
//
// Invariant, maintained here and nowhere else: keys(expires) ⊆ keys(data).
type Store struct {
	data    map[string]string
	expires map[string]time.Time
}

func New() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
	}
}

// expired reports whether key has a deadline that now has reached. The deadline
// instant itself counts as expired.
//
// A key can be expired without having been removed. Logical expiration and
// physical deletion are separate events: a key is absent to callers the moment
// its deadline passes, and reclamation only returns its memory later.
func (s *Store) expired(key string, now time.Time) bool {
	deadline, ok := s.expires[key]
	return ok && !now.Before(deadline)
}

func (s *Store) Get(key string, now time.Time) (string, bool) {
	if s.expired(key, now) {
		return "", false
	}
	v, ok := s.data[key]
	return v, ok
}

// Set writes key. hasTTL false clears any deadline the key already had, which
// is Redis's behaviour: a SET with no expiry option is not "leave the old TTL
// alone", it is "this key now has no expiry".
func (s *Store) Set(key, value string, expiresAt time.Time, hasTTL bool) {
	s.data[key] = value
	if hasTTL {
		s.expires[key] = expiresAt
	} else {
		delete(s.expires, key)
	}
}

// Delete removes key and reports whether it was present. An expired key is
// already absent to callers, so deleting one reports false while still
// reclaiming it.
func (s *Store) Delete(key string) bool {
	_, ok := s.data[key]
	delete(s.data, key)
	delete(s.expires, key)
	return ok
}

// Expire attaches a deadline to an existing key and reports whether it applied.
// A key that is missing, or that has already expired without being reclaimed
// yet, cannot be given a new deadline — it is gone as far as callers are
// concerned, and resurrecting it here would make reclamation observable.
func (s *Store) Expire(key string, expiresAt time.Time, now time.Time) bool {
	if _, ok := s.data[key]; !ok || s.expired(key, now) {
		return false
	}
	s.expires[key] = expiresAt
	return true
}

// Persist removes a key's deadline and reports whether there was one.
func (s *Store) Persist(key string, now time.Time) bool {
	if _, ok := s.data[key]; !ok || s.expired(key, now) {
		return false
	}
	if _, had := s.expires[key]; !had {
		return false
	}
	delete(s.expires, key)
	return true
}

// TTL reports the time left on key. The duration is meaningful only when the
// status is HasTTL.
func (s *Store) TTL(key string, now time.Time) (time.Duration, TTLStatus) {
	if _, ok := s.data[key]; !ok || s.expired(key, now) {
		return 0, NoKey
	}
	deadline, ok := s.expires[key]
	if !ok {
		return 0, NoTTL
	}
	return deadline.Sub(now), HasTTL
}

// Len counts the keys that are live at now, not the size of the map. A count
// that included expired-but-unreclaimed keys would make reclamation observable
// to callers, which is exactly the distinction this package maintains.
func (s *Store) Len(now time.Time) int {
	n := 0
	for key := range s.data {
		if !s.expired(key, now) {
			n++
		}
	}
	return n
}

// ReclaimExpired physically removes up to limit expired keys, reporting how
// many it removed and how many it examined. It is the whole surface the active
// expiration worker needs; how often to call it and with what limit is the
// engine's decision, not this package's.
//
// Iterating expires rather than data is the point of keeping that index: a
// scan of the main map finds nothing when TTL-bearing keys are sparse.
//
// No claim is made about which keys get examined. Go's map iteration order is
// not a randomness contract and is not treated as one. The guarantee is only
// that work per call is bounded and that reclamation is eventual.
func (s *Store) ReclaimExpired(now time.Time, limit int) (removed, examined int) {
	if limit <= 0 {
		return 0, 0
	}
	for key, deadline := range s.expires {
		if examined == limit {
			break
		}
		examined++
		if !now.Before(deadline) {
			delete(s.data, key)
			delete(s.expires, key)
			removed++
		}
	}
	return removed, examined
}
