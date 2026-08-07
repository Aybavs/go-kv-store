// Package store holds the in-memory key-value data structure.
//
// Store is deliberately passive: it has no mutex, no goroutines, no I/O, no
// clock reads and no protocol knowledge. All synchronisation is owned by
// package engine. This keeps store's tests fully deterministic.
package store

// Store maps keys to values. Values are strings because Go strings are
// immutable and binary-safe, which removes the aliasing bug class by
// construction.
type Store struct {
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Set(key, value string) {
	s.data[key] = value
}

// Delete removes key and reports whether it was present.
func (s *Store) Delete(key string) bool {
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

func (s *Store) Len() int { return len(s.data) }
