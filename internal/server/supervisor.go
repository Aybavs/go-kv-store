package server

import "sync"

// Supervisor carries fatal conditions from any goroutine to the server
// lifecycle goroutine. Panics do not cross goroutine boundaries in Go, so a
// component that detects an untrustworthy shared state reports it here rather
// than relying on a panic reaching the top level.
type Supervisor struct {
	once  sync.Once
	ch    chan error
	mu    sync.RWMutex
	fired bool
}

func NewSupervisor() *Supervisor {
	return &Supervisor{ch: make(chan error, 1)}
}

// Fatal reports a fatal condition. It is safe to call from any goroutine and
// only the first call has an effect.
func (s *Supervisor) Fatal(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.fired = true
		s.mu.Unlock()
		s.ch <- err
	})
}

// C returns the channel that delivers the first fatal condition.
func (s *Supervisor) C() <-chan error { return s.ch }

// Fired reports whether a fatal condition has been raised. Connection-level
// panic recovery uses this to avoid swallowing an engine-fatal panic.
func (s *Supervisor) Fired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fired
}
