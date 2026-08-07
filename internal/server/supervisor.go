package server

import "sync"

// Supervisor carries fatal conditions from any goroutine to the server
// lifecycle goroutine. Panics do not cross goroutine boundaries in Go, so a
// component that detects an untrustworthy shared state reports it here rather
// than relying on a panic reaching the top level.
//
// The signal is a closed channel rather than a delivered value, deliberately.
// A value can be received exactly once, so whichever waiter happened to read it
// would consume the report and every other waiter would wait forever — which is
// how a fatal condition raised during a graceful shutdown came to be silently
// dropped. Closing broadcasts: every waiter observes it, in any order.
type Supervisor struct {
	once sync.Once
	done chan struct{}

	mu    sync.RWMutex
	cause error
}

func NewSupervisor() *Supervisor {
	return &Supervisor{done: make(chan struct{})}
}

// Fatal reports a fatal condition. It is safe to call from any goroutine, and
// only the first call has an effect.
func (s *Supervisor) Fatal(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.cause = err
		s.mu.Unlock()
		close(s.done)
	})
}

// Done is closed when a fatal condition is first reported.
func (s *Supervisor) Done() <-chan struct{} { return s.done }

// Cause returns the first reported fatal condition, or nil if none has been
// reported.
func (s *Supervisor) Cause() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cause
}

// Fired reports whether a fatal condition has been raised. Connection-level
// panic recovery uses this to avoid swallowing an engine-fatal panic.
func (s *Supervisor) Fired() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}
