package server

import "sync"

// Supervisor carries fatal conditions from any goroutine to the lifecycle
// goroutine, since panics do not cross goroutine boundaries in Go.
//
// It broadcasts by closing a channel rather than delivering a value: a value is
// received exactly once, so one waiter would consume the report and the rest
// would wait forever. A fatal raised during shutdown was lost that way.
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
