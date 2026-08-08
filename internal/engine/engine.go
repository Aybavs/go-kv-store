// Package engine owns all shared-state synchronisation and mutation ordering.
// It holds the only RWMutex in the server; store is passive. Mutation methods
// already return an error because from v0.3 the same critical section also
// orders append-only-file records.
package engine

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/aybavs/go-kv-store/internal/store"
)

// ErrDraining is returned when a mutation is attempted after the server has
// begun shutting down.
var ErrDraining = errors.New("server is shutting down")

type Engine struct {
	mu                 sync.RWMutex
	store              *store.Store
	acceptingMutations bool

	onFatal func(error)

	// store never reads the clock, so the engine supplies it. Injected for the
	// same reason onFatal is: TTL behaviour is otherwise only testable by
	// sleeping, and this project has twice been bitten by tests that wait.
	now func() time.Time
}

// New returns an Engine. onFatal is called when an invariant of the shared
// mutation state can no longer be trusted; the supervisor turns that into a
// fatal shutdown. It must be non-nil.
func New(onFatal func(error)) *Engine {
	return NewWithClock(onFatal, time.Now)
}

// NewWithClock is New with the clock supplied. Production code uses New; tests
// pass a clock they control so expiry can be reached by moving time rather than
// by waiting for it.
func NewWithClock(onFatal func(error), now func() time.Time) *Engine {
	if onFatal == nil {
		panic("engine: New requires a non-nil onFatal; fatal conditions must be reportable")
	}
	if now == nil {
		panic("engine: New requires a non-nil clock")
	}
	return &Engine{
		store:              store.New(),
		acceptingMutations: true,
		onFatal:            onFatal,
		now:                now,
	}
}

// guard reports a panic in the commit path as a fatal condition; panics do not
// cross goroutine boundaries in Go, so reporting is what triggers shutdown.
//
// It is registered before the unlock, so the lock is released by the time this
// runs — reporting while holding it would deadlock if onFatal called back into
// the engine. The cost is that shared state is visible before the report.
func (e *Engine) guard() {
	if r := recover(); r != nil {
		e.onFatal(fmt.Errorf("engine commit path panic: %v\n%s", r, debug.Stack()))
		panic(r)
	}
}

func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.Get(key, e.now())
}

// Exists reports how many of keys are present. Duplicates are counted
// separately, matching Redis.
func (e *Engine) Exists(keys []string) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	// One clock read for the whole call, so a key cannot be judged against a
	// later instant than the key before it in the same EXISTS.
	now := e.now()
	n := 0
	for _, k := range keys {
		if _, ok := e.store.Get(k, now); ok {
			n++
		}
	}
	return n
}

func (e *Engine) Set(key, value string) error {
	defer e.guard()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.acceptingMutations {
		return ErrDraining
	}
	e.store.Set(key, value, time.Time{}, false)
	return nil
}

// Delete removes every listed key and reports how many were present. The whole
// operation happens under one lock hold, so it is atomic with respect to
// concurrent readers.
func (e *Engine) Delete(keys []string) (int, error) {
	defer e.guard()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.acceptingMutations {
		return 0, ErrDraining
	}
	n := 0
	for _, k := range keys {
		if e.store.Delete(k) {
			n++
		}
	}
	return n, nil
}

// BeginDrain closes mutation admission. It takes the same lock that mutations
// take, so a handler cannot pass the check and then be admitted afterwards.
//
// Invariant: once BeginDrain returns, no new mutation can be admitted.
func (e *Engine) BeginDrain() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.acceptingMutations = false
}
