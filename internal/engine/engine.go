// Package engine owns all shared-state synchronisation and mutation ordering.
// It holds the only RWMutex in the server; store is passive. Mutation methods
// already return an error because from v0.3 the same critical section also
// orders append-only-file records.
package engine

import (
	"context"
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

// TTLStatus is engine's own, deliberately not store's: no store type crosses
// this boundary, so command never has to import the storage layer to read a
// reply.
type TTLStatus int

const (
	NoKey TTLStatus = iota
	NoTTL
	HasTTL
)

// TTL is how a caller expresses "with this expiry" or "with none" without a
// sentinel duration. Its zero value means no expiry, so a Set that forgets to
// mention TTL clears one rather than inventing an arbitrary deadline.
type TTL struct {
	d   time.Duration
	set bool
}

// NoExpiry says the key must not carry a TTL. On an existing key it clears one.
func NoExpiry() TTL { return TTL{} }

// ExpiresIn says the key expires d from the moment the mutation is applied.
func ExpiresIn(d time.Duration) TTL { return TTL{d: d, set: true} }

type Engine struct {
	mu                 sync.RWMutex
	store              *store.Store
	acceptingMutations bool

	onFatal func(error)

	// store never reads the clock, so the engine supplies it. Injected for the
	// same reason onFatal is: TTL behaviour is otherwise only testable by
	// sleeping, and this project has twice been bitten by tests that wait.
	now func() time.Time

	// The worker's lifecycle is guarded separately from the data lock: stopping
	// it must not have to wait behind a sweep that is holding mu.
	expMu   sync.Mutex
	expStop chan struct{}
	expDone chan struct{}
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

// readNow supplies the instant expiry is judged against, and skips the clock
// entirely when no key carries a deadline.
//
// This is not a micro-optimisation. Measured on an Apple M4, time.Now costs
// about 54 ns while the map lookups around it cost about 4 ns, so reading the
// clock on every GET made the engine read path more than five times slower than
// v0.1.0 — for a store that might contain no TTLs at all. The zero time is safe
// as a stand-in: with no deadlines recorded, nothing compares against it.
//
// Callers must already hold the lock; len on a map is O(1).
func (e *Engine) readNow() time.Time {
	if !e.store.HasExpirations() {
		return time.Time{}
	}
	return e.now()
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
	return e.store.Get(key, e.readNow())
}

// Exists reports how many of keys are present. Duplicates are counted
// separately, matching Redis.
func (e *Engine) Exists(keys []string) int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	// One clock read for the whole call, so a key cannot be judged against a
	// later instant than the key before it in the same EXISTS.
	now := e.readNow()
	n := 0
	for _, k := range keys {
		if _, ok := e.store.Get(k, now); ok {
			n++
		}
	}
	return n
}

// Set writes key. A TTL of NoExpiry clears any expiry the key already had,
// which is Redis's rule: a SET with no expiry option is not "leave the old TTL
// alone".
func (e *Engine) Set(key, value string, ttl TTL) error {
	defer e.guard()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.acceptingMutations {
		return ErrDraining
	}
	var deadline time.Time
	if ttl.set {
		// Computed inside the lock, so the deadline is measured from the
		// instant the write actually lands rather than from when the command
		// arrived.
		deadline = e.now().Add(ttl.d)
	}
	e.store.Set(key, value, deadline, ttl.set)
	return nil
}

// TTL reports the time left on key. The duration is meaningful only when the
// status is HasTTL. A key whose deadline has passed reports NoKey, whether or
// not the worker has reclaimed it yet.
func (e *Engine) TTL(key string) (time.Duration, TTLStatus) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	d, st := e.store.TTL(key, e.readNow())
	switch st {
	case store.HasTTL:
		return d, HasTTL
	case store.NoTTL:
		return 0, NoTTL
	default:
		return 0, NoKey
	}
}

// Expire attaches a deadline to an existing key and reports whether it applied.
func (e *Engine) Expire(key string, d time.Duration) (bool, error) {
	defer e.guard()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.acceptingMutations {
		return false, ErrDraining
	}
	now := e.now()
	return e.store.Expire(key, now.Add(d), now), nil
}

// Persist removes a key's deadline and reports whether there was one.
func (e *Engine) Persist(key string) (bool, error) {
	defer e.guard()
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.acceptingMutations {
		return false, ErrDraining
	}
	return e.store.Persist(key, e.now()), nil
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
	now := e.readNow()
	n := 0
	for _, k := range keys {
		// An expired key is already absent to callers, so it must not be
		// counted as removed even though deleting it reclaims the entry.
		_, live := e.store.Get(k, now)
		if e.store.Delete(k) && live {
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

// Interval and sample size are starting points, not values copied from Redis.
// The spec's guarantee is only that work per cycle is bounded and reclamation
// is eventual, so these are tuned by measurement if measurement ever asks.
const (
	expirationInterval = 100 * time.Millisecond
	expirationSample   = 20
)

// StartExpiration runs the active expiration worker. It reclaims memory; it is
// not what makes an expired key disappear. Keys become invisible the moment
// their deadline passes, on the read path, whether or not this ever runs.
func (e *Engine) StartExpiration() {
	ticker := time.NewTicker(expirationInterval)
	e.startExpiration(ticker.C, ticker.Stop)
}

// startExpiration takes its tick source from the caller so tests can drive
// cycles exactly rather than waiting for a real interval to elapse.
func (e *Engine) startExpiration(ticks <-chan time.Time, cleanup func()) {
	e.expMu.Lock()
	defer e.expMu.Unlock()
	if e.expStop != nil {
		return // already running
	}
	stop, done := make(chan struct{}), make(chan struct{})
	e.expStop, e.expDone = stop, done

	go func() {
		defer close(done)
		if cleanup != nil {
			defer cleanup()
		}
		for {
			select {
			case <-stop:
				return
			case <-ticks:
				e.reclaimOnce()
			}
		}
	}()
}

// reclaimOnce takes the write lock for one bounded sweep. It does not consult
// the admission gate: reclamation is not a client mutation, and refusing it
// with ErrDraining would conflate two different things. The worker is stopped
// before the gate closes instead.
func (e *Engine) reclaimOnce() (removed, examined int) {
	defer e.guard()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.store.ReclaimExpired(e.now(), expirationSample)
}

// StopExpiration stops the worker and waits for the cycle in flight. It is safe
// to call when the worker was never started, and safe to call twice.
func (e *Engine) StopExpiration(ctx context.Context) error {
	e.expMu.Lock()
	stop, done := e.expStop, e.expDone
	e.expStop, e.expDone = nil, nil
	e.expMu.Unlock()

	if stop == nil {
		return nil
	}
	close(stop)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// physicalLen reports how many entries are still held, expired or not. It
// exists for the reclamation tests: every public count deliberately hides
// expired keys, which is exactly what those tests need to see through.
func (e *Engine) physicalLen() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.PhysicalLen()
}
