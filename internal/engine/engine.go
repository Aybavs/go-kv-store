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

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/store"
)

// ErrDraining is returned when a mutation is attempted after the server has
// begun shutting down.
var ErrDraining = errors.New("server is shutting down")

// ErrPersistenceUnavailable is returned when the log has already failed. The
// mutation is refused before it reaches memory: applying it against a log known
// to be broken would widen a divergence we already know about.
var ErrPersistenceUnavailable = errors.New("persistence unavailable")

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

	// log is nil when persistence is off, which is the whole of the difference:
	// every mutation path below degrades to exactly its v0.2 behaviour.
	log    *aof.Log
	policy aof.Policy
	// appliedSeq is the sequence of the last mutation applied to memory. It is
	// written under mu, so applied order and persisted order are the same
	// order by construction rather than by care.
	appliedSeq uint64
}

// AttachLog wires persistence in. It must be called before the engine serves,
// and never twice.
func (e *Engine) AttachLog(l *aof.Log, p aof.Policy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.log != nil {
		panic("engine: AttachLog called twice")
	}
	e.log = l
	e.policy = p
}

// OpenLog recovers path into this engine's store and then appends to it.
//
// Recovery is done here rather than by the caller because the store is not
// exported: handing it out so that main could replay into it would open the one
// hole the package spends its whole design closing.
//
// The returned Result describes what recovery found. An error means the file
// could not be trusted, and the caller must not start.
func (e *Engine) OpenLog(path string, p aof.Policy, onFatal func(error)) (aof.Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.log != nil {
		panic("engine: OpenLog called after a log was already attached")
	}

	// One instant for the whole replay, so a key cannot expire part-way
	// through it and make the recovered state depend on how long it took.
	l, res, err := aof.OpenFile(path, p, e.now(), e.store, onFatal)
	if err != nil {
		return res, err
	}
	e.log = l
	e.policy = p
	return res, nil
}

// Finalize drains the writer and syncs. It is the persistence stage of
// shutdown, between draining and stopped: mutations have already stopped being
// admitted, so whatever is still buffered is all there will ever be.
func (e *Engine) Finalize(ctx context.Context) error {
	e.mu.Lock()
	l := e.log
	e.mu.Unlock()

	if l == nil {
		return nil
	}
	return l.Close(ctx)
}

// commit appends the effect and applies it to memory under one acquisition of
// the lock, then returns the sequence to wait for. The caller awaits outside
// the lock — see the mutation methods.
//
// The ordering here is Model A: memory becomes visible before the durability
// acknowledgement. Another client may observe a mutation after its in-memory
// linearisation point but before the originating client is told it is durable.
// That is a documented consequence, not an oversight.
//
// Must be called with e.mu held.
func (e *Engine) commitSet(key, value string, deadline time.Time, hasTTL bool, apply func()) (uint64, error) {
	if e.log == nil {
		apply()
		return 0, nil
	}
	return e.commit(aof.DeriveSet(key, value, deadline, hasTTL), apply)
}

// commitDel is commitSet's counterpart for the other record shape.
func (e *Engine) commitDel(keys []string, apply func()) (uint64, error) {
	if e.log == nil {
		apply()
		return 0, nil
	}
	return e.commit(aof.DeriveDel(keys), apply)
}

// commit appends and applies. The two wrappers above exist so the record is
// never built when there is no log to put it in: passing aof.DeriveSet(...) as
// an argument evaluates it whether or not it is used, and that cost showed up
// as a 55% regression on EngineSet with persistence switched off.
func (e *Engine) commit(rec aof.Record, apply func()) (uint64, error) {
	seq, err := e.log.Append(rec)
	if err != nil {
		// Memory is untouched. This is the only failure mode where that is
		// true, and it is why the check happens before the apply.
		if errors.Is(err, aof.ErrFailed) {
			return 0, errors.Join(ErrPersistenceUnavailable, err)
		}
		return 0, err
	}
	apply()
	e.appliedSeq = seq
	return seq, nil
}

// await blocks for durability outside the lock. A zero sequence means
// persistence is off and there is nothing to wait for.
func (e *Engine) await(seq uint64) error {
	if e.log == nil || seq == 0 {
		return nil
	}
	if err := e.log.Await(seq, e.policy); err != nil {
		return errors.Join(ErrPersistenceUnavailable, err)
	}
	return nil
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
	seq, err := func() (uint64, error) {
		defer e.guard()
		e.mu.Lock()
		defer e.mu.Unlock()
		if !e.acceptingMutations {
			return 0, ErrDraining
		}
		var deadline time.Time
		if ttl.set {
			// Computed inside the lock, so the deadline is measured from the
			// instant the write actually lands rather than from when the
			// command arrived.
			deadline = e.now().Add(ttl.d)
		}
		return e.commitSet(key, value, deadline, ttl.set, func() {
			e.store.Set(key, value, deadline, ttl.set)
		})
	}()
	if err != nil {
		return err
	}
	// Outside the lock, always. The closure above is what guarantees it: there
	// is no path from here back into a held mutex.
	return e.await(seq)
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
	var applied bool
	seq, err := func() (uint64, error) {
		defer e.guard()
		e.mu.Lock()
		defer e.mu.Unlock()
		if !e.acceptingMutations {
			return 0, ErrDraining
		}
		now := e.now()
		// The effect is a complete SET carrying the value the key already
		// holds, so derivation has to read it. That read is inside the same
		// lock acquisition as the write, so no other mutation can land between
		// the two.
		value, ok := e.store.Get(key, now)
		if !ok {
			return 0, nil // nothing happened, so nothing is recorded
		}
		applied = true
		deadline := now.Add(d)
		return e.commitSet(key, value, deadline, true, func() {
			e.store.Expire(key, deadline, now)
		})
	}()
	if err != nil {
		return false, err
	}
	if !applied {
		return false, nil
	}
	// applied is reported true even if the wait fails: the mutation is in
	// memory and may already have been read by someone else.
	return true, e.await(seq)
}

// Persist removes a key's deadline and reports whether there was one.
func (e *Engine) Persist(key string) (bool, error) {
	var removed bool
	seq, err := func() (uint64, error) {
		defer e.guard()
		e.mu.Lock()
		defer e.mu.Unlock()
		if !e.acceptingMutations {
			return 0, ErrDraining
		}
		now := e.now()
		value, ok := e.store.Get(key, now)
		if !ok {
			return 0, nil
		}
		if _, st := e.store.TTL(key, now); st != store.HasTTL {
			return 0, nil // no TTL to remove; nothing changes, nothing recorded
		}
		removed = true
		return e.commitSet(key, value, time.Time{}, false, func() {
			e.store.Persist(key, now)
		})
	}()
	if err != nil {
		return false, err
	}
	if !removed {
		return false, nil
	}
	return true, e.await(seq)
}

// Delete removes every listed key and reports how many were present. The whole
// operation happens under one lock hold, so it is atomic with respect to
// concurrent readers.
func (e *Engine) Delete(keys []string) (int, error) {
	var removed int
	seq, err := func() (uint64, error) {
		defer e.guard()
		e.mu.Lock()
		defer e.mu.Unlock()
		if !e.acceptingMutations {
			return 0, ErrDraining
		}
		now := e.readNow()

		// Decide what the record will say before anything is applied. Only
		// keys that are live count and only they are recorded: an expired key
		// is already absent to callers, and is already absent on replay too.
		// Duplicates collapse, so DEL a a removes one key and reports one.
		seen := make(map[string]struct{}, len(keys))
		live := make([]string, 0, len(keys))
		for _, k := range keys {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			if _, ok := e.store.Get(k, now); ok {
				live = append(live, k)
			}
		}
		removed = len(live)

		apply := func() {
			// Every requested key is deleted, not only the live ones: that is
			// how an expired entry gets reclaimed. Only the count and the
			// record are restricted to live keys.
			for _, k := range keys {
				e.store.Delete(k)
			}
		}
		if removed == 0 {
			apply()
			return 0, nil // nothing observable changed, so nothing is recorded
		}
		return e.commitDel(live, apply)
	}()
	if err != nil {
		return 0, err
	}
	return removed, e.await(seq)
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
