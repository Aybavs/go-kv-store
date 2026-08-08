package aof

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aybavs/go-kv-store/internal/resp"
)

// File is the seam over the operations this package actually performs. It is
// deliberately not a stand-in for *os.File: three methods, and two real
// implementations — the file itself, and a fake that fails on demand. Without
// it the write-failure paths could not be tested at all, and those are the
// paths where correctness matters most.
type File interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// Policy decides what a successful acknowledgement means.
//
// EverySec is not Redis's everysec. Redis acknowledges before the write; we
// acknowledge only once write() has succeeded, so an ACK here means the data
// reached the operating system. A machine or power failure can still lose
// writes made since the last successful Sync. Same name, stronger guarantee —
// this difference must be stated wherever the policy is named, including flag
// help text, not only in docs.
type Policy int

const (
	EverySec Policy = iota
	Always
)

const syncInterval = time.Second

var (
	// ErrFailed means the log has already failed. A mutation must be refused
	// before it touches memory rather than applied against a log known to be
	// broken.
	ErrFailed = errors.New("aof: persistence unavailable")

	// ErrClosed means the log was closed while a caller was waiting on it.
	ErrClosed = errors.New("aof: log is closed")
)

// pending records where one record ends in the batch being assembled, so a
// write that delivers only part of the buffer can advance the marker to the
// last record that was fully delivered and no further.
type pending struct {
	seq uint64
	end int // cumulative byte offset within the batch
}

// Log is the append-only file: an in-memory batch, a writer goroutine, and the
// three markers that describe how far durability has progressed.
//
// Its mutex protects the buffer, the markers and the failure state. It is the
// inner lock: engine.mu is taken first and this one second, and the reverse is
// unconstructible because this package does not know engine exists.
//
// Real I/O never happens while the mutex is held. The batch is swapped out
// under it and written outside it, so an Append never waits on a disk.
type Log struct {
	mu   sync.Mutex
	cond *sync.Cond

	f      File
	policy Policy

	buf     bytes.Buffer
	w       *resp.Writer
	records []pending

	nextSeq uint64
	// appended is the last sequence handed out; written and synced are the
	// last made durable at each stage. synced <= written <= appended always.
	appended uint64
	written  uint64
	synced   uint64

	failed  error
	closing bool
	closed  bool

	notify  chan struct{}
	closeCh chan struct{}
	done    chan struct{}
	onFatal func(error)

	fatalOnce sync.Once
	closeOnce sync.Once
}

// Open starts the writer. onFatal is called at most once, when the log enters a
// failed state; the caller turns that into a fatal shutdown.
func Open(f File, policy Policy, onFatal func(error)) *Log {
	if onFatal == nil {
		panic("aof: Open requires a non-nil onFatal; a failed log must be reportable")
	}
	l := &Log{
		f:       f,
		policy:  policy,
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
		done:    make(chan struct{}),
		onFatal: onFatal,
	}
	l.cond = sync.NewCond(&l.mu)
	l.w = resp.NewWriter(&l.buf)

	ticker := time.NewTicker(syncInterval)
	go l.run(ticker.C, ticker.Stop)
	return l
}

// Append encodes a record into the pending batch and returns its sequence
// number. It performs no I/O, so the caller may hold its own lock across this
// call — which is what makes "persisted order == applied order" structural
// rather than a matter of care.
func (l *Log) Append(r Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.failed != nil {
		return 0, l.failed
	}
	if l.closing || l.closed {
		return 0, ErrClosed
	}

	before := l.buf.Len()
	if err := Encode(l.w, r); err != nil {
		return 0, err
	}
	if err := l.w.Flush(); err != nil {
		// The writer writes into a bytes.Buffer, so this cannot fail in
		// practice; returning it rather than ignoring it keeps the claim
		// honest if that ever changes.
		return 0, err
	}
	if l.buf.Len() == before {
		return 0, corruptf("record encoded to no bytes")
	}

	l.nextSeq++
	l.appended = l.nextSeq
	l.records = append(l.records, pending{seq: l.nextSeq, end: l.buf.Len()})

	select {
	case l.notify <- struct{}{}:
	default: // already signalled; the writer will see this batch too
	}
	return l.nextSeq, nil
}

// Await blocks until the record is durable to the degree the policy requires.
// It wakes on progress, on failure and on shutdown, so no caller can wait on a
// log that will never advance.
func (l *Log) Await(seq uint64, p Policy) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for {
		if l.failed != nil {
			return l.failed
		}
		if l.reached(seq, p) {
			return nil
		}
		if l.closed {
			return ErrClosed
		}
		l.cond.Wait()
	}
}

func (l *Log) reached(seq uint64, p Policy) bool {
	if p == Always {
		return l.synced >= seq
	}
	return l.written >= seq
}

// Markers reports the three progress markers. Tests read them; nothing in the
// server depends on them directly.
func (l *Log) Markers() (appended, written, synced uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.appended, l.written, l.synced
}

// Failed reports the failure that put the log into its terminal state, if any.
func (l *Log) Failed() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.failed
}

func (l *Log) run(ticks <-chan time.Time, stop func()) {
	defer close(l.done)
	if stop != nil {
		defer stop()
	}

	lastSync := time.Now()
	for {
		select {
		case <-l.notify:
			l.flush(false)
		case now := <-ticks:
			// A periodic Sync even with nothing new to write: everysec's
			// promise is about elapsed time, not about traffic.
			if now.Sub(lastSync) >= syncInterval {
				l.flush(true)
				lastSync = now
			}
		case <-l.closeCh:
			l.flush(true)
			l.finish()
			return
		}
		if l.policy == Always {
			lastSync = time.Now()
		}
	}
}

// Close stops accepting appends, flushes and syncs what is already buffered,
// then closes the file. It is safe to call more than once.
//
// The final Sync happens even under EverySec: a clean shutdown is exactly the
// moment where the difference between "written" and "durable" stops being
// acceptable.
func (l *Log) Close(ctx context.Context) error {
	l.mu.Lock()
	l.closing = true
	l.mu.Unlock()

	l.closeOnce.Do(func() { close(l.closeCh) })

	select {
	case <-l.done:
		return l.Failed()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flush swaps the pending batch out under the mutex and does the I/O without
// it, then reports progress. forceSync makes it Sync even under EverySec, which
// is how both the periodic timer and shutdown get their durability.
func (l *Log) flush(forceSync bool) {
	l.mu.Lock()
	if l.failed != nil {
		l.mu.Unlock()
		return
	}
	batch := make([]byte, l.buf.Len())
	copy(batch, l.buf.Bytes())
	l.buf.Reset()
	records := l.records
	l.records = nil
	l.mu.Unlock()

	if len(batch) > 0 {
		n, err := writeFull(l.f, batch)
		// Advance to the last record fully delivered, and no further: a record
		// half-way through the pipe is not durable in any sense.
		l.advanceWritten(records, n)
		if err != nil {
			l.fail(err)
			return
		}
	}

	if !forceSync && l.policy != Always {
		return
	}
	if err := l.f.Sync(); err != nil {
		l.fail(err)
		return
	}
	l.advanceSynced()
}

// writeFull keeps calling Write until the buffer is delivered or an error
// stops it, returning how many bytes actually made it. A single Write is not
// assumed to consume everything, and n > 0 alongside an error is its own case
// rather than an impossibility.
func writeFull(f File, p []byte) (int, error) {
	total := 0
	for total < len(p) {
		n, err := f.Write(p[total:])
		if n < 0 {
			return total, errors.New("aof: writer returned a negative count")
		}
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, errors.New("aof: writer made no progress")
		}
	}
	return total, nil
}

func (l *Log) advanceWritten(records []pending, n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range records {
		if r.end > n {
			break
		}
		l.written = r.seq
	}
	l.cond.Broadcast()
}

func (l *Log) advanceSynced() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.synced = l.written
	l.cond.Broadcast()
}

// fail moves the log into its terminal state, wakes everyone waiting, and
// reports once. Waiters are woken with the error rather than left blocked: their
// mutation is already applied to memory and may already have been seen by other
// clients, and that irreducible ambiguity is why this is fatal rather than
// something to retry.
func (l *Log) fail(err error) {
	l.mu.Lock()
	if l.failed == nil {
		l.failed = errors.Join(ErrFailed, err)
	}
	reported := l.failed
	l.cond.Broadcast()
	l.mu.Unlock()

	l.fatalOnce.Do(func() { l.onFatal(reported) })
}

func (l *Log) finish() {
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
	_ = l.f.Close()
}
