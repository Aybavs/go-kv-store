package engine

import (
	"errors"
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestScanSessionLifecycleRotatesTokenAndReleasesOnCompletion(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	tokens := sequenceTokens(101, 202)
	m := newScanSessionManager(
		func() time.Time { return now },
		tokens,
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)

	first, err := m.start([]string{"alpha", "beta", "gamma"}, "*", 1, now)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if first.Cursor != 101 || len(first.Keys) != 1 || first.Keys[0] != "alpha" {
		t.Fatalf("first page = %+v, want cursor 101 and [alpha]", first)
	}

	second, err := m.next(first.Cursor, "", false, 1, now)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if second.Cursor != 202 || len(second.Keys) != 1 || second.Keys[0] != "beta" {
		t.Fatalf("second page = %+v, want cursor 202 and [beta]", second)
	}
	if _, err := m.next(first.Cursor, "", false, 1, now); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("consumed cursor error = %v, want %v", err, ErrInvalidCursor)
	}

	last, err := m.next(second.Cursor, "", false, 1, now)
	if err != nil {
		t.Fatalf("final next: %v", err)
	}
	if last.Cursor != 0 || len(last.Keys) != 1 || last.Keys[0] != "gamma" {
		t.Fatalf("final page = %+v, want cursor 0 and [gamma]", last)
	}
	if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
		t.Fatalf("stats after completion = %+v, want zero", got)
	}
	for name, cursor := range map[string]uint64{"completed": second.Cursor, "unknown": 999} {
		t.Run(name+" cursor is invalid", func(t *testing.T) {
			if _, err := m.next(cursor, "", false, 1, now); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("next(%d) error = %v, want %v", cursor, err, ErrInvalidCursor)
			}
		})
	}
}

func TestScanSessionSinglePageAndEmptyBypassRetainedLimits(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	tokenCalls := 0
	m := newScanSessionManager(
		func() time.Time { return now },
		func() (uint64, error) {
			tokenCalls++
			return 0, errors.New("token source must not be called")
		},
		scanSessionLimits{ttl: time.Minute, maxSessions: 0, maxBytes: 0},
	)

	empty, err := m.start(nil, "arbitrarily-large-pattern", 1, now)
	if err != nil {
		t.Fatalf("empty start: %v", err)
	}
	if empty.Cursor != 0 || len(empty.Keys) != 0 {
		t.Fatalf("empty page = %+v, want terminal empty page", empty)
	}

	one, err := m.start([]string{"only"}, "arbitrarily-large-pattern", math.MaxUint64, now)
	if err != nil {
		t.Fatalf("one-page start: %v", err)
	}
	if one.Cursor != 0 || !slices.Equal(one.Keys, []string{"only"}) {
		t.Fatalf("one page = %+v, want cursor 0 and [only]", one)
	}
	if tokenCalls != 0 {
		t.Fatalf("token source calls = %d, want 0", tokenCalls)
	}
	if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
		t.Fatalf("stats = %+v, want zero", got)
	}
}

func TestScanSessionCountMayChangeAndMatchMayRepeatOrBeOmitted(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(11, 22),
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)

	first, err := m.start([]string{"a", "b", "c", "d", "e"}, "prefix:*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.next(first.Cursor, "prefix:*", true, 2, now)
	if err != nil {
		t.Fatalf("next with identical MATCH: %v", err)
	}
	if second.Cursor != 22 || !slices.Equal(second.Keys, []string{"b", "c"}) {
		t.Fatalf("second page = %+v, want cursor 22 and [b c]", second)
	}
	last, err := m.next(second.Cursor, "", false, 99, now)
	if err != nil {
		t.Fatalf("next with omitted MATCH: %v", err)
	}
	if last.Cursor != 0 || !slices.Equal(last.Keys, []string{"d", "e"}) {
		t.Fatalf("last page = %+v, want cursor 0 and [d e]", last)
	}
}

func TestScanSessionChangedMatchDoesNotConsumeCursor(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(41, 42),
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)

	first, err := m.start([]string{"a", "b", "c"}, "a*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.next(first.Cursor, "b*", true, 1, now); !errors.Is(err, ErrScanMatchChanged) {
		t.Fatalf("changed MATCH error = %v, want %v", err, ErrScanMatchChanged)
	}
	second, err := m.next(first.Cursor, "a*", true, 1, now)
	if err != nil {
		t.Fatalf("original cursor after changed MATCH: %v", err)
	}
	if second.Cursor != 42 || !slices.Equal(second.Keys, []string{"b"}) {
		t.Fatalf("page after changed MATCH = %+v, want cursor 42 and [b]", second)
	}
}

func TestScanSessionConcurrentSessionsRemainIsolated(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(51, 52, 53, 54),
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)

	a, err := m.start([]string{"a1", "a2", "a3"}, "a*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.start([]string{"b1", "b2", "b3"}, "b*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	bNext, err := m.next(b.Cursor, "", false, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	aNext, err := m.next(a.Cursor, "", false, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(bNext.Keys, []string{"b2"}) || !slices.Equal(aNext.Keys, []string{"a2"}) {
		t.Fatalf("isolated pages = a:%+v b:%+v", aNext, bNext)
	}
	if got := m.stats(); got.active != 2 {
		t.Fatalf("active sessions = %d, want 2", got.active)
	}
}

func TestScanSessionConcurrentConsumptionIsAtomic(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	tokens := []uint64{141, 142, 143}
	var tokenIndex atomic.Uint64
	m := newScanSessionManager(
		func() time.Time { return now },
		func() (uint64, error) {
			index := tokenIndex.Add(1) - 1
			if index >= uint64(len(tokens)) {
				return 0, errors.New("token sequence exhausted")
			}
			return tokens[index], nil
		},
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)

	first, err := m.start([]string{"a", "b", "c"}, "*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	type nextResult struct {
		page ScanResult
		err  error
	}
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	results := make(chan nextResult, 2)
	for range 2 {
		go func() {
			ready <- struct{}{}
			<-start
			page, err := m.next(first.Cursor, "", false, 1, now)
			results <- nextResult{page: page, err: err}
		}()
	}
	<-ready
	<-ready
	close(start)

	var winner ScanResult
	successes, invalid := 0, 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil:
			successes++
			winner = result.page
			if winner.Cursor == 0 || winner.Cursor == first.Cursor || !slices.Equal(winner.Keys, []string{"b"}) {
				t.Fatalf("successful concurrent next = %+v, want replacement cursor and [b]", winner)
			}
		case errors.Is(result.err, ErrInvalidCursor):
			invalid++
			if result.page.Cursor != 0 || len(result.page.Keys) != 0 {
				t.Fatalf("invalid concurrent next returned page %+v", result.page)
			}
		default:
			t.Fatalf("concurrent next returned unexpected error %v and page %+v", result.err, result.page)
		}
	}
	if successes != 1 || invalid != 1 {
		t.Fatalf("concurrent next outcomes = %d success, %d invalid; want one each", successes, invalid)
	}

	last, err := m.next(winner.Cursor, "", false, 99, now)
	if err != nil {
		t.Fatalf("winning replacement cursor: %v", err)
	}
	if last.Cursor != 0 || !slices.Equal(last.Keys, []string{"c"}) {
		t.Fatalf("final page = %+v, want cursor 0 and [c]", last)
	}
	if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
		t.Fatalf("stats after completion = %+v, want zero", got)
	}
}

func TestScanSessionRetriesZeroAndCollidingTokens(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(0, 11, 11, 22, 0, 11, 22, 33),
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)

	a, err := m.start([]string{"a1", "a2", "a3"}, "a*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.start([]string{"b1", "b2", "b3"}, "b*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if a.Cursor != 11 || b.Cursor != 22 {
		t.Fatalf("initial cursors = %d and %d, want 11 and 22", a.Cursor, b.Cursor)
	}
	aNext, err := m.next(a.Cursor, "", false, 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if aNext.Cursor != 33 || !slices.Equal(aNext.Keys, []string{"a2"}) {
		t.Fatalf("rotated page = %+v, want cursor 33 and [a2]", aNext)
	}
}

func TestScanSessionTokenSourceErrorsAreAtomic(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	tokenErr := errors.New("token source failed")
	t.Run("start retains nothing", func(t *testing.T) {
		m := newScanSessionManager(
			func() time.Time { return now },
			func() (uint64, error) { return 0, tokenErr },
			scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
		)
		if _, err := m.start([]string{"a", "b"}, "*", 1, now); !errors.Is(err, tokenErr) {
			t.Fatalf("start error = %v, want %v", err, tokenErr)
		}
		if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
			t.Fatalf("stats after failed start = %+v, want zero", got)
		}
	})

	t.Run("continuation leaves old cursor retryable", func(t *testing.T) {
		calls := 0
		m := newScanSessionManager(
			func() time.Time { return now },
			func() (uint64, error) {
				calls++
				switch calls {
				case 1:
					return 61, nil
				case 2:
					return 0, tokenErr
				default:
					return 62, nil
				}
			},
			scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
		)
		first, err := m.start([]string{"a", "b", "c"}, "*", 1, now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.next(first.Cursor, "", false, 1, now); !errors.Is(err, tokenErr) {
			t.Fatalf("first continuation error = %v, want %v", err, tokenErr)
		}
		retry, err := m.next(first.Cursor, "", false, 1, now)
		if err != nil {
			t.Fatalf("retry old cursor: %v", err)
		}
		if retry.Cursor != 62 || !slices.Equal(retry.Keys, []string{"b"}) {
			t.Fatalf("retry page = %+v, want cursor 62 and [b]", retry)
		}
	})
}

func TestScanSessionUsesNonzeroProductionTokenSource(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		nil,
		scanSessionLimits{ttl: time.Minute, maxSessions: 4, maxBytes: 1 << 20},
	)
	page, err := m.start([]string{"a", "b"}, "*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if page.Cursor == 0 {
		t.Fatal("production token source returned zero cursor")
	}
}

func TestScanSessionExpiresAtExactInactivityTTL(t *testing.T) {
	start := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	const ttl = 10 * time.Second
	m := newScanSessionManager(
		func() time.Time { return start },
		sequenceTokens(71),
		scanSessionLimits{ttl: ttl, maxSessions: 4, maxBytes: 1 << 20},
	)
	page, err := m.start([]string{"a", "b"}, "*", 1, start)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.next(page.Cursor, "", false, 1, start.Add(ttl)); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("next at exact TTL error = %v, want %v", err, ErrInvalidCursor)
	}
	if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
		t.Fatalf("stats after expiry = %+v, want zero", got)
	}
}

func TestScanSessionContinuationRefreshesInactivityTTL(t *testing.T) {
	start := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	const ttl = 10 * time.Second
	m := newScanSessionManager(
		func() time.Time { return start },
		sequenceTokens(76, 77, 78),
		scanSessionLimits{ttl: ttl, maxSessions: 4, maxBytes: 1 << 20},
	)
	first, err := m.start([]string{"a", "b", "c", "d"}, "*", 1, start)
	if err != nil {
		t.Fatal(err)
	}
	secondAt := start.Add(ttl - time.Nanosecond)
	second, err := m.next(first.Cursor, "", false, 1, secondAt)
	if err != nil {
		t.Fatalf("first continuation: %v", err)
	}
	thirdAt := secondAt.Add(ttl - time.Nanosecond)
	third, err := m.next(second.Cursor, "", false, 1, thirdAt)
	if err != nil {
		t.Fatalf("continuation within refreshed TTL: %v", err)
	}
	if third.Cursor != 78 || !slices.Equal(third.Keys, []string{"c"}) {
		t.Fatalf("third page = %+v, want cursor 78 and [c]", third)
	}
}

func TestScanSessionCleansExpiredSessionsOnStart(t *testing.T) {
	start := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	const ttl = 10 * time.Second
	m := newScanSessionManager(
		func() time.Time { return start },
		sequenceTokens(81, 82),
		scanSessionLimits{ttl: ttl, maxSessions: 1, maxBytes: 1 << 20},
	)
	old, err := m.start([]string{"old-1", "old-2"}, "old*", 1, start)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := m.start([]string{"new-1", "new-2"}, "new*", 1, start.Add(ttl))
	if err != nil {
		t.Fatalf("start after expiry: %v", err)
	}
	if fresh.Cursor != 82 {
		t.Fatalf("fresh cursor = %d, want 82", fresh.Cursor)
	}
	if got := m.stats(); got.active != 1 {
		t.Fatalf("active sessions = %d, want 1", got.active)
	}
	if _, err := m.next(old.Cursor, "", false, 1, start.Add(ttl)); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired old cursor error = %v, want %v", err, ErrInvalidCursor)
	}
}

func TestScanSessionCleansExpiredSessionsOnContinuation(t *testing.T) {
	start := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	const ttl = 10 * time.Second
	m := newScanSessionManager(
		func() time.Time { return start },
		sequenceTokens(91, 92, 93),
		scanSessionLimits{ttl: ttl, maxSessions: 2, maxBytes: 1 << 20},
	)
	old, err := m.start([]string{"old-1", "old-2"}, "old*", 1, start)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := m.start([]string{"new-1", "new-2", "new-3"}, "new*", 1, start.Add(ttl/2))
	if err != nil {
		t.Fatal(err)
	}
	continued, err := m.next(fresh.Cursor, "", false, 1, start.Add(ttl))
	if err != nil {
		t.Fatalf("continue fresh session: %v", err)
	}
	if continued.Cursor != 93 || !slices.Equal(continued.Keys, []string{"new-2"}) {
		t.Fatalf("continued page = %+v, want cursor 93 and [new-2]", continued)
	}
	if got := m.stats(); got.active != 1 {
		t.Fatalf("active sessions = %d, want only fresh session", got.active)
	}
	if _, err := m.next(old.Cursor, "", false, 1, start.Add(ttl)); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired old cursor error = %v, want %v", err, ErrInvalidCursor)
	}
}

func TestScanSessionActiveLimitRejectsAtomically(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(101),
		scanSessionLimits{ttl: time.Minute, maxSessions: 1, maxBytes: 1 << 20},
	)
	if _, err := m.start([]string{"a1", "a2"}, "a*", 1, now); err != nil {
		t.Fatal(err)
	}
	before := m.stats()
	if _, err := m.start([]string{"b1", "b2"}, "b*", 1, now); !errors.Is(err, ErrScanSessionLimit) {
		t.Fatalf("second start error = %v, want %v", err, ErrScanSessionLimit)
	}
	if got := m.stats(); got != before {
		t.Fatalf("stats after rejected start = %+v, want unchanged %+v", got, before)
	}
}

func TestScanSessionMemoryEstimateAndLimit(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	keys := make([]string, 3, 8)
	copy(keys, []string{"a", "bb", "ccc"})
	pattern := "\x00\xff*"
	wantBytes := uint64(cap(keys))*scanStringBytes + 1 + 2 + 3 + uint64(len(pattern)) + scanSessionOverhead

	t.Run("counts capacity key bytes pattern bytes and overhead", func(t *testing.T) {
		m := newScanSessionManager(
			func() time.Time { return now },
			sequenceTokens(111),
			scanSessionLimits{ttl: time.Minute, maxSessions: 2, maxBytes: wantBytes},
		)
		if _, err := m.start(keys, pattern, 1, now); err != nil {
			t.Fatalf("start at exact byte limit: %v", err)
		}
		if got := m.stats(); got.active != 1 || got.retainedBytes != wantBytes {
			t.Fatalf("stats = %+v, want active 1 and %d bytes", got, wantBytes)
		}
	})

	t.Run("rejects one byte over atomically", func(t *testing.T) {
		m := newScanSessionManager(
			func() time.Time { return now },
			sequenceTokens(112),
			scanSessionLimits{ttl: time.Minute, maxSessions: 2, maxBytes: wantBytes - 1},
		)
		if _, err := m.start(keys, pattern, 1, now); !errors.Is(err, ErrScanSessionLimit) {
			t.Fatalf("start error = %v, want %v", err, ErrScanSessionLimit)
		}
		if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
			t.Fatalf("stats after rejected start = %+v, want zero", got)
		}
	})

	t.Run("counts existing sessions before accepting another", func(t *testing.T) {
		m := newScanSessionManager(
			func() time.Time { return now },
			sequenceTokens(113),
			scanSessionLimits{ttl: time.Minute, maxSessions: 2, maxBytes: wantBytes*2 - 1},
		)
		if _, err := m.start(keys, pattern, 1, now); err != nil {
			t.Fatal(err)
		}
		before := m.stats()
		if _, err := m.start(keys, pattern, 1, now); !errors.Is(err, ErrScanSessionLimit) {
			t.Fatalf("second start error = %v, want %v", err, ErrScanSessionLimit)
		}
		if got := m.stats(); got != before {
			t.Fatalf("stats after rejected start = %+v, want unchanged %+v", got, before)
		}
	})
}

func TestScanSessionMemoryArithmeticSaturates(t *testing.T) {
	if got := scanSessionRetainedBytes(math.MaxUint64, 0, 0); got != math.MaxUint64 {
		t.Fatalf("capacity multiplication = %d, want saturation", got)
	}
	if got := scanSessionRetainedBytes(1, math.MaxUint64, 1); got != math.MaxUint64 {
		t.Fatalf("byte addition = %d, want saturation", got)
	}

	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(121),
		scanSessionLimits{ttl: time.Minute, maxSessions: 1, maxBytes: math.MaxUint64},
	)
	m.bytes = math.MaxUint64 - 1
	if _, err := m.start([]string{"a", "b"}, "*", 1, now); !errors.Is(err, ErrScanSessionLimit) {
		t.Fatalf("start with overflowing total error = %v, want %v", err, ErrScanSessionLimit)
	}
	if got := m.stats(); got.active != 0 || got.retainedBytes != math.MaxUint64-1 {
		t.Fatalf("stats after overflow rejection = %+v, want unchanged bytes", got)
	}
}

func TestScanSessionClearReleasesCountersAndInvalidatesCursors(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	m := newScanSessionManager(
		func() time.Time { return now },
		sequenceTokens(131, 132),
		scanSessionLimits{ttl: time.Minute, maxSessions: 2, maxBytes: 1 << 20},
	)
	a, err := m.start([]string{"a1", "a2"}, "a*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.start([]string{"b1", "b2"}, "b*", 1, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.stats(); got.active != 2 || got.retainedBytes == 0 {
		t.Fatalf("stats before clear = %+v, want two retained sessions", got)
	}
	m.clear()
	if got := m.stats(); got.active != 0 || got.retainedBytes != 0 {
		t.Fatalf("stats after clear = %+v, want zero", got)
	}
	for _, cursor := range []uint64{a.Cursor, b.Cursor} {
		if _, err := m.next(cursor, "", false, 1, now); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("cleared cursor %d error = %v, want %v", cursor, err, ErrInvalidCursor)
		}
	}
}

func sequenceTokens(tokens ...uint64) func() (uint64, error) {
	return func() (uint64, error) {
		if len(tokens) == 0 {
			return 0, errors.New("token sequence exhausted")
		}
		token := tokens[0]
		tokens = tokens[1:]
		return token, nil
	}
}
