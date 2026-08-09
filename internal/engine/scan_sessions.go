package engine

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"
)

const (
	scanSessionTTL      = 30 * time.Second
	maxScanSessions     = 16
	maxScanSessionBytes = uint64(128 << 20)
	scanStringBytes     = uint64(16) // conservative on 32-bit, exact header on 64-bit
	scanSessionOverhead = uint64(64)
)

var (
	ErrInvalidCursor    = errors.New("invalid cursor")
	ErrScanSessionLimit = errors.New("scan session limit reached")
	ErrScanMatchChanged = errors.New("scan MATCH cannot change during iteration")
)

type scanSessionLimits struct {
	ttl         time.Duration
	maxSessions int
	maxBytes    uint64
}

type scanSessionStats struct {
	active        int
	retainedBytes uint64
}

type scanSession struct {
	keys     []string
	pattern  string
	position int
	lastUsed time.Time
	bytes    uint64
	token    uint64
}

type scanSessionManager struct {
	mu       sync.Mutex
	sessions map[uint64]*scanSession
	bytes    uint64
	limits   scanSessionLimits
	now      func() time.Time
	token    func() (uint64, error)
}

func defaultScanSessionLimits() scanSessionLimits {
	return scanSessionLimits{
		ttl:         scanSessionTTL,
		maxSessions: maxScanSessions,
		maxBytes:    maxScanSessionBytes,
	}
}

func newScanSessionManager(now func() time.Time, token func() (uint64, error), limits scanSessionLimits) *scanSessionManager {
	if now == nil {
		panic("engine: scan session manager requires a non-nil clock")
	}
	if token == nil {
		token = randomScanSessionToken
	}
	return &scanSessionManager{
		sessions: make(map[uint64]*scanSession),
		limits:   limits,
		now:      now,
		token:    token,
	}
}

func (m *scanSessionManager) start(keys []string, pattern string, count uint64, now time.Time) (ScanResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.expireLocked(now)
	end := scanPageEnd(0, len(keys), count)
	page := keys[:end:end]
	if end == len(keys) {
		return ScanResult{Keys: page}, nil
	}

	retainedBytes := scanSessionBytes(keys, pattern)
	if len(m.sessions) >= m.limits.maxSessions ||
		retainedBytes > m.limits.maxBytes ||
		m.bytes > m.limits.maxBytes-retainedBytes {
		return ScanResult{}, ErrScanSessionLimit
	}

	token, err := m.nextTokenLocked()
	if err != nil {
		return ScanResult{}, err
	}
	session := &scanSession{
		keys:     keys,
		pattern:  pattern,
		position: end,
		lastUsed: now,
		bytes:    retainedBytes,
		token:    token,
	}
	m.sessions[token] = session
	m.bytes += retainedBytes
	return ScanResult{Cursor: token, Keys: page}, nil
}

func (m *scanSessionManager) next(cursor uint64, pattern string, patternSet bool, count uint64, now time.Time) (ScanResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.expireLocked(now)
	session, ok := m.sessions[cursor]
	if !ok {
		return ScanResult{}, ErrInvalidCursor
	}
	if patternSet && pattern != session.pattern {
		return ScanResult{}, ErrScanMatchChanged
	}

	end := scanPageEnd(session.position, len(session.keys), count)
	page := session.keys[session.position:end:end]
	if end == len(session.keys) {
		m.removeLocked(cursor, session)
		return ScanResult{Keys: page}, nil
	}

	replacement, err := m.nextTokenLocked()
	if err != nil {
		return ScanResult{}, err
	}
	delete(m.sessions, cursor)
	session.position = end
	session.lastUsed = now
	session.token = replacement
	m.sessions[replacement] = session
	return ScanResult{Cursor: replacement, Keys: page}, nil
}

func (m *scanSessionManager) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions = make(map[uint64]*scanSession)
	m.bytes = 0
}

func (m *scanSessionManager) stats() scanSessionStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return scanSessionStats{active: len(m.sessions), retainedBytes: m.bytes}
}

func (m *scanSessionManager) expireLocked(now time.Time) {
	for token, session := range m.sessions {
		if !now.Before(session.lastUsed.Add(m.limits.ttl)) {
			m.removeLocked(token, session)
		}
	}
}

func (m *scanSessionManager) removeLocked(token uint64, session *scanSession) {
	delete(m.sessions, token)
	m.bytes -= session.bytes
}

func (m *scanSessionManager) nextTokenLocked() (uint64, error) {
	for {
		token, err := m.token()
		if err != nil {
			return 0, err
		}
		if token == 0 {
			continue
		}
		if _, exists := m.sessions[token]; exists {
			continue
		}
		return token, nil
	}
}

func randomScanSessionToken() (uint64, error) {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(bytes[:]), nil
}

func scanPageEnd(start, length int, count uint64) int {
	remaining := length - start
	if count >= uint64(remaining) {
		return length
	}
	return start + int(count)
}

func scanSessionBytes(keys []string, pattern string) uint64 {
	var keyBytes uint64
	for _, key := range keys {
		keyBytes = scanSaturatingAdd(keyBytes, uint64(len(key)))
	}
	return scanSessionRetainedBytes(uint64(cap(keys)), keyBytes, uint64(len(pattern)))
}

func scanSessionRetainedBytes(keyCapacity, keyBytes, patternBytes uint64) uint64 {
	stringHeaders := scanSaturatingMultiply(keyCapacity, scanStringBytes)
	total := scanSaturatingAdd(stringHeaders, keyBytes)
	total = scanSaturatingAdd(total, patternBytes)
	return scanSaturatingAdd(total, scanSessionOverhead)
}

func scanSaturatingAdd(a, b uint64) uint64 {
	if math.MaxUint64-a < b {
		return math.MaxUint64
	}
	return a + b
}

func scanSaturatingMultiply(a, b uint64) uint64 {
	if a != 0 && b > math.MaxUint64/a {
		return math.MaxUint64
	}
	return a * b
}
