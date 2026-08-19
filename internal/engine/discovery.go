package engine

import (
	"sort"
	"time"

	"github.com/aybavs/go-kv-store/internal/glob"
)

type ScanResult struct {
	Cursor uint64
	Keys   []string
}

type ScanRequest struct {
	Cursor     uint64
	Pattern    string
	PatternSet bool
	Count      uint64
}

func (e *Engine) snapshotLiveKeys() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.LiveKeys(e.readNow())
}

func (e *Engine) captureLiveKeys() ([]string, time.Time) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := e.now()
	return e.scanLiveKeys(now), now
}

func sortSnapshot(keys []string) { sort.Strings(keys) }

func filterSnapshot(keys []string, pattern string) []string {
	out := keys[:0]
	for _, key := range keys {
		if glob.Match(pattern, key) {
			out = append(out, key)
		}
	}
	clear(keys[len(out):])
	return out
}

func (e *Engine) Keys(pattern string) []string {
	keys := e.snapshotLiveKeys()
	sortSnapshot(keys)
	return filterSnapshot(keys, pattern)
}

func (e *Engine) Scan(req ScanRequest) (ScanResult, error) {
	if req.Cursor != 0 {
		return e.scanSessions.next(req.Cursor, req.Pattern, req.PatternSet, req.Count, e.now())
	}
	keys, now := e.captureLiveKeys()
	pattern := "*"
	if req.PatternSet {
		pattern = req.Pattern
	}
	keys = e.scanFilter(keys, pattern)
	e.scanSort(keys)
	return e.scanSessions.start(keys, pattern, req.Count, now)
}

func (e *Engine) ClearScanSessions() {
	e.scanSessions.clear()
}

func (e *Engine) DBSize() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.Len(e.readNow())
}
