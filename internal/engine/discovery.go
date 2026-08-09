package engine

import (
	"sort"

	"github.com/aybavs/go-kv-store/internal/glob"
)

type ScanResult struct {
	Cursor uint64
	Keys   []string
}

func (e *Engine) snapshotLiveKeys() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.LiveKeys(e.readNow())
}

func sortSnapshot(keys []string) { sort.Strings(keys) }

func filterSnapshot(keys []string, pattern string) []string {
	out := keys[:0]
	for _, key := range keys {
		if glob.Match(pattern, key) {
			out = append(out, key)
		}
	}
	return out
}

func (e *Engine) Keys(pattern string) []string {
	keys := e.snapshotLiveKeys()
	sortSnapshot(keys)
	return filterSnapshot(keys, pattern)
}

func (e *Engine) Scan(cursor uint64, pattern string, count uint64) ScanResult {
	keys := e.snapshotLiveKeys()
	sortSnapshot(keys)
	if cursor >= uint64(len(keys)) || count == 0 {
		return ScanResult{}
	}
	remaining := uint64(len(keys)) - cursor
	examined := count
	if examined > remaining {
		examined = remaining
	}
	end := cursor + examined
	next := end
	if end == uint64(len(keys)) {
		next = 0
	}
	page := keys[int(cursor):int(end)]
	return ScanResult{Cursor: next, Keys: filterSnapshot(page, pattern)}
}

func (e *Engine) DBSize() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.store.Len(e.readNow())
}
