package aof_test

import (
	"go/build"
	"strings"
	"testing"
)

// TestAOFDoesNotImportEngine pins the lock-order rule structurally. Spec §6.6
// requires engine.mu → aof.mu in one direction only; aof never acquiring
// engine.mu is guaranteed by aof not knowing engine exists, which makes the
// reverse edge unconstructible rather than merely forbidden.
//
// store is checked too: aof deals in records, not in the data structure they
// are eventually applied to.
func TestAOFDoesNotImportEngine(t *testing.T) {
	pkg, err := build.Import("github.com/aybavs/go-kv-store/internal/aof", "", 0)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	for _, imp := range pkg.Imports {
		if strings.HasSuffix(imp, "/internal/engine") || strings.HasSuffix(imp, "/internal/store") {
			t.Fatalf("aof imports %s; the lock order depends on it not knowing that package", imp)
		}
	}
}
