package cmdgen_test

import (
	"go/build"
	"os/exec"
	"strings"
	"testing"
)

// TestCmdgenStaysOutOfTheBinary is the same check the redigo client gets: a
// test-only helper that finds its way into the server is no longer test-only.
// The generator has no place in a running server and importing it there would
// also drag math/rand into the binary for nothing.
func TestCmdgenStaysOutOfTheBinary(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "../../cmd/kv-server").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasSuffix(line, "/internal/cmdgen") {
			t.Fatalf("the server binary depends on %s", line)
		}
	}
}

// It generates text. Knowing how to run what it generates would make it a
// second implementation of the thing it is testing.
func TestCmdgenKnowsNothingAboutTheServer(t *testing.T) {
	pkg, err := build.Import("github.com/aybavs/go-kv-store/internal/cmdgen", "", 0)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	for _, imp := range pkg.Imports {
		if strings.Contains(imp, "/internal/") {
			t.Fatalf("cmdgen imports %s; it is supposed to know nothing about what runs its output", imp)
		}
	}
}
