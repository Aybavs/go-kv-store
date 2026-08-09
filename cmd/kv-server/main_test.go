package main

import (
	"flag"
	"strings"
	"testing"
)

// TestFlagSurfaceIsStable is one half of turning v1.0's compatibility promise
// into something that can be broken loudly.
//
// ADR 0007 puts flag names *and their defaults* inside the promise: a default is
// behaviour a user never typed and therefore never opted into, so changing one
// in a patch release alters a running system silently, and nobody reads the
// notes for a patch.
//
// The table below is that promise written down. A 1.x may add a flag, which
// means adding a row here. It may not rename one, remove one, or change what one
// defaults to — each of those fails this test instead of reaching a user.
//
// Defaults are compared as the strings the flag package reports, because that is
// what `--help` prints and therefore what a reader is told.
func TestFlagSurfaceIsStable(t *testing.T) {
	want := map[string]string{
		"host":               "127.0.0.1",
		"port":               "6380",
		"max-clients":        "1024",
		"timeout":            "0s",
		"shutdown-timeout":   "10s",
		"max-bulk-length":    "67108864", // 64 MiB
		"max-array-elements": "1024",
		"max-command-bytes":  "134217728", // 128 MiB
		"loglevel":           "info",
		"appendonly":         "false",
		"appendfilename":     "appendonly.aof",
		"appendfsync":        "everysec",
	}

	// A FlagSet the test owns: registerFlags binds into whatever it is given,
	// so this enumerates the real surface without touching the global one or
	// running the server.
	fs := flag.NewFlagSet("kv-server", flag.ContinueOnError)
	registerFlags(fs)

	got := map[string]string{}
	fs.VisitAll(func(f *flag.Flag) { got[f.Name] = f.DefValue })

	for name, def := range want {
		actual, ok := got[name]
		if !ok {
			t.Errorf("flag -%s is gone; 1.x may add a flag, never remove or rename one", name)
			continue
		}
		if actual != def {
			t.Errorf("flag -%s defaults to %q, want %q; a changed default alters a running "+
				"system that never opted into it", name, actual, def)
		}
	}

	for name, def := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("flag -%s (default %q) is not in the compatibility table; adding a flag "+
				"is allowed, but it has to be written down here and in docs", name, def)
		}
	}

	// Both directions are needed and both are here: the first loop catches a
	// promised flag that is no longer registered, the second catches a
	// registered flag nobody promised. A third test that only counted them
	// would add nothing, which is why there is not one.
}

// The help text is the only place a user learns that our everysec is not
// Redis's, and ADR 0005 requires it to be stated wherever the flag is
// documented rather than only in docs/. Nothing checked that it still is.
func TestAppendfsyncHelpStatesTheRedisDifference(t *testing.T) {
	fs := flag.NewFlagSet("kv-server", flag.ContinueOnError)
	registerFlags(fs)

	f := fs.Lookup("appendfsync")
	if f == nil {
		t.Fatal("-appendfsync is missing")
	}
	for _, want := range []string{"Redis", "before writing"} {
		if !strings.Contains(f.Usage, want) {
			t.Errorf("the -appendfsync help does not mention %q; the difference from Redis's "+
				"everysec has to be stated where the flag is documented, not only in docs/:\n%s",
				want, f.Usage)
		}
	}
}
