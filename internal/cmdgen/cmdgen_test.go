package cmdgen

import (
	"strings"
	"testing"
)

// Determinism is the whole reason this package exists in the form it does: a
// generated failure is only useful if the seed reproduces it.
func TestSequenceIsDeterministic(t *testing.T) {
	cfg := DefaultConfig()
	a := Sequence(7, cfg)
	b := Sequence(7, cfg)

	if len(a) != len(b) {
		t.Fatalf("same seed produced %d and %d steps", len(a), len(b))
	}
	for i := range a {
		if a[i].String() != b[i].String() {
			t.Fatalf("step %d differs between two runs of seed 7:\n  %s\n  %s", i, a[i], b[i])
		}
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	cfg := DefaultConfig()
	a, b := Sequence(1, cfg), Sequence(2, cfg)

	same := true
	for i := range a {
		if a[i].String() != b[i].String() {
			same = false
			break
		}
	}
	if same {
		t.Fatal("seeds 1 and 2 produced identical sequences; the seed is not reaching the generator")
	}
}

func TestSequenceHonoursBounds(t *testing.T) {
	cfg := Config{Keys: []string{"a", "b"}, Steps: 25}
	got := Sequence(3, cfg)

	if len(got) != cfg.Steps {
		t.Fatalf("generated %d steps, want %d", len(got), cfg.Steps)
	}
	allowed := map[string]bool{"a": true, "b": true}
	for i, s := range got {
		for _, arg := range keysOf(s) {
			if !allowed[arg] {
				t.Fatalf("step %d (%s) used key %q, which is not in the configured key space", i, s, arg)
			}
		}
	}
}

func TestEmptyConfigGeneratesNothing(t *testing.T) {
	if got := Sequence(1, Config{Keys: nil, Steps: 10}); got != nil {
		t.Errorf("no keys should generate nothing, got %d steps", len(got))
	}
	if got := Sequence(1, Config{Keys: []string{"a"}, Steps: 0}); got != nil {
		t.Errorf("zero steps should generate nothing, got %d steps", len(got))
	}
}

// A generator that emitted only GET would satisfy every assertion above. This
// is what stops it looking like evidence when it is not.
func TestDefaultSequenceReachesEveryVerb(t *testing.T) {
	cfg := DefaultConfig()

	seen := map[string]bool{}
	for seed := int64(0); seed < 8; seed++ {
		for _, s := range Sequence(seed, cfg) {
			seen[s[0]] = true
			// SET with an expiry is a distinct shape, not a distinct name.
			if s[0] == "SET" && len(s) > 3 {
				seen["SETEX"] = true
			}
		}
	}

	for _, verb := range Verbs() {
		if !seen[verb] {
			t.Errorf("no sequence in the first 8 seeds ever emitted %s", verb)
		}
	}
}

// Every step must be something the command table would accept as well formed.
// A generator that emitted a wrong-arity command would still compare equal
// against Redis, and would be spending its budget on the error path instead of
// on the states it exists to reach.
func TestEveryStepIsWellFormed(t *testing.T) {
	arity := map[string][2]int{
		"SET":     {3, -1},
		"GET":     {2, 2},
		"MGET":    {2, -1},
		"DEL":     {2, -1},
		"EXISTS":  {2, -1},
		"INCR":    {2, 2},
		"DECR":    {2, 2},
		"EXPIRE":  {3, 3},
		"TTL":     {2, 2},
		"PERSIST": {2, 2},
	}

	for seed := int64(0); seed < 8; seed++ {
		for i, s := range Sequence(seed, DefaultConfig()) {
			bounds, known := arity[s[0]]
			if !known {
				t.Fatalf("seed %d step %d emitted unknown command %q", seed, i, s[0])
			}
			if len(s) < bounds[0] || (bounds[1] >= 0 && len(s) > bounds[1]) {
				t.Fatalf("seed %d step %d (%s) has %d args, outside %v", seed, i, s, len(s), bounds)
			}
		}
	}
}

// The generated expiries are long on purpose. A short one would make both
// consumers of this package time-dependent, so this pins the rule rather than
// leaving it to the package comment.
func TestGeneratedExpiriesAreLong(t *testing.T) {
	for seed := int64(0); seed < 16; seed++ {
		for i, s := range Sequence(seed, DefaultConfig()) {
			switch {
			case s[0] == "EXPIRE":
				if s[2] != "100" && s[2] != "1000" {
					t.Fatalf("seed %d step %d (%s) uses a short or non-positive expiry", seed, i, s)
				}
			case s[0] == "SET" && len(s) == 5:
				unit, amount := s[3], s[4]
				if (unit == "EX" && amount != "100" && amount != "1000") ||
					(unit == "PX" && amount != "100000") {
					t.Fatalf("seed %d step %d (%s) uses a short expiry", seed, i, s)
				}
			}
		}
	}
}

func TestStepStringKeepsBinaryOnOneLine(t *testing.T) {
	got := Step{"SET", "k", "x\x00y\r\nz"}.String()
	if strings.ContainsAny(got, "\r\n") {
		t.Fatalf("String() emitted a raw CRLF, which makes one step look like two: %q", got)
	}
	if !strings.Contains(got, `\r\n`) {
		t.Fatalf("String() lost the CRLF instead of escaping it: %s", got)
	}
}

// keysOf returns the arguments of a step that are key positions.
func keysOf(s Step) []string {
	switch s[0] {
	case "SET":
		return s[1:2]
	case "EXPIRE":
		return s[1:2]
	case "GET", "INCR", "DECR", "TTL", "PERSIST":
		return s[1:2]
	default: // MGET, DEL, EXISTS
		return s[1:]
	}
}
