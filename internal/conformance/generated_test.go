package conformance

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aybavs/go-kv-store/internal/cmdgen"
)

// Fixed, not derived from the clock. A seed taken from time.Now turns a
// reproducible failure into a screenshot: CI would report a divergence nobody
// could run again. KV_GEN_SEED and KV_GEN_RUNS exist for exploring further
// locally; neither is set in CI.
var generatorSeeds = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// TestGeneratedSequencesMatchRedis runs each seed's sequence against both
// servers and requires the normalised replies to agree step by step.
//
// The handwritten scenarios cover the cases their author thought of. This
// covers the ones nobody would write down: a sequence that expires a key,
// increments it, reads its TTL, persists it and increments it again reaches a
// state no scenario list contains, and that is where a derived effect is wrong.
func TestGeneratedSequencesMatchRedis(t *testing.T) {
	redisTarget := redisAddr(t)
	cfg := cmdgen.DefaultConfig()

	for _, seed := range seeds(t) {
		t.Run("seed="+strconv.FormatInt(seed, 10), func(t *testing.T) {
			steps := cmdgen.Sequence(seed, cfg)

			// Fresh server per sequence, as the handwritten scenarios get.
			got := runGenerated(t, startOurServer(t), steps, false)
			want := runGenerated(t, redisTarget, steps, true)

			if len(got) != len(want) {
				t.Fatalf("seed %d: reply count mismatch: ours=%d redis=%d", seed, len(got), len(want))
			}
			for i := range got {
				if got[i] == want[i] {
					continue
				}
				// The failure message is the deliverable: a generated
				// divergence that cannot be reproduced is worth very little.
				t.Fatalf("seed %d diverged at step %d\n"+
					"  step:  %s\n"+
					"  ours:  %s\n"+
					"  redis: %s\n"+
					"reproduce with: KV_GEN_SEED=%d go test ./internal/conformance/ -run TestGeneratedSequencesMatchRedis\n"+
					"sequence up to and including the divergence:\n%s",
					seed, i, steps[i], got[i], want[i], seed, render(steps[:i+1], got, want))
			}
		})
	}
}

// seeds is the fixed list, unless the environment asks for something narrower
// or wider.
func seeds(t *testing.T) []int64 {
	t.Helper()
	if raw := os.Getenv("KV_GEN_SEED"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatalf("KV_GEN_SEED=%q is not a number: %v", raw, err)
		}
		return []int64{n}
	}
	if raw := os.Getenv("KV_GEN_RUNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			t.Fatalf("KV_GEN_RUNS=%q is not a positive number", raw)
		}
		out := make([]int64, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, int64(i+1))
		}
		return out
	}
	return generatorSeeds
}

func runGenerated(t *testing.T, addr string, steps []cmdgen.Step, isRedis bool) []string {
	t.Helper()
	conn := dial(t, addr)

	if isRedis {
		if _, err := conn.Do("FLUSHDB"); err != nil {
			t.Fatalf("FLUSHDB: %v", err)
		}
	}

	out := make([]string, 0, len(steps))
	for _, s := range steps {
		reply, err := conn.Do(s[0], toArgs(s[1:])...)
		out = append(out, normaliseGenerated(t, s, reply, err))
	}
	return out
}

func toArgs(parts []string) []interface{} {
	out := make([]interface{}, len(parts))
	for i, p := range parts {
		out[i] = p
	}
	return out
}

// normaliseGenerated is normalise with one exception, and only one.
//
// A positive TTL reply is compared as a bucket rather than as a number. The two
// servers are asked in turn, seconds apart at worst, so a generated sequence
// that compared the exact remaining seconds would be comparing the clock. The
// exact value is already compared deterministically by the handwritten
// scenarios, which use deadlines far enough out that the number cannot change
// between the two questions; re-deriving it from a generator would buy no
// coverage and cost reproducibility.
//
// -1 and -2 still compare exactly, because those are the answers that carry the
// property this milestone is about: an INCR that dropped a key's expiry turns a
// positive TTL into -1, and that difference survives the bucketing.
func normaliseGenerated(t *testing.T, s cmdgen.Step, reply interface{}, err error) string {
	t.Helper()
	got := normalise(t, reply, err)
	if s[0] != "TTL" {
		return got
	}
	n, isInt := strings.CutPrefix(got, "INT:")
	if !isInt {
		return got
	}
	if v, convErr := strconv.ParseInt(n, 10, 64); convErr == nil && v > 0 {
		return "TTL:HAS"
	}
	return got
}

func render(steps []cmdgen.Step, got, want []string) string {
	var b strings.Builder
	for i, s := range steps {
		b.WriteString("  ")
		b.WriteString(strconv.Itoa(i))
		b.WriteString(": ")
		b.WriteString(s.String())
		if i < len(got) && i < len(want) {
			b.WriteString("  -> ours ")
			b.WriteString(got[i])
			if got[i] != want[i] {
				b.WriteString(" | redis ")
				b.WriteString(want[i])
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}
