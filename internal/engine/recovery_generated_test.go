// This file is package engine_test rather than package engine because it drives
// the engine through the real command layer, and command imports engine. Only an
// external test package may close that loop.
package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/cmdgen"
	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
)

// Same fixed list the differential runner uses, for the same reason: a failure
// has to be reproducible from the seed.
var recoverySeeds = []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// Recovery is checked after every step, not once at the end. Only 3 of 64 final
// key-states are decided by a TTL-preserving INCR; every other key is last
// written by a later SET, DEL or EXPIRE whose record is correct regardless. An
// end-state comparison passes with the mutation this exists to catch.
const checkpointEvery = 1

// TestGeneratedSequencesSurviveRecovery replays an arbitrary command sequence
// out of the append-only file and requires the recovered state to match the live
// one at every step. The differential suite compares us against Redis, and Redis
// has no opinion about our AOF; INCR is the first command whose effect depends
// on stored state, which is ADR-0004's counterexample.
//
// Always is what makes checkpointing possible: every acknowledged mutation is
// already on disk, so a copy taken mid-sequence holds exactly the records issued
// so far. Surviving a kill is package durability's question.
func TestGeneratedSequencesSurviveRecovery(t *testing.T) {
	cfg := cmdgen.DefaultConfig()

	for _, seed := range recoverySeeds {
		t.Run("seed="+strconv.FormatInt(seed, 10), func(t *testing.T) {
			steps := cmdgen.Sequence(seed, cfg)
			dir := t.TempDir()
			path := filepath.Join(dir, "appendonly.aof")

			live := newLoggedEngine(t, path)
			defer finalize(t, live)
			reg := command.New(live)

			for i, s := range steps {
				reply := reg.Dispatch(toBytes(s))
				if reply.Kind == command.ReplyError && isServerFault(reply.Str) {
					t.Fatalf("seed %d step %d (%s): %s", seed, i, s, reply.Str)
				}
				if (i+1)%checkpointEvery != 0 && i != len(steps)-1 {
					continue
				}

				before := snapshot(live, cfg.Keys)
				// Closed straight away rather than deferred: with a checkpoint
				// per step this would otherwise hold one open log per step
				// until the subtest ended.
				recovered := replayInto(t, dir, path, i)
				after := snapshot(recovered, cfg.Keys)
				finalize(t, recovered)

				for k, key := range cfg.Keys {
					if diff := before[k].compare(after[k]); diff != "" {
						t.Fatalf("seed %d: after step %d (%s), key %q did not survive recovery: %s\n"+
							"reproduce with: go test ./internal/engine/ -run 'TestGeneratedSequencesSurviveRecovery/seed=%d'\n"+
							"sequence up to the divergence:\n%s",
							seed, i, s, key, diff, seed, renderSteps(steps[:i+1]))
					}
				}
			}
		})
	}
}

// replayInto copies the log as it stands and recovers it into a fresh engine.
// The copy is what keeps the live engine untouched: recovery reopens the file
// for append at the last valid offset, and pointing a second writer at the live
// file would make the test the thing that corrupted it.
func replayInto(t *testing.T, dir, path string, step int) *engine.Engine {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	copyPath := filepath.Join(dir, "replay-"+strconv.Itoa(step)+".aof")
	if err := os.WriteFile(copyPath, data, 0o600); err != nil {
		t.Fatalf("copying the log: %v", err)
	}
	return newLoggedEngine(t, copyPath)
}

func newLoggedEngine(t *testing.T, path string) *engine.Engine {
	t.Helper()
	e := engine.New(func(err error) { t.Errorf("unexpected fatal: %v", err) })
	if _, err := e.OpenLog(path, aof.Always, func(err error) { t.Errorf("log fatal: %v", err) }); err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	return e
}

func finalize(t *testing.T, e *engine.Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.Finalize(ctx); err != nil {
		t.Errorf("finalising the log: %v", err)
	}
}

// keyState is everything a client can observe about one key.
type keyState struct {
	value     string
	found     bool
	ttlStatus engine.TTLStatus
	ttl       time.Duration
}

// Recovery restores an absolute deadline, so the time left on a recovered key
// is the live engine's minus however long the replay took. The tolerance covers
// that and nothing more: every generated expiry is at least 100 seconds, so no
// honest difference can approach it, and a dropped TTL shows up as a changed
// status rather than as a changed duration.
const ttlTolerance = 5 * time.Second

func (a keyState) compare(b keyState) string {
	if a.found != b.found {
		return "present=" + strconv.FormatBool(a.found) + " before, " + strconv.FormatBool(b.found) + " after"
	}
	if !a.found {
		return ""
	}
	if a.value != b.value {
		return "value " + strconv.Quote(a.value) + " before, " + strconv.Quote(b.value) + " after"
	}
	if a.ttlStatus != b.ttlStatus {
		return "TTL status " + ttlName(a.ttlStatus) + " before, " + ttlName(b.ttlStatus) + " after"
	}
	if a.ttlStatus == engine.HasTTL {
		drift := a.ttl - b.ttl
		if drift < 0 {
			drift = -drift
		}
		if drift > ttlTolerance {
			return "TTL moved by " + drift.String() + " (" + a.ttl.String() + " before, " + b.ttl.String() + " after)"
		}
	}
	return ""
}

func ttlName(st engine.TTLStatus) string {
	switch st {
	case engine.NoKey:
		return "NoKey"
	case engine.NoTTL:
		return "NoTTL"
	default:
		return "HasTTL"
	}
}

func snapshot(e *engine.Engine, keys []string) []keyState {
	out := make([]keyState, 0, len(keys))
	for _, k := range keys {
		v, found := e.Get(k)
		d, st := e.TTL(k)
		out = append(out, keyState{value: v, found: found, ttlStatus: st, ttl: d})
	}
	return out
}

func toBytes(s cmdgen.Step) [][]byte {
	out := make([][]byte, len(s))
	for i, part := range s {
		out[i] = []byte(part)
	}
	return out
}

// A value error is part of what the sequence is exercising; a drain or a broken
// log is not, and would make the comparison meaningless rather than failing it.
func isServerFault(msg string) bool {
	switch msg {
	case "ERR internal error", "ERR persistence unavailable", "ERR server is shutting down":
		return true
	}
	return false
}

func renderSteps(steps []cmdgen.Step) string {
	out := ""
	for i, s := range steps {
		out += "  " + strconv.Itoa(i) + ": " + s.String() + "\n"
	}
	return out
}
