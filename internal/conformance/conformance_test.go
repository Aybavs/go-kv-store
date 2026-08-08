// Package conformance compares this server against a real Redis for the command
// subset in docs/protocol.md. Unlike every other suite here, these tests assert
// what an independent implementation does rather than what we expect. Redis
// behaviour outside the documented subset is not part of our contract.
//
// Skips unless REDIS_ADDR is set.
package conformance

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/server"
)

// A framing defect makes a client wait for a reply that never arrives, so an
// unbounded read would turn a failure into a hang.
const dialTimeout = 5 * time.Second

// Each call gets a fresh engine: Redis is reset with FLUSHDB and we have no
// equivalent, so both sides start clean.
func startOurServer(t *testing.T) string {
	t.Helper()
	cfg := server.DefaultConfig()
	cfg.Addr = "127.0.0.1:0"

	sup := server.NewSupervisor()
	eng := engine.New(sup.Fatal)
	reg := command.New(eng)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := server.New(cfg, eng, reg, sup, logger)

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- srv.RunWithReady(ctx, ready) }()
	<-ready

	addr := srv.Addr().String()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("server did not shut down")
		}
	})
	return addr
}

func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR is not set; start Redis and set REDIS_ADDR=127.0.0.1:6379 to run conformance tests")
	}
	return addr
}

func dial(t *testing.T, addr string) redis.Conn {
	t.Helper()
	conn, err := redis.Dial("tcp", addr,
		redis.DialConnectTimeout(dialTimeout),
		redis.DialReadTimeout(dialTimeout),
		redis.DialWriteTimeout(dialTimeout))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestClientSendsOnlySupportedCommands checks the client does not smuggle in
// HELLO or CLIENT. Our server rejects anything outside the subset, so it is its
// own detector; without this, a mismatch could be the client's fault, not ours.
func TestClientSendsOnlySupportedCommands(t *testing.T) {
	conn := dial(t, startOurServer(t))

	if _, err := redis.String(conn.Do("PING")); err != nil {
		t.Fatalf("PING (client may be negotiating RESP3 or sending HELLO): %v", err)
	}
	if _, err := redis.String(conn.Do("SET", "k", "v")); err != nil {
		t.Fatalf("SET: %v", err)
	}
	got, err := redis.String(conn.Do("GET", "k"))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got != "v" {
		t.Fatalf("GET = %q, want %q", got, "v")
	}
}

// step is one command in a scenario.
type step struct {
	args []interface{}
}

func cmd(parts ...interface{}) step { return step{args: parts} }

// normalise reduces a reply to a comparable form; errors to their class, since
// docs/protocol.md documents classes rather than text. A transport failure is
// not a reply and fails the test outright rather than being classified.
func normalise(t *testing.T, reply interface{}, err error) string {
	t.Helper()
	if err != nil {
		var rerr redis.Error
		if !asRedisError(err, &rerr) {
			t.Fatalf("transport failure, not a reply: %v", err)
		}
		return "ERROR:" + errorClass(string(rerr))
	}
	switch v := reply.(type) {
	case nil:
		return "NIL"
	case int64:
		return "INT:" + strconv.FormatInt(v, 10)
	case []byte:
		return "BULK:" + string(v)
	case string:
		return "STATUS:" + v
	case []interface{}:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, normalise(t, item, nil))
		}
		return "ARRAY:[" + strings.Join(parts, ",") + "]"
	default:
		t.Fatalf("unhandled reply type %T (%v)", reply, reply)
		return ""
	}
}

// redigo models server-sent error replies as redis.Error, everything else as an
// ordinary error.
func asRedisError(err error, out *redis.Error) bool {
	rerr, ok := err.(redis.Error)
	if ok {
		*out = rerr
	}
	return ok
}

func errorClass(msg string) string {
	switch {
	case strings.Contains(msg, "unknown command"):
		return "unknown-command"
	case strings.Contains(msg, "wrong number of arguments"):
		return "wrong-arity"
	case strings.Contains(msg, "syntax error"):
		return "syntax-error"
	default:
		return "other"
	}
}

var scenarios = map[string][]step{
	"ping":                {cmd("PING")},
	"ping with message":   {cmd("PING", "hello")},
	"ping binary message": {cmd("PING", "a\x00\r\nb")},
	"set then get":        {cmd("SET", "a", "1"), cmd("GET", "a")},
	"get missing":         {cmd("GET", "nope")},
	"overwrite":           {cmd("SET", "a", "1"), cmd("SET", "a", "2"), cmd("GET", "a")},
	"empty value":         {cmd("SET", "a", ""), cmd("GET", "a")},
	"empty key":           {cmd("SET", "", "v"), cmd("GET", ""), cmd("EXISTS", "")},
	"binary value":        {cmd("SET", "a", "x\x00y\r\nz"), cmd("GET", "a")},
	"binary key":          {cmd("SET", "k\x00\r\n", "v"), cmd("GET", "k\x00\r\n")},
	"del one":             {cmd("SET", "a", "1"), cmd("DEL", "a"), cmd("GET", "a")},
	"del many":            {cmd("SET", "a", "1"), cmd("SET", "b", "2"), cmd("DEL", "a", "b", "missing")},
	"del twice":           {cmd("SET", "a", "1"), cmd("DEL", "a"), cmd("DEL", "a")},
	"del duplicates":      {cmd("SET", "a", "1"), cmd("DEL", "a", "a")},
	"del missing":         {cmd("DEL", "nope")},
	"exists one":          {cmd("SET", "a", "1"), cmd("EXISTS", "a")},
	"exists duplicates":   {cmd("SET", "a", "1"), cmd("EXISTS", "a", "a", "missing")},
	"exists missing":      {cmd("EXISTS", "nope")},
	"exists empty value":  {cmd("SET", "a", ""), cmd("EXISTS", "a")},
	"unknown command":     {cmd("TOTALLYBOGUS")},
	"unknown lowercase":   {cmd("totallybogus")},
	"set wrong arity":     {cmd("SET", "only-key")},
	// "extra" is not a SET option in Redis either, so both servers reject it.
	// An option Redis does implement (EX, NX, ...) would diverge by design
	// until v0.2 and is recorded as a deviation in docs/protocol.md instead.
	"set unknown option": {cmd("SET", "k", "v", "extra")},
	"get wrong arity":    {cmd("GET")},
	"del wrong arity":    {cmd("DEL")},
	"exists wrong arity": {cmd("EXISTS")},
	"ping wrong arity":   {cmd("PING", "a", "b")},
	"lowercase command":  {cmd("set", "a", "1"), cmd("get", "a")},
	"mixed case command": {cmd("SeT", "a", "1"), cmd("GeT", "a")},
	"long value":         {cmd("SET", "a", makeString(70000)), cmd("GET", "a")},
}

func makeString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return string(b)
}

// TestDifferentialAgainstRedis runs each scenario against both servers and
// requires the normalised results to match.
func TestDifferentialAgainstRedis(t *testing.T) {
	redisTarget := redisAddr(t)

	names := make([]string, 0, len(scenarios))
	for name := range scenarios {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		steps := scenarios[name]
		t.Run(name, func(t *testing.T) {
			// Fresh server per scenario: nothing can depend on a leftover key.
			got := runScenario(t, startOurServer(t), steps, false)
			want := runScenario(t, redisTarget, steps, true)
			if len(got) != len(want) {
				t.Fatalf("step count mismatch: ours=%d redis=%d", len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("step %d (%s):\n  ours:  %s\n  redis: %s",
						i, describe(steps[i]), got[i], want[i])
				}
			}
		})
	}
}

func runScenario(t *testing.T, addr string, steps []step, isRedis bool) []string {
	t.Helper()
	conn := dial(t, addr)

	if isRedis {
		if _, err := conn.Do("FLUSHDB"); err != nil {
			t.Fatalf("FLUSHDB: %v", err)
		}
	}

	out := make([]string, 0, len(steps))
	for _, s := range steps {
		name := s.args[0].(string)
		reply, err := conn.Do(name, s.args[1:]...)
		out = append(out, normalise(t, reply, err))
	}
	return out
}

// Renders a step for a failure message without dumping a 70 KB value.
func describe(s step) string {
	parts := make([]string, 0, len(s.args))
	for _, a := range s.args {
		str, ok := a.(string)
		if !ok {
			parts = append(parts, "?")
			continue
		}
		if len(str) > 32 {
			str = str[:32] + "...(" + strconv.Itoa(len(str)) + " bytes)"
		}
		parts = append(parts, strconv.Quote(str))
	}
	return strings.Join(parts, " ")
}

// TestPipelinedRepliesStayAligned is the differential form of the framing
// property. Request/response exchanges cannot see a split reply: the client
// reads the first frame, calls it the answer, and the stray one is not noticed
// until the next command consumes it. Two command names here contain CRLF.
func TestPipelinedRepliesStayAligned(t *testing.T) {
	redisTarget := redisAddr(t)

	steps := []step{
		cmd("SET", "a", "1"),
		cmd("BOGUS\r\n+INJECTED"),
		cmd("GET", "a"),
		cmd("BOGUS\r\n-ERR fake"),
		cmd("EXISTS", "a"),
		cmd("PING"),
	}

	got := runPipelined(t, startOurServer(t), steps, false)
	want := runPipelined(t, redisTarget, steps, true)

	if len(got) != len(want) {
		t.Fatalf("reply count mismatch: ours=%d redis=%d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("reply %d (%s):\n  ours:  %s\n  redis: %s",
				i, describe(steps[i]), got[i], want[i])
		}
	}
}

// Sends everything before reading, so replies match commands by position in the
// stream rather than by turn-taking.
func runPipelined(t *testing.T, addr string, steps []step, isRedis bool) []string {
	t.Helper()
	conn := dial(t, addr)

	if isRedis {
		if _, err := conn.Do("FLUSHDB"); err != nil {
			t.Fatalf("FLUSHDB: %v", err)
		}
	}

	for _, s := range steps {
		if err := conn.Send(s.args[0].(string), s.args[1:]...); err != nil {
			t.Fatalf("send %s: %v", describe(s), err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	out := make([]string, 0, len(steps))
	for i := range steps {
		reply, err := conn.Receive()
		if err != nil {
			var rerr redis.Error
			if !asRedisError(err, &rerr) {
				// A timeout means a reply never arrived: the stream is misaligned.
				t.Fatalf("receive %d (%s) failed, replies are misaligned: %v",
					i, describe(steps[i]), err)
			}
		}
		out = append(out, normalise(t, reply, err))
	}
	return out
}
