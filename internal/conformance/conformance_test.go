// Package conformance compares this server's observable behaviour against a
// real Redis instance for the command subset documented in docs/protocol.md.
//
// The value of these tests is that they are not written against our own
// expectations. Every other suite in this repository asserts what we believe
// the behaviour should be; these assert what an independent implementation
// actually does. Redis behaviour outside the documented subset is not part of
// our contract and is not exercised here.
//
// They skip unless REDIS_ADDR is set.
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

// dialTimeout bounds every conformance connection. A framing defect makes a
// client wait for a reply that will never arrive, so an unbounded read would
// turn a failure into a hang.
const dialTimeout = 5 * time.Second

// startOurServer starts an in-process server on an ephemeral port and returns
// its address. Each call gets a fresh engine, which is how these tests keep the
// two sides symmetric: Redis is reset with FLUSHDB, and we have no equivalent
// command, so we start over instead.
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

// TestClientSendsOnlySupportedCommands verifies the test client does not
// smuggle in HELLO, CLIENT or any other command outside our documented subset.
//
// Our server answers anything outside the subset with an unknown-command error,
// so it doubles as the detector: if the client negotiated RESP3 or issued a
// hidden administrative command, dialling and running the basic operations
// would fail here. Without this check, a differential mismatch could be the
// client's fault rather than ours.
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

// normalise reduces a redigo reply to a comparable form. Error messages are
// reduced to their class, because docs/protocol.md documents classes, not text.
//
// A transport failure is not a reply and must never be normalised into one: if
// it were, a connection our server dropped could be compared against a Redis
// reply and reported as a mere difference in wording. It fails the test loudly
// instead.
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

// asRedisError reports whether err is a server-sent error reply. redigo models
// those as redis.Error and everything else as an ordinary error.
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
			// A fresh server per scenario, so no scenario can depend on a key
			// another one happened to leave behind.
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
		// Start from a clean slate so key state cannot leak between scenarios.
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

// describe renders a step for a failure message without dumping a 70 KB value.
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
// property. Single request/response exchanges cannot detect a reply that splits
// into two frames: the client reads the first, calls it the answer, and the
// stray second frame is not noticed until the next command reads it by mistake.
//
// The scenario deliberately includes a command name containing CRLF. Error text
// quotes that name, so an encoder that does not neutralise CR and LF emits an
// extra frame here and every later reply is answered by the previous command's
// leftovers. Redis is the oracle for what the sequence should be.
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

// runPipelined sends every command before reading any reply, so the replies are
// matched to commands by position in the stream rather than by turn-taking.
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
				// A timeout here means a reply never arrived: the stream is
				// misaligned, which is exactly the defect this test exists for.
				t.Fatalf("receive %d (%s) failed, replies are misaligned: %v",
					i, describe(steps[i]), err)
			}
		}
		out = append(out, normalise(t, reply, err))
	}
	return out
}
