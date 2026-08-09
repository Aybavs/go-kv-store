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
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/server"
)

// A framing defect makes a client wait for a reply that never arrives, so an
// unbounded read would turn a failure into a hang.
const dialTimeout = 5 * time.Second

// Each call gets a fresh engine: Redis is reset with FLUSHDB and we have no
// equivalent, so both sides start clean.
//
// With KV_AOF=1 the engine also writes an append-only file, into a directory
// the test owns. Persistence is meant to be invisible to the protocol, and the
// whole suite passing either way is what makes that a checked claim rather than
// an assumption.
func startOurServer(t *testing.T) string {
	t.Helper()
	cfg := server.DefaultConfig()
	cfg.Addr = "127.0.0.1:0"

	sup := server.NewSupervisor()
	eng := engine.New(sup.Fatal)

	if os.Getenv("KV_AOF") == "1" {
		path := filepath.Join(t.TempDir(), "appendonly.aof")
		if _, err := eng.OpenLog(path, aof.Always, sup.Fatal); err != nil {
			t.Fatalf("opening the append-only file: %v", err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := eng.Finalize(ctx); err != nil {
				t.Errorf("finalising the log: %v", err)
			}
		})
	}
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
	case strings.Contains(msg, "not an integer"):
		return "not-an-integer"
	case strings.Contains(msg, "invalid expire time"):
		return "invalid-expire-time"
	// Its own class, not a variant of not-an-integer. Without this row both
	// servers report "other" for every overflow scenario below and the whole
	// comparison passes without comparing anything.
	case strings.Contains(msg, "would overflow"):
		return "overflow"
	case strings.Contains(msg, "invalid cursor"):
		return "invalid-cursor"
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
	// MGET is the only command that replies with an array, so these are also
	// the only scenarios that compare array framing against Redis.
	"mget one":          {cmd("SET", "a", "1"), cmd("MGET", "a")},
	"mget many":         {cmd("SET", "a", "1"), cmd("SET", "b", "2"), cmd("MGET", "a", "b")},
	"mget with a miss":  {cmd("SET", "a", "1"), cmd("MGET", "a", "nope", "b")},
	"mget duplicates":   {cmd("SET", "a", "1"), cmd("MGET", "a", "a")},
	"mget all missing":  {cmd("MGET", "x", "y")},
	"mget empty value":  {cmd("SET", "a", ""), cmd("MGET", "a", "nope")},
	"mget binary key":   {cmd("SET", "k\x00\r\n", "v"), cmd("MGET", "k\x00\r\n", "k")},
	"mget binary value": {cmd("SET", "a", "x\x00y\r\nz"), cmd("MGET", "a")},
	"mget after del":    {cmd("SET", "a", "1"), cmd("DEL", "a"), cmd("MGET", "a")},
	"mget key with ttl": {cmd("SET", "a", "1", "EX", "100"), cmd("MGET", "a")},
	"mget long value":   {cmd("SET", "a", makeString(70000)), cmd("MGET", "a")},
	"mget wrong arity":  {cmd("MGET")},
	// INCR and DECR. The value-grammar rows are the point: Go's strconv accepts
	// "+5", "07", "00" and "-0", and Redis accepts none of them, so those four
	// are exactly where an implementation built on the standard library
	// diverges from the oracle.
	"incr missing key":       {cmd("INCR", "n"), cmd("GET", "n")},
	"decr missing key":       {cmd("DECR", "n"), cmd("GET", "n")},
	"incr existing":          {cmd("SET", "n", "5"), cmd("INCR", "n"), cmd("GET", "n")},
	"decr existing":          {cmd("SET", "n", "5"), cmd("DECR", "n"), cmd("GET", "n")},
	"incr zero":              {cmd("SET", "n", "0"), cmd("INCR", "n")},
	"incr negative":          {cmd("SET", "n", "-3"), cmd("INCR", "n")},
	"decr below zero":        {cmd("DECR", "n"), cmd("DECR", "n")},
	"incr plus sign":         {cmd("SET", "n", "+5"), cmd("INCR", "n"), cmd("GET", "n")},
	"incr leading zero":      {cmd("SET", "n", "07"), cmd("INCR", "n"), cmd("GET", "n")},
	"incr double zero":       {cmd("SET", "n", "00"), cmd("INCR", "n")},
	"incr negative zero":     {cmd("SET", "n", "-0"), cmd("INCR", "n")},
	"incr leading space":     {cmd("SET", "n", " 5"), cmd("INCR", "n")},
	"incr trailing space":    {cmd("SET", "n", "5 "), cmd("INCR", "n")},
	"incr non numeric":       {cmd("SET", "n", "abc"), cmd("INCR", "n"), cmd("GET", "n")},
	"incr empty value":       {cmd("SET", "n", ""), cmd("INCR", "n")},
	"incr float value":       {cmd("SET", "n", "3.0"), cmd("INCR", "n")},
	"incr binary value":      {cmd("SET", "n", "5\r\n"), cmd("INCR", "n")},
	"incr beyond int64":      {cmd("SET", "n", "92233720368547758080"), cmd("INCR", "n")},
	"incr overflows":         {cmd("SET", "n", "9223372036854775807"), cmd("INCR", "n"), cmd("GET", "n")},
	"decr underflows":        {cmd("SET", "n", "-9223372036854775808"), cmd("DECR", "n"), cmd("GET", "n")},
	"decr from the maximum":  {cmd("SET", "n", "9223372036854775807"), cmd("DECR", "n")},
	"incr keeps ttl":         {cmd("SET", "n", "5", "EX", "100"), cmd("INCR", "n"), cmd("TTL", "n")},
	"decr keeps ttl":         {cmd("SET", "n", "5", "EX", "100"), cmd("DECR", "n"), cmd("TTL", "n")},
	"incr creates no ttl":    {cmd("INCR", "n"), cmd("TTL", "n")},
	"incr after persist":     {cmd("SET", "n", "5", "EX", "100"), cmd("PERSIST", "n"), cmd("INCR", "n"), cmd("TTL", "n")},
	"incr after expire":      {cmd("SET", "n", "5"), cmd("EXPIRE", "n", "100"), cmd("INCR", "n"), cmd("TTL", "n")},
	"incr then mget":         {cmd("INCR", "n"), cmd("INCR", "n"), cmd("MGET", "n", "nope")},
	"incr after del":         {cmd("SET", "n", "5"), cmd("DEL", "n"), cmd("INCR", "n")},
	"incr rejected is inert": {cmd("SET", "n", "abc"), cmd("INCR", "n"), cmd("INCR", "n"), cmd("GET", "n")},
	"incr wrong arity":       {cmd("INCR")},
	"decr wrong arity":       {cmd("DECR")},
	"incr too many args":     {cmd("INCR", "a", "b")},

	"unknown command":   {cmd("TOTALLYBOGUS")},
	"unknown lowercase": {cmd("totallybogus")},
	"set wrong arity":   {cmd("SET", "only-key")},
	// EX and PX are implemented as of v0.2, so the interesting cases moved to
	// the expiration block below. NX and KEEPTTL still diverge by design: Redis
	// executes them and we answer with the same syntax error it gives an option
	// it does not know, so the texts agree while the behaviour does not. That
	// deviation is recorded in docs/protocol.md rather than exercised here.
	"set option redis also rejects": {cmd("SET", "k", "v", "extra")},
	"get wrong arity":               {cmd("GET")},
	"del wrong arity":               {cmd("DEL")},
	"exists wrong arity":            {cmd("EXISTS")},
	"ping wrong arity":              {cmd("PING", "a", "b")},
	"lowercase command":             {cmd("set", "a", "1"), cmd("get", "a")},
	"mixed case command":            {cmd("SeT", "a", "1"), cmd("GeT", "a")},
	"long value":                    {cmd("SET", "a", makeString(70000)), cmd("GET", "a")},

	// Expiration. Deadlines here are long enough that the seconds a TTL
	// reports cannot change between the two servers being asked, so these stay
	// deterministic despite involving time. The one genuine transition is
	// exercised separately in TestExpiryIsObservedByBothServers.
	"set ex then ttl":         {cmd("SET", "a", "1", "EX", "100"), cmd("TTL", "a")},
	"set px then ttl":         {cmd("SET", "a", "1", "PX", "100000"), cmd("TTL", "a")},
	"set lowercase option":    {cmd("SET", "a", "1", "ex", "100"), cmd("TTL", "a")},
	"set repeated option":     {cmd("SET", "a", "1", "EX", "10", "EX", "100"), cmd("TTL", "a")},
	"set clears ttl":          {cmd("SET", "a", "1", "EX", "100"), cmd("SET", "a", "2"), cmd("TTL", "a")},
	"ttl without ttl":         {cmd("SET", "a", "1"), cmd("TTL", "a")},
	"ttl missing key":         {cmd("TTL", "nope")},
	"persist removes ttl":     {cmd("SET", "a", "1", "EX", "100"), cmd("PERSIST", "a"), cmd("TTL", "a")},
	"persist without ttl":     {cmd("SET", "a", "1"), cmd("PERSIST", "a")},
	"persist missing key":     {cmd("PERSIST", "nope")},
	"expire applies":          {cmd("SET", "a", "1"), cmd("EXPIRE", "a", "100"), cmd("TTL", "a")},
	"expire missing key":      {cmd("EXPIRE", "nope", "100")},
	"expire replaces ttl":     {cmd("SET", "a", "1", "EX", "10"), cmd("EXPIRE", "a", "100"), cmd("TTL", "a")},
	"expire zero deletes":     {cmd("SET", "a", "1"), cmd("EXPIRE", "a", "0"), cmd("EXISTS", "a")},
	"expire negative deletes": {cmd("SET", "a", "1"), cmd("EXPIRE", "a", "-5"), cmd("EXISTS", "a")},
	"expire zero missing key": {cmd("EXPIRE", "nope", "0")},

	// Option and argument errors, compared by class.
	"set ex zero":           {cmd("SET", "a", "1", "EX", "0")},
	"set ex negative":       {cmd("SET", "a", "1", "EX", "-1")},
	"set px zero":           {cmd("SET", "a", "1", "PX", "0")},
	"set ex out of range":   {cmd("SET", "a", "1", "EX", "9999999999999999")},
	"set ex not an integer": {cmd("SET", "a", "1", "EX", "abc")},
	"set ex without value":  {cmd("SET", "a", "1", "EX")},
	"set ex and px":         {cmd("SET", "a", "1", "EX", "10", "PX", "100")},
	"set unknown option":    {cmd("SET", "a", "1", "BOGUS")},
	"expire not an integer": {cmd("SET", "a", "1"), cmd("EXPIRE", "a", "abc")},
	"expire out of range":   {cmd("SET", "a", "1"), cmd("EXPIRE", "a", "9999999999999999")},
	"expire wrong arity":    {cmd("EXPIRE", "a")},
	"ttl wrong arity":       {cmd("TTL")},
	"persist wrong arity":   {cmd("PERSIST")},

	// Key discovery. Ordinary KEYS and SCAN replies are intentionally absent:
	// KEYS order and SCAN cursor/page boundaries are implementation details, so
	// TestDiscoveryMatchesRedisOnStableDataset compares their complete sets.
	"scan invalid cursor": {cmd("SCAN", "nope")},
	"scan count zero":     {cmd("SCAN", "0", "COUNT", "0")},
	"scan count invalid":  {cmd("SCAN", "0", "COUNT", "nope")},
	"scan missing match":  {cmd("SCAN", "0", "MATCH")},
	"dbsize empty":        {cmd("DBSIZE")},
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

// do runs one command and returns its normalised reply. Go will not let a
// two-value call be spliced into a longer argument list, so this exists rather
// than a temporary pair at every call site.
func do(t *testing.T, conn redis.Conn, args ...interface{}) string {
	t.Helper()
	reply, err := conn.Do(args[0].(string), args[1:]...)
	return normalise(t, reply, err)
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func collectScan(t *testing.T, conn redis.Conn, pattern string, count int) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	seenCursors := map[string]struct{}{}
	cursor := "0"
	for first := true; first || cursor != "0"; first = false {
		if _, exists := seenCursors[cursor]; exists {
			t.Fatalf("SCAN repeated cursor %q", cursor)
		}
		seenCursors[cursor] = struct{}{}

		reply, err := redis.Values(conn.Do("SCAN", cursor, "MATCH", pattern, "COUNT", count))
		if err != nil {
			t.Fatalf("SCAN %s: %v", cursor, err)
		}
		var keys []string
		if _, err := redis.Scan(reply, &cursor, &keys); err != nil {
			t.Fatalf("decode SCAN: %v", err)
		}
		for _, key := range keys {
			if _, exists := out[key]; exists {
				t.Fatalf("SCAN returned key %q more than once", key)
			}
			out[key] = struct{}{}
		}
	}
	return out
}

func TestOurScanRejectsUnknownConsumedAndCompletedNumericTokens(t *testing.T) {
	conn := dial(t, startOurServer(t))
	if _, err := conn.Do("SCAN", "424242"); err == nil || err.Error() != "ERR invalid cursor" {
		t.Fatalf("unknown numeric cursor error = %v, want ERR invalid cursor", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if _, err := conn.Do("SET", key, "v"); err != nil {
			t.Fatalf("SET %s: %v", key, err)
		}
	}

	reply, err := redis.Values(conn.Do("SCAN", "0", "COUNT", "1"))
	if err != nil {
		t.Fatalf("initial SCAN: %v", err)
	}
	var firstCursor string
	var firstKeys []string
	if _, err := redis.Scan(reply, &firstCursor, &firstKeys); err != nil {
		t.Fatalf("decode initial SCAN: %v", err)
	}
	if firstCursor == "0" || len(firstKeys) != 1 {
		t.Fatalf("initial SCAN = cursor %q keys %q, want retained one-key page", firstCursor, firstKeys)
	}

	reply, err = redis.Values(conn.Do("SCAN", firstCursor, "COUNT", "1"))
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	var secondCursor string
	var secondKeys []string
	if _, err := redis.Scan(reply, &secondCursor, &secondKeys); err != nil {
		t.Fatalf("decode continuation: %v", err)
	}
	if secondCursor == "0" || secondCursor == firstCursor || len(secondKeys) != 1 {
		t.Fatalf("continuation = cursor %q keys %q, want replacement one-key page", secondCursor, secondKeys)
	}
	if _, err := conn.Do("SCAN", firstCursor); err == nil || err.Error() != "ERR invalid cursor" {
		t.Fatalf("consumed numeric cursor error = %v, want ERR invalid cursor", err)
	}
	if _, err := conn.Do("SCAN", secondCursor, "COUNT", "99"); err != nil {
		t.Fatalf("terminal continuation: %v", err)
	}
	if _, err := conn.Do("SCAN", secondCursor); err == nil || err.Error() != "ERR invalid cursor" {
		t.Fatalf("completed numeric cursor error = %v, want ERR invalid cursor", err)
	}
}

// TestDiscoveryMatchesRedisOnStableDataset compares only observable discovery
// results. Redis may choose a different KEYS order or SCAN cursor/page layout;
// on a stable dataset, the complete matching key set and cardinality must agree.
func TestDiscoveryMatchesRedisOnStableDataset(t *testing.T) {
	targets := []struct {
		name    string
		addr    string
		isRedis bool
	}{
		{"ours", startOurServer(t), false},
		{"redis", redisAddr(t), true},
	}
	keys := []string{"alpha", "alpine", "beta", "classa", "classb", "literal*", "q?mark", "é", "binary\x00\xff"}
	patterns := []string{"*", "alp*", "alph?", "class[ab]", "class[^a]", "literal\\*", "q\\?mark", "??", "binary??"}

	type result struct {
		size  int64
		keys  map[string]map[string]struct{}
		scans map[string]map[string]struct{}
	}
	results := map[string]result{}
	for _, target := range targets {
		conn := dial(t, target.addr)
		if target.isRedis {
			if _, err := conn.Do("FLUSHDB"); err != nil {
				t.Fatal(err)
			}
		}
		for _, key := range keys {
			if _, err := conn.Do("SET", key, "v"); err != nil {
				t.Fatalf("%s SET %q: %v", target.name, key, err)
			}
		}
		size, err := redis.Int64(conn.Do("DBSIZE"))
		if err != nil {
			t.Fatal(err)
		}
		r := result{size: size, keys: map[string]map[string]struct{}{}, scans: map[string]map[string]struct{}{}}
		for _, pattern := range patterns {
			gotKeys, err := redis.Strings(conn.Do("KEYS", pattern))
			if err != nil {
				t.Fatal(err)
			}
			r.keys[pattern] = stringSet(gotKeys)
			r.scans[pattern] = collectScan(t, conn, pattern, 3)
		}
		results[target.name] = r
	}
	if !reflect.DeepEqual(results["ours"], results["redis"]) {
		t.Fatalf("discovery mismatch:\nours:  %#v\nredis: %#v", results["ours"], results["redis"])
	}
}

// TestExpiryIsObservedByBothServers is the one place in this suite where
// waiting is legitimate. Everywhere else time is a parameter and expiry is
// reached by moving a clock; Redis's clock is genuinely out of reach, so the
// only way to compare the transition is to let it happen.
//
// The assertion is the transition itself — present, then absent — not how long
// it took. A test that asserted a duration would be measuring the scheduler.
func TestExpiryIsObservedByBothServers(t *testing.T) {
	redisTarget := redisAddr(t)

	const (
		lifetimeMillis = 200
		// Comfortably past the deadline. The test does not care how much of
		// this is slack, only that the key is gone by the end of it.
		waitFor = 600 * time.Millisecond
	)

	observe := func(addr string, isRedis bool) (before, after string) {
		t.Helper()
		conn := dial(t, addr)
		if isRedis {
			if _, err := conn.Do("FLUSHDB"); err != nil {
				t.Fatalf("FLUSHDB: %v", err)
			}
		}
		if _, err := conn.Do("SET", "k", "v", "PX", strconv.Itoa(lifetimeMillis)); err != nil {
			t.Fatalf("SET: %v", err)
		}
		before = do(t, conn, "GET", "k")
		time.Sleep(waitFor)
		after = do(t, conn, "GET", "k")
		return before, after
	}

	oursBefore, oursAfter := observe(startOurServer(t), false)
	redisBefore, redisAfter := observe(redisTarget, true)

	if oursBefore != redisBefore {
		t.Errorf("before the deadline:\n  ours:  %s\n  redis: %s", oursBefore, redisBefore)
	}
	if oursAfter != redisAfter {
		t.Errorf("after the deadline:\n  ours:  %s\n  redis: %s", oursAfter, redisAfter)
	}
	// Stated independently of the comparison, so a bug that made both sides
	// return the same wrong thing still fails.
	if oursBefore != "BULK:v" {
		t.Errorf("key was not readable before its deadline: %s", oursBefore)
	}
	if oursAfter != "NIL" {
		t.Errorf("key survived its deadline: %s", oursAfter)
	}
}

// TestExpiredKeyIsInvisibleToEveryRead checks that expiry is not something only
// GET notices. A key hidden from GET but still counted by EXISTS, or still
// reporting a TTL, would be a partially applied deadline.
func TestExpiredKeyIsInvisibleToEveryRead(t *testing.T) {
	redisTarget := redisAddr(t)

	probe := func(addr string, isRedis bool) []string {
		t.Helper()
		conn := dial(t, addr)
		if isRedis {
			if _, err := conn.Do("FLUSHDB"); err != nil {
				t.Fatalf("FLUSHDB: %v", err)
			}
		}
		if _, err := conn.Do("SET", "k", "v", "PX", "200"); err != nil {
			t.Fatalf("SET: %v", err)
		}
		time.Sleep(600 * time.Millisecond)
		out := []string{
			do(t, conn, "GET", "k"),
			do(t, conn, "EXISTS", "k"),
			do(t, conn, "TTL", "k"),
			do(t, conn, "PERSIST", "k"),
		}
		if got := do(t, conn, "DBSIZE"); got != "INT:0" {
			t.Errorf("DBSIZE after expiry = %s, want INT:0", got)
		}
		keys, err := redis.Strings(conn.Do("KEYS", "*"))
		if err != nil || len(keys) != 0 {
			t.Errorf("KEYS after expiry = %q, %v", keys, err)
		}
		if got := collectScan(t, conn, "*", 1); len(got) != 0 {
			t.Errorf("SCAN after expiry = %q, want empty", got)
		}
		return append(out, do(t, conn, "DEL", "k"))
	}

	ours := probe(startOurServer(t), false)
	want := probe(redisTarget, true)

	names := []string{"GET", "EXISTS", "TTL", "PERSIST", "DEL"}
	for i := range ours {
		if ours[i] != want[i] {
			t.Errorf("%s on an expired key:\n  ours:  %s\n  redis: %s", names[i], ours[i], want[i])
		}
	}
}
