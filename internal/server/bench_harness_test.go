package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
)

// The end-to-end measurement harness for v0.5.
//
// It lives in package server rather than in a package of its own so that it can
// wrap the listener by assigning s.ln, which adds nothing to the production
// build — no flag, no config field, no nil check on the accept path. The
// counting conn it installs is next to the code whose syscalls it counts.
//
// Everything here is a tool, not a gate. The workload runs only under
// KV_BENCH=1; a benchmark that CI must pass is a benchmark that will eventually
// be weakened to keep CI green. The one exception is
// TestSyscallCounterCountsWhatItClaims, which always runs, because every number
// the tool produces rests on that counter being right.

// benchServer starts a server whose accepted connections are counted. It
// deliberately does not go through RunWithReady: that would create its own
// listener, and this needs to supply one.
func benchServer(t testing.TB, policy string) (addr string, counter *connCounter, stop func()) {
	t.Helper()
	return benchServerWith(t, policy, nil)
}

func benchServerWith(t testing.TB, policy string, mutate func(*Config)) (addr string, counter *connCounter, stop func()) {
	t.Helper()

	cfg := DefaultConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	sup := NewSupervisor()
	eng := engine.New(sup.Fatal)

	if policy != "none" {
		p := aof.EverySec
		if policy == "always" {
			p = aof.Always
		}
		path := t.TempDir() + "/appendonly.aof"
		if _, err := eng.OpenLog(path, p, sup.Fatal); err != nil {
			t.Fatalf("opening the append-only file: %v", err)
		}
	}

	reg := command.New(eng)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, eng, reg, sup, logger)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	counter = &connCounter{}
	srv.ln = countingListener{Listener: ln, counter: counter}

	// Started explicitly because RunWithReady is not being used, so that the
	// measured server behaves like a running one rather than like a stripped
	// version of it.
	eng.StartExpiration()
	go srv.acceptLoop()

	stop = func() {
		_ = srv.ln.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = eng.StopExpiration(ctx)
		srv.closeAllConns()
		_ = eng.Finalize(ctx)
	}
	return srv.Addr().String(), counter, stop
}

// workload is one measured configuration.
//
// Pipeline is the number of commands sent before any reply is read. At 1 this
// is an ordinary request/response client; above 1 it is the shape the v0.5 work
// is about, because that is where a reply per write stops being necessary.
type workload struct {
	name     string
	conns    int
	pipeline int
	commands int // total across all connections
	policy   string
	// flushEveryReply selects the pre-v0.5 behaviour, so the two can be
	// compared inside one process rather than across two commits minutes apart.
	flushEveryReply bool
	command         func(i int) (string, []interface{})
}

func getWorkload(name string, conns, pipeline, commands int, policy string) workload {
	return workload{
		name: name, conns: conns, pipeline: pipeline, commands: commands, policy: policy,
		command: func(i int) (string, []interface{}) {
			return "GET", []interface{}{"key" + strconv.Itoa(i%1000)}
		},
	}
}

func setWorkload(name string, conns, pipeline, commands int, policy string) workload {
	return workload{
		name: name, conns: conns, pipeline: pipeline, commands: commands, policy: policy,
		command: func(i int) (string, []interface{}) {
			return "SET", []interface{}{"key" + strconv.Itoa(i%1000), "value"}
		},
	}
}

// result is one repetition of one workload.
type result struct {
	throughput    float64 // commands per second
	p50, p95, p99 time.Duration
	readsPerCmd   float64
	writesPerCmd  float64
}

// run executes one repetition and returns its result.
//
// Latencies are per *batch*, and at pipeline 1 a batch is one command. Above 1
// the two are not the same thing and are not comparable with each other; the
// report says so rather than leaving a reader to assume.
func run(t testing.TB, w workload) result {
	t.Helper()
	addr, counter, stop := benchServerWith(t, w.policy, func(c *Config) { c.flushEveryReply = w.flushEveryReply })
	defer stop()

	perConn := w.commands / w.conns
	batches := perConn / w.pipeline
	if batches == 0 {
		t.Fatalf("workload %q: pipeline %d exceeds %d commands per connection", w.name, w.pipeline, perConn)
	}

	// Preallocated: an allocation inside the measured loop is the defect v0.4
	// found in the parallel benchmarks, and here it would be measured as
	// latency.
	latencies := make([][]time.Duration, w.conns)
	for i := range latencies {
		latencies[i] = make([]time.Duration, 0, batches)
	}

	conns := make([]redis.Conn, w.conns)
	for i := range conns {
		c, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(5*time.Second))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conns[i] = c
		defer c.Close()
	}

	// Warm the keys and the connections before the counter is read, so neither
	// setup traffic nor a cold map lands in the measurement.
	for i := 0; i < 1000; i++ {
		if _, err := conns[0].Do("SET", "key"+strconv.Itoa(i), "value"); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	counter.reset()
	var wg sync.WaitGroup
	start := time.Now()
	for ci := 0; ci < w.conns; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			conn := conns[ci]
			n := ci * perConn
			for b := 0; b < batches; b++ {
				t0 := time.Now()
				for p := 0; p < w.pipeline; p++ {
					name, args := w.command(n)
					n++
					if err := conn.Send(name, args...); err != nil {
						return
					}
				}
				if err := conn.Flush(); err != nil {
					return
				}
				for p := 0; p < w.pipeline; p++ {
					if _, err := conn.Receive(); err != nil {
						return
					}
				}
				latencies[ci] = append(latencies[ci], time.Since(t0))
			}
		}(ci)
	}
	wg.Wait()
	elapsed := time.Since(start)

	all := make([]time.Duration, 0, w.conns*batches)
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	done := float64(len(all) * w.pipeline)
	return result{
		throughput:   done / elapsed.Seconds(),
		p50:          quantile(all, 0.50),
		p95:          quantile(all, 0.95),
		p99:          quantile(all, 0.99),
		readsPerCmd:  float64(counter.reads.Load()) / done,
		writesPerCmd: float64(counter.writes.Load()) / done,
	}
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}

// runInterleaved executes every workload once per round, in order, for reps
// rounds — A B A B, never A A B B.
//
// Machine state drifts across a run: thermal, other processes, page cache. A
// block layout attributes all of that drift to whichever configuration happened
// to run second. v0.4's everysec comparison used interleaved pairs and that is
// the only reason its deltas mean anything.
func runInterleaved(t testing.TB, reps int, workloads []workload) map[string][]result {
	t.Helper()
	out := make(map[string][]result, len(workloads))
	for r := 0; r < reps; r++ {
		for _, w := range workloads {
			out[w.name] = append(out[w.name], run(t, w))
		}
	}
	return out
}

// median and spread, because a single figure hides whether it means anything.
// spread is (max-min)/median: any difference smaller than it is not separable
// from noise on this machine, and saying so is a result.
func summarise(rs []result, pick func(result) float64) (median, spread float64) {
	vals := make([]float64, len(rs))
	for i, r := range rs {
		vals[i] = pick(r)
	}
	sort.Float64s(vals)
	median = vals[len(vals)/2]
	if median == 0 {
		return median, 0
	}
	return median, (vals[len(vals)-1] - vals[0]) / median
}

func report(t testing.TB, results map[string][]result, order []workload) {
	t.Helper()
	fmt.Printf("\n%-34s %12s %7s %9s %9s %9s %9s %9s\n",
		"workload", "cmd/s", "spread", "p50", "p95", "p99", "reads/cmd", "writes/cmd")
	for _, w := range order {
		rs := results[w.name]
		if len(rs) == 0 {
			continue
		}
		tp, spread := summarise(rs, func(r result) float64 { return r.throughput })
		p50, _ := summarise(rs, func(r result) float64 { return float64(r.p50) })
		p95, _ := summarise(rs, func(r result) float64 { return float64(r.p95) })
		p99, _ := summarise(rs, func(r result) float64 { return float64(r.p99) })
		reads, _ := summarise(rs, func(r result) float64 { return r.readsPerCmd })
		writes, _ := summarise(rs, func(r result) float64 { return r.writesPerCmd })
		fmt.Printf("%-34s %12.0f %6.1f%% %9s %9s %9s %9.3f %9.3f\n",
			w.name, tp, spread*100,
			time.Duration(p50).Round(time.Microsecond),
			time.Duration(p95).Round(time.Microsecond),
			time.Duration(p99).Round(time.Microsecond),
			reads, writes)
	}
	fmt.Printf("\nLatency is per batch. At pipeline 1 a batch is one command; above 1 the two\n" +
		"are different quantities and must not be compared with each other.\n" +
		"Absolute throughput includes the client's own cost and is not comparable with\n" +
		"redis-benchmark's; differences between rows are.\n\n")
}

// TestSyscallCounterCountsWhatItClaims runs always, unlike the rest of this
// file, because every number the harness produces is built on it.
//
// A request/response client must cost exactly one write per command: the server
// writes one reply into a 4 KiB buffer and flushes it, and a flush that fits is
// one Write call. If that is not what the counter says, the counter is wrong
// and so is everything measured with it.
//
// Reads are asserted as a lower bound rather than exactly, because a command
// arriving split across two segments legitimately costs two.
func TestSyscallCounterCountsWhatItClaims(t *testing.T) {
	const commands = 200
	addr, counter, stop := benchServer(t, "none")
	defer stop()

	conn, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Do("SET", "k", "v"); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	counter.reset()

	for i := 0; i < commands; i++ {
		if _, err := conn.Do("GET", "k"); err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
	}

	if got := counter.writes.Load(); got != commands {
		t.Errorf("writes = %d, want exactly %d; the counter is not counting one write per reply", got, commands)
	}
	if got := counter.reads.Load(); got < commands {
		t.Errorf("reads = %d, want at least %d", got, commands)
	}
	if got := counter.bytesWritten.Load(); got == 0 {
		t.Error("bytesWritten = 0; the counter is not seeing payload")
	}
}

func benchWorkloads() []workload {
	return []workload{
		getWorkload("get/1conn/nopipe/nolog", 1, 1, 2000, "none"),
		getWorkload("get/10conn/nopipe/nolog", 10, 1, 20000, "none"),
		getWorkload("get/50conn/nopipe/nolog", 50, 1, 50000, "none"),
		getWorkload("get/10conn/pipe8/nolog", 10, 8, 20000, "none"),
		getWorkload("get/10conn/pipe64/nolog", 10, 64, 20000, "none"),
		getWorkload("get/50conn/pipe64/nolog", 50, 64, 50000, "none"),
		setWorkload("set/10conn/nopipe/nolog", 10, 1, 20000, "none"),
		setWorkload("set/10conn/pipe64/nolog", 10, 64, 20000, "none"),
		setWorkload("set/10conn/nopipe/everysec", 10, 1, 20000, "everysec"),
		setWorkload("set/10conn/pipe64/everysec", 10, 64, 20000, "everysec"),
		setWorkload("set/10conn/nopipe/always", 10, 1, 2000, "always"),
	}
}

// TestBenchEndToEnd is the tool. KV_BENCH_REPS overrides the repetition count.
func TestBenchEndToEnd(t *testing.T) {
	if os.Getenv("KV_BENCH") != "1" {
		t.Skip("set KV_BENCH=1 to run the end-to-end measurement harness")
	}
	reps := 5
	if raw := os.Getenv("KV_BENCH_REPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			t.Fatalf("KV_BENCH_REPS=%q is not a positive number", raw)
		}
		reps = n
	}

	ws := benchWorkloads()
	report(t, runInterleaved(t, reps, ws), ws)
}

// TestBenchFlushComparison is the A/B the milestone turns on: the same workload
// with the deferred flush on and off, interleaved inside one process.
//
// Interleaving is not a nicety here. v0.4 measured this machine's end-to-end
// spread at up to 9%, so two runs minutes apart cannot separate a change worth
// less than that. Alternating them inside one process makes the pair share
// whatever the machine was doing.
func TestBenchFlushComparison(t *testing.T) {
	if os.Getenv("KV_BENCH") != "1" {
		t.Skip("set KV_BENCH=1 to run the end-to-end measurement harness")
	}
	reps := 5
	if raw := os.Getenv("KV_BENCH_REPS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			t.Fatalf("KV_BENCH_REPS=%q is not a positive number", raw)
		}
		reps = n
	}

	bases := []workload{
		getWorkload("get/1conn/nopipe", 1, 1, 2000, "none"),
		getWorkload("get/10conn/nopipe", 10, 1, 20000, "none"),
		getWorkload("get/50conn/nopipe", 50, 1, 50000, "none"),
		getWorkload("get/10conn/pipe8", 10, 8, 20000, "none"),
		getWorkload("get/10conn/pipe64", 10, 64, 20000, "none"),
		getWorkload("get/50conn/pipe64", 50, 64, 50000, "none"),
		setWorkload("set/10conn/pipe64", 10, 64, 20000, "none"),
		setWorkload("set/10conn/pipe64/everysec", 10, 64, 20000, "everysec"),
	}
	if os.Getenv("KV_BENCH_ONLY") != "" {
		var kept []workload
		for _, b := range bases {
			if strings.Contains(b.name, os.Getenv("KV_BENCH_ONLY")) {
				kept = append(kept, b)
			}
		}
		bases = kept
	}

	var ws []workload
	for _, base := range bases {
		before := base
		before.name = base.name + " [before]"
		before.flushEveryReply = true
		after := base
		after.name = base.name + " [after]"
		ws = append(ws, before, after)
	}

	report(t, runInterleaved(t, reps, ws), ws)
}

// TestBenchProfile writes a CPU profile of one workload, so the profile in
// docs/benchmarks.md is reproducible by a command rather than collected by hand.
func TestBenchProfile(t *testing.T) {
	if os.Getenv("KV_BENCH") != "1" {
		t.Skip("set KV_BENCH=1 to run the profiler")
	}
	path := os.Getenv("KV_BENCH_PROFILE")
	if path == "" {
		path = "cpu.prof"
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	w := getWorkload("profile/50conn/nopipe/nolog", 50, 1, 300000, "none")
	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatalf("start profile: %v", err)
	}
	r := run(t, w)
	pprof.StopCPUProfile()

	fmt.Printf("\nprofile written to %s\n  %.0f cmd/s, %.3f reads/cmd, %.3f writes/cmd\n\n",
		path, r.throughput, r.readsPerCmd, r.writesPerCmd)
}

// TestPipelinedBatchCostsOneWrite is the property v0.5 exists to produce, and
// the counter is what makes it a fact rather than an inference.
//
// The client sends every command before reading any reply, so they arrive in
// one segment, are parsed out of one buffer, and the reader never asks for more
// until the batch is exhausted. One write should carry all sixty-four replies.
func TestPipelinedBatchCostsOneWrite(t *testing.T) {
	const batch = 64
	addr, counter, stop := benchServer(t, "none")
	defer stop()

	conn, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Do("SET", "k", "v"); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	counter.reset()

	for i := 0; i < batch; i++ {
		if err := conn.Send("GET", "k"); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for i := 0; i < batch; i++ {
		got, err := redis.String(conn.Receive())
		if err != nil {
			t.Fatalf("receive %d: %v", i, err)
		}
		if got != "v" {
			t.Fatalf("reply %d = %q, want %q", i, got, "v")
		}
	}

	// One write for the whole batch. Allowing a couple covers the case where
	// the client's send is split across segments and the reader legitimately
	// has to block part-way through; anything near the batch size means the
	// replies are not being coalesced at all.
	if got := counter.writes.Load(); got > 2 {
		t.Errorf("writes = %d for a batch of %d, want 1 (2 tolerated for a split send); bytes written = %d, reads = %d",
			got, batch, counter.bytesWritten.Load(), counter.reads.Load())
	}
	if got := counter.writes.Load(); got == 0 {
		t.Error("writes = 0; the replies never left the server")
	}
}

// The same batch with the deferral switched off must still cost one write per
// reply. This pins that the switch the harness measures against actually
// switches something, so an interleaved A/B comparison is not measuring one
// configuration twice.
func TestFlushEveryReplyCostsOneWritePerReply(t *testing.T) {
	const batch = 64
	addr, counter, stop := benchServerWith(t, "none", func(c *Config) { c.flushEveryReply = true })
	defer stop()

	conn, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Do("SET", "k", "v"); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	counter.reset()

	for i := 0; i < batch; i++ {
		if err := conn.Send("GET", "k"); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	for i := 0; i < batch; i++ {
		if _, err := conn.Receive(); err != nil {
			t.Fatalf("receive %d: %v", i, err)
		}
	}

	// bytesWritten distinguishes the two ways this can go wrong: a total of
	// batch*len(reply) with fewer writes means replies were coalesced, while a
	// smaller total means a write went unseen.
	if got := counter.writes.Load(); got != batch {
		t.Errorf("writes = %d, want exactly %d with the deferral off (bytes written = %d, reads = %d)",
			got, batch, counter.bytesWritten.Load(), counter.reads.Load())
	}
}
