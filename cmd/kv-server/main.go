// Command kv-server runs the go-kv-store server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aybavs/go-kv-store/internal/aof"
	"github.com/aybavs/go-kv-store/internal/command"
	"github.com/aybavs/go-kv-store/internal/engine"
	"github.com/aybavs/go-kv-store/internal/resp"
	"github.com/aybavs/go-kv-store/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kv-server:", err)
		os.Exit(1)
	}
}

// options holds every value the command line can set. It exists so the flag
// surface can be enumerated by something other than the code that consumes it.
//
// From v1.0 that surface is part of the compatibility promise: a 1.x may add a
// flag and may not rename one, remove one, or change what one defaults to. ADR
// 0007 explains why defaults are included — a default is behaviour nobody typed
// and therefore never opted into. registerFlags is what lets a test hold the
// promise to that, by binding into a FlagSet it owns rather than into the global
// one the process uses.
type options struct {
	host            *string
	port            *int
	maxClients      *int
	timeout         *time.Duration
	shutdownTimeout *time.Duration
	maxBulk         *int
	maxArgs         *int
	maxCommand      *int
	logLevel        *string

	appendOnly     *bool
	appendFilename *string
	appendFsync    *string
}

func registerFlags(fs *flag.FlagSet) *options {
	return &options{
		host:            fs.String("host", "127.0.0.1", "address to bind"),
		port:            fs.Int("port", 6380, "port to listen on"),
		maxClients:      fs.Int("max-clients", 1024, "maximum concurrent client connections"),
		timeout:         fs.Duration("timeout", 0, "idle read timeout (0 disables)"),
		shutdownTimeout: fs.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown budget"),
		maxBulk:         fs.Int("max-bulk-length", 64<<20, "maximum bulk string length in bytes"),
		maxArgs:         fs.Int("max-array-elements", 1024, "maximum arguments per command"),
		maxCommand:      fs.Int("max-command-bytes", 128<<20, "maximum total argument bytes in one command"),
		logLevel:        fs.String("loglevel", "info", "log level: debug, info, warn, error"),

		appendOnly:     fs.Bool("appendonly", false, "write an append-only file so data survives a restart"),
		appendFilename: fs.String("appendfilename", "appendonly.aof", "path to the append-only file"),
		// The everysec wording is deliberate and belongs here rather than only
		// in docs. Redis's everysec acknowledges before the write; ours
		// acknowledges only once write() has succeeded. Someone reading --help
		// is exactly the person who would otherwise assume the names mean the
		// same thing.
		appendFsync: fs.String("appendfsync", "everysec",
			"durability: 'always' acknowledges after fsync; 'everysec' acknowledges after write() succeeds "+
				"and syncs about once a second. Note this is stronger than Redis's everysec, which acknowledges "+
				"before writing"),
	}
}

func run() error {
	opt := registerFlags(flag.CommandLine)
	flag.Parse()

	level, err := parseLevel(*opt.logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg := server.DefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", *opt.host, *opt.port)
	cfg.MaxClients = *opt.maxClients
	cfg.ReadTimeout = *opt.timeout
	cfg.ShutdownTimeout = *opt.shutdownTimeout
	cfg.Limits = resp.Limits{
		MaxArrayElements: *opt.maxArgs,
		MaxBulkLength:    *opt.maxBulk,
		MaxCommandBytes:  *opt.maxCommand,
	}

	policy, err := parsePolicy(*opt.appendFsync)
	if err != nil {
		return err
	}

	// The supervisor is created first so the engine can report fatal
	// conditions into it without a dependency cycle.
	sup := server.NewSupervisor()
	eng := engine.New(sup.Fatal)

	if *opt.appendOnly {
		// Recovery happens before the listener opens. A file we cannot trust
		// must stop the process, not produce a server answering from a state
		// nobody can account for.
		res, err := eng.OpenLog(*opt.appendFilename, policy, sup.Fatal)
		if err != nil {
			return fmt.Errorf("recovering %s: %w", *opt.appendFilename, err)
		}
		logger.Info("recovered append-only file",
			"path", *opt.appendFilename,
			"records", res.Records,
			"offset", res.LastValidOffset)
		if res.Truncated {
			// Expected after a crash, and worth saying out loud: an operator
			// looking at a restart should know the tail was incomplete.
			logger.Warn("append-only file ended part-way through a record; truncated to the last complete one",
				"offset", res.LastValidOffset)
		}
	}

	reg := command.New(eng)
	srv := server.New(cfg, eng, reg, sup, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return srv.Run(ctx)
}

func parsePolicy(s string) (aof.Policy, error) {
	switch s {
	case "everysec":
		return aof.EverySec, nil
	case "always":
		return aof.Always, nil
	default:
		return 0, fmt.Errorf("invalid -appendfsync %q: want everysec or always", s)
	}
}

func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid -loglevel %q (want debug, info, warn or error)", s)
	}
}
