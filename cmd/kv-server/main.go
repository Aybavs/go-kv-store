// Command kv-server runs the go-kv-store server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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

func run() error {
	var (
		host            = flag.String("host", "127.0.0.1", "address to bind")
		port            = flag.Int("port", 6380, "port to listen on")
		maxClients      = flag.Int("max-clients", 1024, "maximum concurrent client connections")
		timeout         = flag.Duration("timeout", 0, "idle read timeout (0 disables)")
		shutdownTimeout = flag.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown budget")
		maxBulk         = flag.Int("max-bulk-length", 64<<20, "maximum bulk string length in bytes")
		maxArgs         = flag.Int("max-array-elements", 1024, "maximum arguments per command")
		logLevel        = flag.String("loglevel", "info", "log level: debug, info, warn, error")
	)
	flag.Parse()

	level, err := parseLevel(*logLevel)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	cfg := server.DefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.MaxClients = *maxClients
	cfg.ReadTimeout = *timeout
	cfg.ShutdownTimeout = *shutdownTimeout
	cfg.Limits = resp.Limits{MaxArrayElements: *maxArgs, MaxBulkLength: *maxBulk}

	// The supervisor is created first so the engine can report fatal
	// conditions into it without a dependency cycle.
	sup := server.NewSupervisor()
	eng := engine.New(sup.Fatal)
	reg := command.New(eng)
	srv := server.New(cfg, eng, reg, sup, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		if errors.Is(err, server.ErrShutdownTimeout) {
			return err
		}
		return err
	}
	return nil
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
