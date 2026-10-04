package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/ozon/internal/core"
	"example.com/ozon/internal/httpapi"
	"example.com/ozon/internal/store/memory"
	"example.com/ozon/internal/store/postgres"
	"example.com/ozon/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	storage := flag.String("storage", "memory", "storage: memory or postgres")
	address := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	migrate := flag.Bool("migrate", true, "apply embedded migrations when using PostgreSQL")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var store core.Store
	switch *storage {
	case "memory":
		store = memory.New()
	case "postgres":
		dsn := os.Getenv("DATABASE_URL")
		if dsn == "" {
			return errors.New("DATABASE_URL is required for postgres storage")
		}
		startup, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if *migrate {
			if err := migrations.Up(startup, dsn); err != nil {
				return err
			}
		}
		config, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return errors.New("invalid DATABASE_URL")
		}
		config.MaxConns = 20
		pool, err := pgxpool.NewWithConfig(startup, config)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer pool.Close()
		if err = pool.Ping(startup); err != nil {
			return fmt.Errorf("ping database: %w", err)
		}
		pgStore := postgres.New(pool)
		stopNotifications, err := pgStore.StartNotifications(ctx, logger)
		if err != nil {
			return err
		}
		defer stopNotifications()
		store = pgStore
	default:
		return fmt.Errorf("unknown storage %q (expected memory or postgres)", *storage)
	}
	server := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Addr: *address, Handler: httpapi.New(core.NewService(store), logger), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	failures := make(chan error, 1)
	go func() { failures <- server.ListenAndServe() }()
	logger.Info("server starting", "address", *address, "storage", *storage, "identity", "development X-User-ID header")
	select {
	case err := <-failures:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			return errors.Join(err, server.Close())
		}
		if err := <-failures; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
