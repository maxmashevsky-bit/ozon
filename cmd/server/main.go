package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"example.com/ozon/internal/auth"
	"example.com/ozon/internal/config"
	"example.com/ozon/internal/core"
	"example.com/ozon/internal/events"
	"example.com/ozon/internal/httpapi"
	"example.com/ozon/internal/store/memory"
	"example.com/ozon/internal/store/postgres"
	"example.com/ozon/internal/telemetry"
	"example.com/ozon/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("server stopped", "reason", err.Error())
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	storage := flag.String("storage", "memory", "memory or postgres")
	address := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	migrate := flag.Bool("migrate", false, "apply migrations before serving (normally use the migrator)")
	flag.Parse()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var identity *auth.Authenticator
	if cfg.AuthMode == "dev" {
		identity = auth.DevHeader()
		logger.Warn("development identity mode enabled")
	} else {
		key, err := auth.ReadKey(cfg.SecretFile)
		if err != nil {
			return err
		}
		identity, err = auth.JWT(key)
		if err != nil {
			return err
		}
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	eventConfig := events.Config{BufferSize: cfg.BufferSize, MaxSubscribers: cfg.Subscribers}
	metrics := &telemetry.Metrics{}
	var store core.Store
	var ready func(context.Context) error
	var closeSubscriptions func()
	switch *storage {
	case "memory":
		s := memory.New(eventConfig)
		store = s
		metrics.Events = s.EventMetrics
		closeSubscriptions = s.CloseSubscriptions
	case "postgres":
		dsn, err := config.DatabaseURL()
		if err != nil {
			return err
		}
		startup, cancel := context.WithTimeout(lifetime, 30*time.Second)
		defer cancel()
		if *migrate {
			if migrations.Up(startup, dsn) != nil {
				return errors.New("migration failed")
			}
		}
		poolConfig, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			return errors.New("invalid DATABASE_URL")
		}
		poolConfig.MaxConns = cfg.PoolSize
		poolConfig.ConnConfig.Tracer = metrics
		poolConfig.ConnConfig.RuntimeParams["application_name"] = "ozon-api"
		poolConfig.ConnConfig.ConnectTimeout = 5 * time.Second
		pool, err := pgxpool.NewWithConfig(startup, poolConfig)
		if err != nil {
			return errors.New("cannot open database pool")
		}
		defer pool.Close()
		s := postgres.New(pool, eventConfig)
		if s.Ready(startup) != nil {
			return errors.New("database or schema unavailable; apply migrations first")
		}
		stopNotifications, err := s.StartNotifications(lifetime, logger)
		if err != nil {
			return errors.New("cannot start comment listener")
		}
		defer stopNotifications()
		store = s
		metrics.Pool = pool
		metrics.Events = s.EventMetrics
		ready = s.Ready
		closeSubscriptions = s.CloseSubscriptions
	default:
		return errors.New("storage must be memory or postgres")
	}
	var draining atomic.Bool
	handler := httpapi.New(core.NewService(store), logger, httpapi.Options{Auth: identity, Metrics: metrics, Ready: ready, Draining: draining.Load, MaxOperations: cfg.Operations, MaxSubscriptions: cfg.Subscribers, RequestTimeout: cfg.Timeout, StreamLifetime: cfg.StreamLifetime, Heartbeat: cfg.Heartbeat, WriteTimeout: cfg.WriteTimeout, MaxBodyBytes: cfg.BodyBytes, Rate: cfg.Rate, Burst: cfg.Burst, TrustedProxies: cfg.Trusted, Depth: cfg.Depth, Fields: cfg.Fields, Cost: cfg.Cost})
	server := &http.Server{Addr: *address, Handler: handler, BaseContext: func(net.Listener) context.Context { return lifetime }, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: cfg.Timeout + cfg.WriteTimeout, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	failures := make(chan error, 1)
	go func() { failures <- server.ListenAndServe() }()
	logger.Info("server starting", "address", *address, "storage", *storage, "auth", cfg.AuthMode, "pool_max", cfg.PoolSize)
	select {
	case err := <-failures:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.New("HTTP listener failed")
	case <-signalCtx.Done():
		draining.Store(true)
		closeSubscriptions()
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.Shutdown)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			cancelLifetime()
			if server.Close() != nil {
				return errors.New("forced HTTP shutdown failed")
			}
		}
		cancelLifetime()
		if err := <-failures; !errors.Is(err, http.ErrServerClosed) {
			return errors.New("HTTP shutdown failed")
		}
		logger.Info("server stopped cleanly")
		return nil
	}
}
