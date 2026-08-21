package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/theHerta27/ModelGate/internal/api"
	"github.com/theHerta27/ModelGate/internal/cache"
	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	providerconcurrency "github.com/theHerta27/ModelGate/internal/concurrency"
	"github.com/theHerta27/ModelGate/internal/config"
	"github.com/theHerta27/ModelGate/internal/governance"
	"github.com/theHerta27/ModelGate/internal/idempotency"
	"github.com/theHerta27/ModelGate/internal/metrics"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/redisstore"
	"github.com/theHerta27/ModelGate/internal/retry"
	"github.com/theHerta27/ModelGate/internal/routing"
	"github.com/theHerta27/ModelGate/internal/service"
	"github.com/theHerta27/ModelGate/internal/storage"
	"github.com/theHerta27/ModelGate/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ModelGate stopped", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)
	gin.SetMode(gin.ReleaseMode)
	telemetry := metrics.New()

	chatProvider, err := buildProviderRouter(cfg, telemetry)
	if err != nil {
		return err
	}
	chatService := service.NewChatService(chatProvider)
	gatewayOptions := service.GatewayOptions{
		ProviderName:      "router",
		RequestTimeout:    cfg.RequestTimeout,
		OperationTimeout:  cfg.StorageTimeout,
		RateLimitFailOpen: cfg.RateLimitFailOpen,
		CachePolicy:       cache.Policy{},
		Observer:          telemetry,
		ErrorSink: func(err error) {
			logger.Error("gateway dependency error", slog.Any("error", err))
		},
	}

	if cfg.RedisEnabled {
		redisClient := redisstore.New(
			cfg.RedisAddr,
			cfg.RedisPassword,
			cfg.RedisDB,
			cfg.RedisTimeout,
		)
		defer redisClient.Close()
		redisCtx, cancel := context.WithTimeout(context.Background(), cfg.RedisTimeout)
		if err := redisClient.Ping(redisCtx); err != nil {
			cancel()
			return fmt.Errorf("ping Redis: %w", err)
		}
		cancel()

		limiter, err := ratelimit.NewRedisLimiter(
			redisClient,
			cfg.RateLimitRPM,
			cfg.RateLimitBurst,
		)
		if err != nil {
			return fmt.Errorf("create rate limiter: %w", err)
		}
		idempotencyStore, err := idempotency.NewRedisStore(
			redisClient,
			cfg.IdempotencyLockTTL,
			cfg.IdempotencyTTL,
		)
		if err != nil {
			return fmt.Errorf("create idempotency store: %w", err)
		}
		responseCache, err := cache.NewRedisCache(redisClient, cfg.CacheTTL)
		if err != nil {
			return fmt.Errorf("create response cache: %w", err)
		}
		gatewayOptions.Limiter = limiter
		gatewayOptions.Idempotency = idempotencyStore
		gatewayOptions.Cache = responseCache
	}

	if cfg.PostgresEnabled {
		poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
		if err != nil {
			return fmt.Errorf("parse PostgreSQL config: %w", err)
		}
		poolConfig.MaxConns = cfg.PostgresMaxConns
		poolConfig.ConnConfig.ConnectTimeout = cfg.StorageTimeout
		storageCtx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
		pool, err := pgxpool.NewWithConfig(storageCtx, poolConfig)
		if err != nil {
			cancel()
			return fmt.Errorf("create PostgreSQL pool: %w", err)
		}
		defer pool.Close()
		if err := pool.Ping(storageCtx); err != nil {
			cancel()
			return fmt.Errorf("ping PostgreSQL: %w", err)
		}
		adapter := storage.NewPoolAdapter(pool)
		if err := storage.ApplyMigrations(storageCtx, adapter, migrations.FS); err != nil {
			cancel()
			return fmt.Errorf("apply PostgreSQL migrations: %w", err)
		}
		cancel()
		recorder, err := storage.NewUsageRepository(adapter)
		if err != nil {
			return fmt.Errorf("create usage repository: %w", err)
		}
		gatewayOptions.Recorder = recorder
	}

	gateway := service.NewGatewayService(chatService, gatewayOptions)
	handler := api.NewHandler(gateway)
	router := api.NewRouter(handler, api.RouterOptions{Metrics: telemetry, Logger: logger})

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	shutdownSignal, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info(
			"ModelGate listening",
			slog.String("address", cfg.HTTPAddr),
			slog.String("routing_strategy", cfg.RoutingStrategy),
			slog.Int("provider_count", len(cfg.Providers)),
		)
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-shutdownSignal.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGracePeriod)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	return nil
}

func buildProviderRouter(cfg config.Config, telemetry *metrics.Metrics) (provider.Provider, error) {
	targets := make([]routing.WeightedTarget, 0, len(cfg.Providers))
	for _, targetConfig := range cfg.Providers {
		rawProvider, err := provider.NewTargetFromConfig(cfg, targetConfig)
		if err != nil {
			return nil, fmt.Errorf("create provider %q: %w", targetConfig.Name, err)
		}
		providerName := targetConfig.Name
		retryPolicy, err := retry.New(retry.Options{
			MaxAttempts:    cfg.RetryMaxAttempts,
			AttemptTimeout: cfg.UpstreamAttemptTimeout,
			BaseBackoff:    cfg.RetryBaseBackoff,
			MaxBackoff:     cfg.RetryMaxBackoff,
			OnAttempt: func(attempt retry.Attempt) {
				telemetry.ObserveUpstreamAttempt(providerName, attempt)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create retry policy for %q: %w", providerName, err)
		}
		var breaker *circuitbreaker.Breaker
		breaker, err = circuitbreaker.New(circuitbreaker.Options{
			WindowSize:          cfg.CircuitBreakerWindowSize,
			MinimumRequests:     cfg.CircuitBreakerMinimumRequests,
			FailureRatio:        cfg.CircuitBreakerFailureRatio,
			OpenTimeout:         cfg.CircuitBreakerOpenTimeout,
			HalfOpenMaxRequests: cfg.CircuitBreakerHalfOpenMaxRequests,
			OnStateChange: func(circuitbreaker.State) {
				// A delayed callback reads the current state, so concurrent transitions
				// cannot leave the exported gauge at an older value.
				telemetry.SetCircuitState(providerName, breaker.State())
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create circuit breaker for %q: %w", providerName, err)
		}
		telemetry.SetCircuitState(providerName, circuitbreaker.StateClosed)
		semaphore, err := providerconcurrency.New(cfg.ProviderMaxConcurrency)
		if err != nil {
			return nil, fmt.Errorf("create concurrency limit for %q: %w", providerName, err)
		}
		governedProvider, err := governance.NewProvider(
			providerName,
			rawProvider,
			retryPolicy,
			breaker,
			semaphore,
		)
		if err != nil {
			return nil, fmt.Errorf("govern provider %q: %w", providerName, err)
		}
		targets = append(targets, routing.WeightedTarget{
			Target: governedProvider,
			Weight: targetConfig.Weight,
		})
	}

	router, err := routing.New(routing.Strategy(cfg.RoutingStrategy), targets)
	if err != nil {
		return nil, fmt.Errorf("create provider router: %w", err)
	}
	return router, nil
}

func newLogger(levelName string) *slog.Logger {
	level := slog.LevelInfo
	switch levelName {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
