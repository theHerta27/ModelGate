package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/theHerta27/ModelGate/internal/api"
	"github.com/theHerta27/ModelGate/internal/cache"
	"github.com/theHerta27/ModelGate/internal/config"
	"github.com/theHerta27/ModelGate/internal/idempotency"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/redisstore"
	"github.com/theHerta27/ModelGate/internal/service"
	"github.com/theHerta27/ModelGate/internal/storage"
	"github.com/theHerta27/ModelGate/migrations"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	chatProvider, err := provider.NewFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("create provider: %w", err)
	}
	chatService := service.NewChatService(chatProvider)
	gatewayOptions := service.GatewayOptions{
		ProviderName:      cfg.Provider,
		OperationTimeout:  cfg.StorageTimeout,
		RateLimitFailOpen: cfg.RateLimitFailOpen,
		CachePolicy:       cache.Policy{},
		ErrorSink: func(err error) {
			log.Printf("gateway dependency error: %v", err)
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
	router := api.NewRouter(handler)

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
		log.Printf("ModelGate listening on %s with provider %s", cfg.HTTPAddr, cfg.Provider)
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
