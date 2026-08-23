//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/theHerta27/ModelGate/internal/api"
	"github.com/theHerta27/ModelGate/internal/cache"
	"github.com/theHerta27/ModelGate/internal/idempotency"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/redisstore"
	"github.com/theHerta27/ModelGate/internal/service"
	"github.com/theHerta27/ModelGate/internal/storage"
	"github.com/theHerta27/ModelGate/migrations"
)

const integrationTimeout = 10 * time.Second

func TestRedisPoliciesWithRealServer(t *testing.T) {
	client := newRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()

	identity := uniqueName("redis")
	limiter, err := ratelimit.NewRedisLimiter(client, 60, 1)
	if err != nil {
		t.Fatalf("create limiter: %v", err)
	}
	first, err := limiter.Allow(ctx, identity)
	if err != nil {
		t.Fatalf("first limiter decision: %v", err)
	}
	second, err := limiter.Allow(ctx, identity)
	if err != nil {
		t.Fatalf("second limiter decision: %v", err)
	}
	if !first.Allowed || second.Allowed {
		t.Fatalf("expected allow then reject, got first=%+v second=%+v", first, second)
	}

	idempotencyStore, err := idempotency.NewRedisStore(client, time.Minute, time.Minute)
	if err != nil {
		t.Fatalf("create idempotency store: %v", err)
	}
	scopedKey := idempotency.ScopedKey(identity, "integration-key")
	begin, err := idempotencyStore.Begin(ctx, scopedKey, "fingerprint")
	if err != nil {
		t.Fatalf("begin idempotency request: %v", err)
	}
	if begin.Status != idempotency.StatusAcquired {
		t.Fatalf("expected acquired status, got %q", begin.Status)
	}
	wantPayload := []byte(`{"ok":true}`)
	if err := idempotencyStore.Complete(ctx, scopedKey, "fingerprint", wantPayload); err != nil {
		t.Fatalf("complete idempotency request: %v", err)
	}
	replay, err := idempotencyStore.Begin(ctx, scopedKey, "fingerprint")
	if err != nil {
		t.Fatalf("replay idempotency request: %v", err)
	}
	if replay.Status != idempotency.StatusCompleted || !bytes.Equal(replay.Response, wantPayload) {
		t.Fatalf("unexpected replay: status=%q response=%q", replay.Status, replay.Response)
	}

	responseCache, err := cache.NewRedisCache(client, time.Minute)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	cacheKey := uniqueName("cache")
	if err := responseCache.Set(ctx, cacheKey, wantPayload); err != nil {
		t.Fatalf("set cache: %v", err)
	}
	cached, found, err := responseCache.Get(ctx, cacheKey)
	if err != nil {
		t.Fatalf("get cache: %v", err)
	}
	if !found || !bytes.Equal(cached, wantPayload) {
		t.Fatalf("unexpected cache result: found=%t response=%q", found, cached)
	}
}

func TestPostgresMigrationsAndUsageWithRealServer(t *testing.T) {
	pool := newPostgresPool(t)
	applyMigrations(t, pool)

	requestID, err := service.NewRequestID()
	if err != nil {
		t.Fatalf("generate request id: %v", err)
	}
	providerName := uniqueName("postgres")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
		defer cancel()
		_, _ = pool.Exec(
			ctx,
			"DELETE FROM providers WHERE name = $1 AND NOT EXISTS (SELECT 1 FROM requests WHERE provider = $1)",
			providerName,
		)
	})
	cleanupRequest(t, pool, requestID)

	repository, err := storage.NewUsageRepository(storage.NewPoolAdapter(pool))
	if err != nil {
		t.Fatalf("create usage repository: %v", err)
	}
	err = repository.Record(context.Background(), storage.RequestRecord{
		RequestID:    requestID,
		Provider:     providerName,
		Model:        "integration-model",
		Status:       storage.StatusSucceeded,
		InputTokens:  3,
		OutputTokens: 5,
		TotalTokens:  8,
		Latency:      12 * time.Millisecond,
		CacheStatus:  cache.StatusMiss,
	})
	if err != nil {
		t.Fatalf("record usage: %v", err)
	}

	var status string
	var totalTokens int
	err = pool.QueryRow(context.Background(), `
SELECT requests.status, usage_records.total_tokens
FROM requests
JOIN usage_records ON usage_records.request_id = requests.id
WHERE requests.id = $1`, requestID).Scan(&status, &totalTokens)
	if err != nil {
		t.Fatalf("query persisted usage: %v", err)
	}
	if status != storage.StatusSucceeded || totalTokens != 8 {
		t.Fatalf("unexpected persisted usage: status=%q total_tokens=%d", status, totalTokens)
	}
}

func TestGatewayAPIWithRealRedisAndPostgres(t *testing.T) {
	gin.SetMode(gin.TestMode)
	redisClient := newRedisClient(t)
	pool := newPostgresPool(t)
	applyMigrations(t, pool)

	limiter, err := ratelimit.NewRedisLimiter(redisClient, 6000, 100)
	if err != nil {
		t.Fatalf("create limiter: %v", err)
	}
	idempotencyStore, err := idempotency.NewRedisStore(
		redisClient,
		time.Minute,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("create idempotency store: %v", err)
	}
	responseCache, err := cache.NewRedisCache(redisClient, time.Minute)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	recorder, err := storage.NewUsageRepository(storage.NewPoolAdapter(pool))
	if err != nil {
		t.Fatalf("create usage repository: %v", err)
	}

	gateway := service.NewGatewayService(
		service.NewChatService(provider.NewMockProvider()),
		service.GatewayOptions{
			Limiter:          limiter,
			Idempotency:      idempotencyStore,
			Cache:            responseCache,
			CachePolicy:      cache.Policy{},
			Recorder:         recorder,
			ProviderName:     "mock",
			RequestTimeout:   5 * time.Second,
			OperationTimeout: 2 * time.Second,
			ErrorSink: func(err error) {
				t.Errorf("gateway dependency error: %v", err)
			},
		},
	)
	router := api.NewRouter(api.NewHandler(gateway))

	body := []byte(`{
		"model":"integration-model",
		"messages":[{"role":"user","content":"real dependencies"}],
		"temperature":0
	}`)
	first := performChat(t, router, body, "integration-idempotency")
	second := performChat(t, router, body, "integration-idempotency")
	third := performChat(t, router, body, "")

	if first.Header().Get("X-ModelGate-Cache") != cache.StatusMiss {
		t.Fatalf("expected first cache MISS, got %q", first.Header().Get("X-ModelGate-Cache"))
	}
	if second.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("expected idempotency replay, headers=%v", second.Header())
	}
	if third.Header().Get("X-ModelGate-Cache") != cache.StatusHit {
		t.Fatalf("expected third cache HIT, got %q", third.Header().Get("X-ModelGate-Cache"))
	}
	if first.Body.String() != second.Body.String() || first.Body.String() != third.Body.String() {
		t.Fatalf("replayed and cached responses must match the original response")
	}

	requestIDs := []string{
		first.Header().Get("X-Request-ID"),
		second.Header().Get("X-Request-ID"),
		third.Header().Get("X-Request-ID"),
	}
	for _, requestID := range requestIDs {
		if requestID == "" {
			t.Fatal("gateway response is missing X-Request-ID")
		}
		cleanupRequest(t, pool, requestID)
	}

	rows, err := pool.Query(
		context.Background(),
		"SELECT status FROM requests WHERE id IN ($1, $2, $3)",
		requestIDs[0],
		requestIDs[1],
		requestIDs[2],
	)
	if err != nil {
		t.Fatalf("query gateway request records: %v", err)
	}
	defer rows.Close()
	var statuses []string
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			t.Fatalf("scan gateway request status: %v", err)
		}
		statuses = append(statuses, status)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate gateway request statuses: %v", err)
	}
	sort.Strings(statuses)
	wantStatuses := []string{
		storage.StatusCacheHit,
		storage.StatusIdempotencyReplay,
		storage.StatusSucceeded,
	}
	sort.Strings(wantStatuses)
	if fmt.Sprint(statuses) != fmt.Sprint(wantStatuses) {
		t.Fatalf("unexpected gateway request statuses: got=%v want=%v", statuses, wantStatuses)
	}
}

func performChat(
	t *testing.T,
	handler http.Handler,
	body []byte,
	idempotencyKey string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		payload, _ := io.ReadAll(response.Result().Body)
		t.Fatalf("gateway returned %d: %s", response.Code, payload)
	}
	return response
}

func newRedisClient(t *testing.T) *redisstore.Client {
	t.Helper()
	requireIntegration(t)
	database := 15
	if value := os.Getenv("MODELGATE_INTEGRATION_REDIS_DB"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			t.Fatalf("parse MODELGATE_INTEGRATION_REDIS_DB: %v", err)
		}
		database = parsed
	}
	client := redisstore.New(
		envOrDefault("MODELGATE_INTEGRATION_REDIS_ADDR", "127.0.0.1:6379"),
		os.Getenv("MODELGATE_INTEGRATION_REDIS_PASSWORD"),
		database,
		2*time.Second,
	)
	t.Cleanup(func() {
		_ = client.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping real Redis: %v", err)
	}
	return client
}

func newPostgresPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	requireIntegration(t)
	dsn := os.Getenv("MODELGATE_INTEGRATION_POSTGRES_DSN")
	if dsn == "" {
		t.Fatal("MODELGATE_INTEGRATION_POSTGRES_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create real PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping real PostgreSQL: %v", err)
	}
	return pool
}

func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	if err := storage.ApplyMigrations(ctx, storage.NewPoolAdapter(pool), migrations.FS); err != nil {
		t.Fatalf("apply real PostgreSQL migrations: %v", err)
	}
}

func cleanupRequest(t *testing.T, pool *pgxpool.Pool, requestID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
		defer cancel()
		if _, err := pool.Exec(ctx, "DELETE FROM requests WHERE id = $1", requestID); err != nil {
			t.Errorf("clean request %s: %v", requestID, err)
		}
	})
}

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("MODELGATE_INTEGRATION") != "1" {
		t.Skip("set MODELGATE_INTEGRATION=1 to run tests against real services")
	}
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("modelgate-%s-%d", prefix, time.Now().UnixNano())
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
