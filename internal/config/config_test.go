package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsToMock(t *testing.T) {
	clearEnvironment(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Provider != ProviderMock {
		t.Fatalf("Provider = %q, want %q", cfg.Provider, ProviderMock)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8080")
	}
	if cfg.RequestTimeout != 30*time.Second {
		t.Fatalf("RequestTimeout = %s, want 30s", cfg.RequestTimeout)
	}
	if cfg.RedisEnabled || cfg.PostgresEnabled {
		t.Fatalf("external dependencies enabled by default: %#v", cfg)
	}
	if cfg.RateLimitRPM != 60 || cfg.RateLimitBurst != 60 || !cfg.RateLimitFailOpen {
		t.Fatalf("rate limit defaults = %d/%d/%v", cfg.RateLimitRPM, cfg.RateLimitBurst, cfg.RateLimitFailOpen)
	}
}

func TestLoadRequiresDeepSeekAPIKey(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("MODEL_PROVIDER", ProviderDeepSeek)

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DEEPSEEK_API_KEY") {
		t.Fatalf("Load() error = %v, want missing DEEPSEEK_API_KEY", err)
	}
}

func TestLoadRejectsInvalidTimeout(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("REQUEST_TIMEOUT", "forever")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "REQUEST_TIMEOUT") {
		t.Fatalf("Load() error = %v, want invalid REQUEST_TIMEOUT", err)
	}
}

func TestLoadParsesRedisConfiguration(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("REDIS_ENABLED", "true")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("RATE_LIMIT_REQUESTS_PER_MINUTE", "120")
	t.Setenv("RATE_LIMIT_BURST", "10")
	t.Setenv("RATE_LIMIT_FAIL_OPEN", "false")
	t.Setenv("IDEMPOTENCY_TTL", "12h")
	t.Setenv("CACHE_TTL", "90s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.RedisEnabled || cfg.RedisDB != 2 || cfg.RateLimitRPM != 120 || cfg.RateLimitBurst != 10 {
		t.Fatalf("Redis config = %#v", cfg)
	}
	if cfg.RateLimitFailOpen || cfg.IdempotencyTTL != 12*time.Hour || cfg.CacheTTL != 90*time.Second {
		t.Fatalf("Redis policies = %#v", cfg)
	}
}

func TestLoadBuildsEscapedPostgresDSN(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("POSTGRES_ENABLED", "true")
	t.Setenv("POSTGRES_USER", "modelgate")
	t.Setenv("POSTGRES_PASSWORD", "p@ss/word")
	t.Setenv("POSTGRES_DB", "modelgate")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.PostgresEnabled || !strings.Contains(cfg.PostgresDSN, "modelgate:p%40ss%2Fword@") {
		t.Fatalf("PostgresDSN was not safely escaped")
	}
}

func TestLoadRequiresPostgresIdentityWhenEnabled(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("POSTGRES_ENABLED", "true")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "POSTGRES_USER") {
		t.Fatalf("Load() error = %v, want PostgreSQL identity error", err)
	}
}

func TestLoadRejectsInvalidV2Configuration(t *testing.T) {
	tests := []struct {
		name  string
		env   string
		value string
	}{
		{name: "Redis enabled", env: "REDIS_ENABLED", value: "sometimes"},
		{name: "Redis DB", env: "REDIS_DB", value: "-1"},
		{name: "rate limit", env: "RATE_LIMIT_REQUESTS_PER_MINUTE", value: "0"},
		{name: "cache TTL", env: "CACHE_TTL", value: "forever"},
		{name: "Postgres max conns", env: "POSTGRES_MAX_CONNS", value: "101"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, err := Load(); err == nil {
				t.Fatalf("Load() error = nil for %s=%s", test.env, test.value)
			}
		})
	}
}

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"HTTP_ADDR",
		"MODEL_PROVIDER",
		"REQUEST_TIMEOUT",
		"DEEPSEEK_BASE_URL",
		"DEEPSEEK_API_KEY",
		"OPENAI_COMPATIBLE_BASE_URL",
		"OPENAI_API_KEY",
		"REDIS_ENABLED",
		"REDIS_ADDR",
		"REDIS_PASSWORD",
		"REDIS_DB",
		"REDIS_TIMEOUT",
		"RATE_LIMIT_REQUESTS_PER_MINUTE",
		"RATE_LIMIT_BURST",
		"RATE_LIMIT_FAIL_OPEN",
		"IDEMPOTENCY_TTL",
		"IDEMPOTENCY_LOCK_TTL",
		"CACHE_TTL",
		"POSTGRES_ENABLED",
		"POSTGRES_DSN",
		"POSTGRES_HOST",
		"POSTGRES_PORT",
		"POSTGRES_USER",
		"POSTGRES_PASSWORD",
		"POSTGRES_DB",
		"POSTGRES_SSLMODE",
		"POSTGRES_MAX_CONNS",
		"STORAGE_TIMEOUT",
	} {
		t.Setenv(name, "")
	}
}
