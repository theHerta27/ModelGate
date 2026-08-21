package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ProviderMock             = "mock"
	ProviderDeepSeek         = "deepseek"
	ProviderOpenAICompatible = "openai-compatible"

	RoutingRoundRobin         = "round_robin"
	RoutingWeightedRoundRobin = "weighted_round_robin"
)

type ProviderTarget struct {
	Name   string
	Kind   string
	Weight int
}

type Config struct {
	HTTPAddr                          string
	Provider                          string
	Providers                         []ProviderTarget
	RoutingStrategy                   string
	RequestTimeout                    time.Duration
	UpstreamAttemptTimeout            time.Duration
	RetryMaxAttempts                  int
	RetryBaseBackoff                  time.Duration
	RetryMaxBackoff                   time.Duration
	CircuitBreakerWindowSize          int
	CircuitBreakerMinimumRequests     int
	CircuitBreakerFailureRatio        float64
	CircuitBreakerOpenTimeout         time.Duration
	CircuitBreakerHalfOpenMaxRequests int
	ProviderMaxConcurrency            int
	LogLevel                          string
	DeepSeekBaseURL                   string
	DeepSeekAPIKey                    string
	OpenAIBaseURL                     string
	OpenAIAPIKey                      string
	ShutdownGracePeriod               time.Duration

	RedisEnabled       bool
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	RedisTimeout       time.Duration
	RateLimitRPM       int
	RateLimitBurst     int
	RateLimitFailOpen  bool
	IdempotencyTTL     time.Duration
	IdempotencyLockTTL time.Duration
	CacheTTL           time.Duration

	PostgresEnabled  bool
	PostgresDSN      string
	PostgresMaxConns int32
	StorageTimeout   time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:            envOrDefault("HTTP_ADDR", ":8080"),
		Provider:            strings.ToLower(envOrDefault("MODEL_PROVIDER", ProviderMock)),
		RoutingStrategy:     strings.ToLower(envOrDefault("ROUTING_STRATEGY", RoutingRoundRobin)),
		LogLevel:            strings.ToLower(envOrDefault("LOG_LEVEL", "info")),
		DeepSeekBaseURL:     envOrDefault("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
		DeepSeekAPIKey:      strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY")),
		OpenAIBaseURL:       envOrDefault("OPENAI_COMPATIBLE_BASE_URL", "https://api.openai.com/v1"),
		OpenAIAPIKey:        strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		ShutdownGracePeriod: 10 * time.Second,
		RedisAddr:           envOrDefault("REDIS_ADDR", "127.0.0.1:6379"),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
	}

	var err error
	if cfg.RequestTimeout, err = positiveDuration("REQUEST_TIMEOUT", "30s"); err != nil {
		return Config{}, err
	}
	if cfg.UpstreamAttemptTimeout, err = positiveDuration("UPSTREAM_ATTEMPT_TIMEOUT", "10s"); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxAttempts, err = positiveInt("UPSTREAM_MAX_ATTEMPTS", 3); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxAttempts > 10 {
		return Config{}, fmt.Errorf("UPSTREAM_MAX_ATTEMPTS must not exceed 10")
	}
	if cfg.RetryBaseBackoff, err = positiveDuration("RETRY_BASE_BACKOFF", "100ms"); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxBackoff, err = positiveDuration("RETRY_MAX_BACKOFF", "2s"); err != nil {
		return Config{}, err
	}
	if cfg.RetryMaxBackoff < cfg.RetryBaseBackoff {
		return Config{}, fmt.Errorf("RETRY_MAX_BACKOFF must not be below RETRY_BASE_BACKOFF")
	}
	if cfg.UpstreamAttemptTimeout > cfg.RequestTimeout {
		return Config{}, fmt.Errorf("UPSTREAM_ATTEMPT_TIMEOUT must not exceed REQUEST_TIMEOUT")
	}
	if cfg.CircuitBreakerWindowSize, err = positiveInt("CIRCUIT_BREAKER_WINDOW_SIZE", 20); err != nil {
		return Config{}, err
	}
	if cfg.CircuitBreakerMinimumRequests, err = positiveInt("CIRCUIT_BREAKER_MINIMUM_REQUESTS", 10); err != nil {
		return Config{}, err
	}
	if cfg.CircuitBreakerMinimumRequests > cfg.CircuitBreakerWindowSize {
		return Config{}, fmt.Errorf("CIRCUIT_BREAKER_MINIMUM_REQUESTS must not exceed CIRCUIT_BREAKER_WINDOW_SIZE")
	}
	if cfg.CircuitBreakerFailureRatio, err = positiveRatio("CIRCUIT_BREAKER_FAILURE_RATIO", 0.5); err != nil {
		return Config{}, err
	}
	if cfg.CircuitBreakerOpenTimeout, err = positiveDuration("CIRCUIT_BREAKER_OPEN_TIMEOUT", "30s"); err != nil {
		return Config{}, err
	}
	if cfg.CircuitBreakerHalfOpenMaxRequests, err = positiveInt("CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS", 1); err != nil {
		return Config{}, err
	}
	if cfg.ProviderMaxConcurrency, err = positiveInt("PROVIDER_MAX_CONCURRENCY", 50); err != nil {
		return Config{}, err
	}
	if cfg.ProviderMaxConcurrency > 10000 {
		return Config{}, fmt.Errorf("PROVIDER_MAX_CONCURRENCY must not exceed 10000")
	}
	if cfg.RedisEnabled, err = boolean("REDIS_ENABLED", false); err != nil {
		return Config{}, err
	}
	if cfg.RedisDB, err = nonNegativeInt("REDIS_DB", 0); err != nil {
		return Config{}, err
	}
	if cfg.RedisTimeout, err = positiveDuration("REDIS_TIMEOUT", "2s"); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitRPM, err = positiveInt("RATE_LIMIT_REQUESTS_PER_MINUTE", 60); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitBurst, err = positiveInt("RATE_LIMIT_BURST", cfg.RateLimitRPM); err != nil {
		return Config{}, err
	}
	if cfg.RateLimitFailOpen, err = boolean("RATE_LIMIT_FAIL_OPEN", true); err != nil {
		return Config{}, err
	}
	if cfg.IdempotencyTTL, err = positiveDuration("IDEMPOTENCY_TTL", "24h"); err != nil {
		return Config{}, err
	}
	if cfg.IdempotencyLockTTL, err = positiveDuration("IDEMPOTENCY_LOCK_TTL", "30s"); err != nil {
		return Config{}, err
	}
	if cfg.CacheTTL, err = positiveDuration("CACHE_TTL", "5m"); err != nil {
		return Config{}, err
	}
	if cfg.PostgresEnabled, err = boolean("POSTGRES_ENABLED", false); err != nil {
		return Config{}, err
	}
	maxConns, err := positiveInt("POSTGRES_MAX_CONNS", 10)
	if err != nil {
		return Config{}, err
	}
	if maxConns > 100 {
		return Config{}, fmt.Errorf("POSTGRES_MAX_CONNS must not exceed 100")
	}
	cfg.PostgresMaxConns = int32(maxConns)
	if cfg.StorageTimeout, err = positiveDuration("STORAGE_TIMEOUT", "2s"); err != nil {
		return Config{}, err
	}
	if cfg.PostgresEnabled {
		cfg.PostgresDSN, err = postgresDSN()
		if err != nil {
			return Config{}, err
		}
	}

	if cfg.RoutingStrategy != RoutingRoundRobin && cfg.RoutingStrategy != RoutingWeightedRoundRobin {
		return Config{}, fmt.Errorf("ROUTING_STRATEGY must be %q or %q", RoutingRoundRobin, RoutingWeightedRoundRobin)
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return Config{}, fmt.Errorf("LOG_LEVEL must be debug, info, warn, or error")
	}
	cfg.Providers, err = providerTargets(cfg.Provider)
	if err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(os.Getenv("MODEL_PROVIDERS")) != "" {
		cfg.Provider = cfg.Providers[0].Kind
	}
	for _, target := range cfg.Providers {
		switch target.Kind {
		case ProviderMock:
		case ProviderDeepSeek:
			if cfg.DeepSeekAPIKey == "" {
				return Config{}, fmt.Errorf("DEEPSEEK_API_KEY is required for deepseek provider %q", target.Name)
			}
		case ProviderOpenAICompatible:
			if cfg.OpenAIAPIKey == "" {
				return Config{}, fmt.Errorf("OPENAI_API_KEY is required for openai-compatible provider %q", target.Name)
			}
		}
	}

	return cfg, nil
}

func providerTargets(legacyProvider string) ([]ProviderTarget, error) {
	raw := strings.TrimSpace(os.Getenv("MODEL_PROVIDERS"))
	if raw == "" {
		kind, err := providerKind(legacyProvider)
		if err != nil {
			return nil, fmt.Errorf("MODEL_PROVIDER %w", err)
		}
		return []ProviderTarget{{Name: kind, Kind: kind, Weight: 1}}, nil
	}

	parts := strings.Split(raw, ",")
	targets := make([]ProviderTarget, 0, len(parts))
	names := make(map[string]struct{}, len(parts))
	for index, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 3 {
			return nil, fmt.Errorf("MODEL_PROVIDERS entry %d must use name:type:weight", index+1)
		}
		name := strings.TrimSpace(fields[0])
		if !validProviderName(name) {
			return nil, fmt.Errorf("MODEL_PROVIDERS entry %d has an invalid name", index+1)
		}
		if _, exists := names[name]; exists {
			return nil, fmt.Errorf("MODEL_PROVIDERS provider name %q is duplicated", name)
		}
		kind, err := providerKind(strings.TrimSpace(fields[1]))
		if err != nil {
			return nil, fmt.Errorf("MODEL_PROVIDERS entry %d %w", index+1, err)
		}
		weight, err := strconv.Atoi(strings.TrimSpace(fields[2]))
		if err != nil || weight <= 0 || weight > 100 {
			return nil, fmt.Errorf("MODEL_PROVIDERS entry %d weight must be between 1 and 100", index+1)
		}
		names[name] = struct{}{}
		targets = append(targets, ProviderTarget{Name: name, Kind: kind, Weight: weight})
	}
	return targets, nil
}

func providerKind(value string) (string, error) {
	kind := strings.ToLower(strings.TrimSpace(value))
	switch kind {
	case ProviderMock, ProviderDeepSeek, ProviderOpenAICompatible:
		return kind, nil
	default:
		return "", fmt.Errorf("type must be one of %q, %q, or %q", ProviderMock, ProviderDeepSeek, ProviderOpenAICompatible)
	}
}

func validProviderName(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("._-", character) {
			continue
		}
		return false
	}
	return true
}

func postgresDSN() (string, error) {
	if dsn := strings.TrimSpace(os.Getenv("POSTGRES_DSN")); dsn != "" {
		return dsn, nil
	}

	host := envOrDefault("POSTGRES_HOST", "127.0.0.1")
	port := envOrDefault("POSTGRES_PORT", "5432")
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return "", fmt.Errorf("POSTGRES_PORT must be a valid TCP port")
	}
	user := strings.TrimSpace(os.Getenv("POSTGRES_USER"))
	database := strings.TrimSpace(os.Getenv("POSTGRES_DB"))
	if user == "" || database == "" {
		return "", fmt.Errorf("POSTGRES_USER and POSTGRES_DB are required when POSTGRES_ENABLED=true")
	}

	credentials := url.User(user)
	if password := os.Getenv("POSTGRES_PASSWORD"); password != "" {
		credentials = url.UserPassword(user, password)
	}
	connectionURL := &url.URL{
		Scheme: "postgres",
		User:   credentials,
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + database,
	}
	query := connectionURL.Query()
	query.Set("sslmode", envOrDefault("POSTGRES_SSLMODE", "disable"))
	connectionURL.RawQuery = query.Encode()
	return connectionURL.String(), nil
}

func positiveDuration(name, fallback string) (time.Duration, error) {
	value, err := time.ParseDuration(envOrDefault(name, fallback))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return value, nil
}

func boolean(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return value, nil
}

func positiveInt(name string, fallback int) (int, error) {
	raw := envOrDefault(name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func positiveRatio(name string, fallback float64) (float64, error) {
	raw := envOrDefault(name, strconv.FormatFloat(fallback, 'f', -1, 64))
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value <= 0 || value > 1 {
		return 0, fmt.Errorf("%s must be a number in (0, 1]", name)
	}
	return value, nil
}

func nonNegativeInt(name string, fallback int) (int, error) {
	raw := envOrDefault(name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return value, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
