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
)

type Config struct {
	HTTPAddr            string
	Provider            string
	RequestTimeout      time.Duration
	DeepSeekBaseURL     string
	DeepSeekAPIKey      string
	OpenAIBaseURL       string
	OpenAIAPIKey        string
	ShutdownGracePeriod time.Duration

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

	switch cfg.Provider {
	case ProviderMock:
	case ProviderDeepSeek:
		if cfg.DeepSeekAPIKey == "" {
			return Config{}, fmt.Errorf("DEEPSEEK_API_KEY is required when MODEL_PROVIDER=deepseek")
		}
	case ProviderOpenAICompatible:
		if cfg.OpenAIAPIKey == "" {
			return Config{}, fmt.Errorf("OPENAI_API_KEY is required when MODEL_PROVIDER=openai-compatible")
		}
	default:
		return Config{}, fmt.Errorf(
			"MODEL_PROVIDER must be one of %q, %q, or %q",
			ProviderMock,
			ProviderDeepSeek,
			ProviderOpenAICompatible,
		)
	}

	return cfg, nil
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
