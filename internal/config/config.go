package config

import (
	"fmt"
	"os"
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
	}

	requestTimeout, err := time.ParseDuration(envOrDefault("REQUEST_TIMEOUT", "30s"))
	if err != nil || requestTimeout <= 0 {
		return Config{}, fmt.Errorf("REQUEST_TIMEOUT must be a positive Go duration")
	}
	cfg.RequestTimeout = requestTimeout

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

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
