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
	} {
		t.Setenv(name, "")
	}
}
