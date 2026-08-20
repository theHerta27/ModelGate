package provider

import (
	"testing"
	"time"

	"github.com/theHerta27/ModelGate/internal/config"
)

func TestNewFromConfigCreatesMockProvider(t *testing.T) {
	p, err := NewFromConfig(config.Config{
		Provider:       config.ProviderMock,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewFromConfig() error = %v", err)
	}
	if _, ok := p.(*MockProvider); !ok {
		t.Fatalf("provider type = %T, want *MockProvider", p)
	}
}

func TestNewFromConfigCreatesDeepSeekProvider(t *testing.T) {
	p, err := NewFromConfig(config.Config{
		Provider:        config.ProviderDeepSeek,
		RequestTimeout:  time.Second,
		DeepSeekBaseURL: "https://api.deepseek.com",
		DeepSeekAPIKey:  "test-key",
	})
	if err != nil {
		t.Fatalf("NewFromConfig() error = %v", err)
	}
	if _, ok := p.(*DeepSeekProvider); !ok {
		t.Fatalf("provider type = %T, want *DeepSeekProvider", p)
	}
}
