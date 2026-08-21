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

func TestNewTargetFromConfigCreatesNamedCompatibleProvider(t *testing.T) {
	p, err := NewTargetFromConfig(config.Config{
		RequestTimeout:  time.Second,
		DeepSeekBaseURL: "https://api.deepseek.com",
		DeepSeekAPIKey:  "test-key",
	}, config.ProviderTarget{Name: "deepseek-primary", Kind: config.ProviderDeepSeek, Weight: 1})
	if err != nil {
		t.Fatalf("NewTargetFromConfig() error = %v", err)
	}
	compatible, ok := p.(*OpenAICompatibleProvider)
	if !ok {
		t.Fatalf("provider type = %T, want *OpenAICompatibleProvider", p)
	}
	if compatible.name != "deepseek-primary" {
		t.Fatalf("provider name = %q, want deepseek-primary", compatible.name)
	}
}

func TestNewTargetFromConfigRejectsUnknownKind(t *testing.T) {
	_, err := NewTargetFromConfig(config.Config{RequestTimeout: time.Second}, config.ProviderTarget{
		Name: "bad", Kind: "unknown", Weight: 1,
	})
	if err == nil {
		t.Fatal("NewTargetFromConfig() error = nil")
	}
}
