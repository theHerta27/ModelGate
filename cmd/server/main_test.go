package main

import (
	"context"
	"testing"
	"time"

	"github.com/theHerta27/ModelGate/internal/config"
	"github.com/theHerta27/ModelGate/internal/metrics"
	"github.com/theHerta27/ModelGate/internal/provider"
)

func TestBuildProviderRouterUsesConfiguredWeights(t *testing.T) {
	cfg := config.Config{
		Providers: []config.ProviderTarget{
			{Name: "primary", Kind: config.ProviderMock, Weight: 2},
			{Name: "secondary", Kind: config.ProviderMock, Weight: 1},
		},
		RoutingStrategy:                   config.RoutingWeightedRoundRobin,
		RequestTimeout:                    time.Second,
		UpstreamAttemptTimeout:            time.Second,
		RetryMaxAttempts:                  1,
		RetryBaseBackoff:                  time.Millisecond,
		RetryMaxBackoff:                   time.Millisecond,
		CircuitBreakerWindowSize:          4,
		CircuitBreakerMinimumRequests:     2,
		CircuitBreakerFailureRatio:        0.5,
		CircuitBreakerOpenTimeout:         time.Second,
		CircuitBreakerHalfOpenMaxRequests: 1,
		ProviderMaxConcurrency:            2,
	}
	router, err := buildProviderRouter(cfg, metrics.New())
	if err != nil {
		t.Fatalf("buildProviderRouter() error = %v", err)
	}

	counts := make(map[string]int)
	request := &provider.ChatRequest{
		Model:    "mock-model",
		Messages: []provider.ChatMessage{{Role: "user", Content: "hello"}},
	}
	for range 6 {
		response, err := router.Chat(context.Background(), request)
		if err != nil {
			t.Fatalf("Chat() error = %v", err)
		}
		counts[response.Provider]++
	}
	if counts["primary"] != 4 || counts["secondary"] != 2 {
		t.Fatalf("weighted provider counts = %#v, want primary=4 secondary=2", counts)
	}
}
