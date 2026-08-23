package service

import (
	"context"
	"testing"
	"time"

	"github.com/theHerta27/ModelGate/internal/cache"
	"github.com/theHerta27/ModelGate/internal/provider"
)

var benchmarkGatewayResult *ChatResult

func BenchmarkGatewayMock(b *testing.B) {
	gateway, request, metadata := newBenchmarkGateway()
	ctx := context.Background()
	var err error
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkGatewayResult, err = gateway.Chat(ctx, request, metadata)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGatewayMockParallel(b *testing.B) {
	gateway, request, metadata := newBenchmarkGateway()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			result, err := gateway.Chat(ctx, request, metadata)
			if err != nil {
				b.Error(err)
				return
			}
			_ = result
		}
	})
}

func newBenchmarkGateway() (*GatewayService, *provider.ChatRequest, RequestMetadata) {
	gateway := NewGatewayService(
		NewChatService(provider.NewMockProvider()),
		GatewayOptions{
			ProviderName:     "mock",
			RequestTimeout:   30 * time.Second,
			OperationTimeout: 2 * time.Second,
			CachePolicy:      cache.Policy{},
		},
	)
	request := &provider.ChatRequest{
		Model:    "benchmark-model",
		Messages: []provider.ChatMessage{{Role: "user", Content: "benchmark"}},
	}
	metadata := RequestMetadata{
		ClientIdentity: "benchmark-client",
		RequestID:      "00000000-0000-4000-8000-000000000001",
	}
	return gateway, request, metadata
}
