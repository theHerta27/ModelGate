package routing

import (
	"context"
	"testing"

	"github.com/theHerta27/ModelGate/internal/provider"
)

var benchmarkRouterResponse *provider.ChatResponse

func BenchmarkRouter(b *testing.B) {
	request := &provider.ChatRequest{
		Model:    "benchmark-model",
		Messages: []provider.ChatMessage{{Role: "user", Content: "benchmark"}},
	}
	for _, benchmark := range []struct {
		name     string
		strategy Strategy
		weights  []int
	}{
		{name: "RoundRobin", strategy: StrategyRoundRobin, weights: []int{1, 1, 1}},
		{name: "WeightedRoundRobin", strategy: StrategyWeightedRoundRobin, weights: []int{5, 3, 2}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			router := newBenchmarkRouter(b, benchmark.strategy, benchmark.weights)
			ctx := context.Background()
			var err error
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				benchmarkRouterResponse, err = router.Chat(ctx, request)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRouterParallel(b *testing.B) {
	request := &provider.ChatRequest{
		Model:    "benchmark-model",
		Messages: []provider.ChatMessage{{Role: "user", Content: "benchmark"}},
	}
	for _, benchmark := range []struct {
		name     string
		strategy Strategy
		weights  []int
	}{
		{name: "RoundRobin", strategy: StrategyRoundRobin, weights: []int{1, 1, 1}},
		{name: "WeightedRoundRobin", strategy: StrategyWeightedRoundRobin, weights: []int{5, 3, 2}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			router := newBenchmarkRouter(b, benchmark.strategy, benchmark.weights)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(worker *testing.PB) {
				for worker.Next() {
					response, err := router.Chat(ctx, request)
					if err != nil {
						b.Error(err)
						return
					}
					_ = response
				}
			})
		})
	}
}

func newBenchmarkRouter(
	b *testing.B,
	strategy Strategy,
	weights []int,
) *Router {
	b.Helper()
	targets := make([]WeightedTarget, 0, len(weights))
	for index, weight := range weights {
		targets = append(targets, WeightedTarget{
			Target: benchmarkRoutingTarget{name: string(rune('a' + index))},
			Weight: weight,
		})
	}
	router, err := New(strategy, targets)
	if err != nil {
		b.Fatal(err)
	}
	return router
}

type benchmarkRoutingTarget struct {
	name string
}

func (t benchmarkRoutingTarget) Name() string {
	return t.name
}

func (benchmarkRoutingTarget) Available() bool {
	return true
}

func (benchmarkRoutingTarget) Chat(
	_ context.Context,
	request *provider.ChatRequest,
) (*provider.ChatResponse, error) {
	return &provider.ChatResponse{Model: request.Model}, nil
}

func (benchmarkRoutingTarget) ChatStream(
	context.Context,
	*provider.ChatRequest,
) (provider.Stream, error) {
	return nil, context.Canceled
}
