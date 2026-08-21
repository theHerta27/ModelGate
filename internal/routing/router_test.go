package routing

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	"github.com/theHerta27/ModelGate/internal/provider"
)

type fakeTarget struct {
	mu        sync.Mutex
	name      string
	available bool
	chatErr   error
	calls     int
}

func (t *fakeTarget) Name() string {
	return t.name
}

func (t *fakeTarget) Available() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.available
}

func (t *fakeTarget) Chat(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	if t.chatErr != nil {
		return nil, t.chatErr
	}
	return &provider.ChatResponse{ID: t.name}, nil
}

func (t *fakeTarget) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	if t.chatErr != nil {
		return nil, t.chatErr
	}
	return &fakeStream{}, nil
}

func (t *fakeTarget) Calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

func TestRoundRobinAndSkipUnhealthy(t *testing.T) {
	first := &fakeTarget{name: "first", available: true}
	second := &fakeTarget{name: "second", available: true}
	third := &fakeTarget{name: "third", available: false}
	router := newTestRouter(t, StrategyRoundRobin, first, second, third)

	got := make([]string, 0, 4)
	for range 4 {
		response, err := router.Chat(context.Background(), &provider.ChatRequest{})
		if err != nil {
			t.Fatalf("Chat() error = %v", err)
		}
		got = append(got, response.Provider)
	}
	want := []string{"first", "second", "first", "second"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("route sequence = %#v, want %#v", got, want)
		}
	}
	if third.Calls() != 0 {
		t.Fatalf("unhealthy provider calls = %d", third.Calls())
	}
}

func TestSmoothWeightedRoundRobin(t *testing.T) {
	first := &fakeTarget{name: "first", available: true}
	second := &fakeTarget{name: "second", available: true}
	router, err := New(StrategyWeightedRoundRobin, []WeightedTarget{
		{Target: first, Weight: 1},
		{Target: second, Weight: 2},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for range 60 {
		if _, err := router.Chat(context.Background(), &provider.ChatRequest{}); err != nil {
			t.Fatalf("Chat() error = %v", err)
		}
	}
	if first.Calls() != 20 || second.Calls() != 40 {
		t.Fatalf("weighted calls = %d/%d, want 20/40", first.Calls(), second.Calls())
	}
}

func TestRouterFallsThroughCircuitRace(t *testing.T) {
	first := &fakeTarget{name: "first", available: true, chatErr: circuitbreaker.ErrOpen}
	second := &fakeTarget{name: "second", available: true}
	router := newTestRouter(t, StrategyRoundRobin, first, second)
	response, err := router.Chat(context.Background(), &provider.ChatRequest{})
	if err != nil || response.Provider != "second" || first.Calls() != 1 || second.Calls() != 1 {
		t.Fatalf("response/error/calls = %#v/%v/%d/%d", response, err, first.Calls(), second.Calls())
	}
}

func TestRouterReturnsUnavailableWhenAllUnhealthy(t *testing.T) {
	router := newTestRouter(t, StrategyRoundRobin,
		&fakeTarget{name: "first"},
		&fakeTarget{name: "second"},
	)
	_, err := router.Chat(context.Background(), &provider.ChatRequest{})
	if !errors.Is(err, ErrNoHealthyProviders) || provider.ErrorCode(err) != "no_healthy_provider" {
		t.Fatalf("Chat() error/code = %v/%s", err, provider.ErrorCode(err))
	}
}

func TestRouterNamesStream(t *testing.T) {
	target := &fakeTarget{name: "primary", available: true}
	router := newTestRouter(t, StrategyRoundRobin, target)
	stream, err := router.ChatStream(context.Background(), &provider.ChatRequest{Stream: true})
	if err != nil || provider.StreamProviderName(stream) != "primary" {
		t.Fatalf("stream/error/name = %#v/%v/%q", stream, err, provider.StreamProviderName(stream))
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() error = %v", err)
	}
}

func TestNewRejectsInvalidTargets(t *testing.T) {
	target := &fakeTarget{name: "same", available: true}
	for _, test := range []struct {
		strategy Strategy
		targets  []WeightedTarget
	}{
		{strategy: "unknown", targets: []WeightedTarget{{Target: target, Weight: 1}}},
		{strategy: StrategyRoundRobin},
		{strategy: StrategyRoundRobin, targets: []WeightedTarget{{Weight: 1}}},
		{strategy: StrategyRoundRobin, targets: []WeightedTarget{{Target: target, Weight: 0}}},
		{strategy: StrategyRoundRobin, targets: []WeightedTarget{{Target: target, Weight: 1}, {Target: target, Weight: 1}}},
	} {
		if _, err := New(test.strategy, test.targets); err == nil {
			t.Fatalf("New(%q, %#v) error = nil", test.strategy, test.targets)
		}
	}
}

type fakeStream struct{}

func (*fakeStream) Recv() (*provider.ChatStreamChunk, error) { return nil, io.EOF }
func (*fakeStream) Close() error                             { return nil }

func newTestRouter(t *testing.T, strategy Strategy, targets ...Target) *Router {
	t.Helper()
	weighted := make([]WeightedTarget, 0, len(targets))
	for _, target := range targets {
		weighted = append(weighted, WeightedTarget{Target: target, Weight: 1})
	}
	router, err := New(strategy, weighted)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return router
}
