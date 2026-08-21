package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	"github.com/theHerta27/ModelGate/internal/provider"
)

var ErrNoHealthyProviders = errors.New("no healthy providers are available")

type Strategy string

const (
	StrategyRoundRobin         Strategy = "round_robin"
	StrategyWeightedRoundRobin Strategy = "weighted_round_robin"
)

type Target interface {
	provider.Provider
	Name() string
	Available() bool
}

type WeightedTarget struct {
	Target Target
	Weight int
}

type Router struct {
	mu       sync.Mutex
	strategy Strategy
	targets  []targetState
	next     int
}

type targetState struct {
	target        Target
	weight        int64
	currentWeight int64
}

func New(strategy Strategy, targets []WeightedTarget) (*Router, error) {
	if strategy != StrategyRoundRobin && strategy != StrategyWeightedRoundRobin {
		return nil, fmt.Errorf("unsupported routing strategy %q", strategy)
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("at least one provider target is required")
	}
	states := make([]targetState, 0, len(targets))
	names := make(map[string]struct{}, len(targets))
	for _, candidate := range targets {
		if candidate.Target == nil {
			return nil, fmt.Errorf("provider target is required")
		}
		name := strings.TrimSpace(candidate.Target.Name())
		if name == "" {
			return nil, fmt.Errorf("provider target name is required")
		}
		if _, exists := names[name]; exists {
			return nil, fmt.Errorf("provider target %q is duplicated", name)
		}
		if candidate.Weight <= 0 {
			return nil, fmt.Errorf("provider target %q weight must be positive", name)
		}
		names[name] = struct{}{}
		states = append(states, targetState{target: candidate.Target, weight: int64(candidate.Weight)})
	}
	return &Router{strategy: strategy, targets: states}, nil
}

func (r *Router) Chat(ctx context.Context, request *provider.ChatRequest) (*provider.ChatResponse, error) {
	excluded := make(map[string]struct{}, len(r.targets))
	for len(excluded) < len(r.targets) {
		target, err := r.selectTarget(excluded)
		if err != nil {
			return nil, unavailable("chat", err)
		}
		response, err := target.Chat(ctx, request)
		if errors.Is(err, circuitbreaker.ErrOpen) {
			excluded[target.Name()] = struct{}{}
			continue
		}
		if err != nil {
			return nil, err
		}
		if response == nil {
			return nil, &provider.UpstreamError{
				Provider: target.Name(), Operation: "chat", Kind: provider.ErrorKindInvalidResponse,
				Cause: fmt.Errorf("provider returned a nil response"),
			}
		}
		response.Provider = target.Name()
		return response, nil
	}
	return nil, unavailable("chat", ErrNoHealthyProviders)
}

func (r *Router) ChatStream(ctx context.Context, request *provider.ChatRequest) (provider.Stream, error) {
	excluded := make(map[string]struct{}, len(r.targets))
	for len(excluded) < len(r.targets) {
		target, err := r.selectTarget(excluded)
		if err != nil {
			return nil, unavailable("chat stream", err)
		}
		stream, err := target.ChatStream(ctx, request)
		if errors.Is(err, circuitbreaker.ErrOpen) {
			excluded[target.Name()] = struct{}{}
			continue
		}
		if err != nil {
			return nil, err
		}
		if stream == nil {
			return nil, &provider.UpstreamError{
				Provider: target.Name(), Operation: "chat stream", Kind: provider.ErrorKindInvalidResponse,
				Cause: fmt.Errorf("provider returned a nil stream"),
			}
		}
		return &namedStream{name: target.Name(), inner: stream}, nil
	}
	return nil, unavailable("chat stream", ErrNoHealthyProviders)
}

func (r *Router) selectTarget(excluded map[string]struct{}) (Target, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.strategy {
	case StrategyRoundRobin:
		for checked := 0; checked < len(r.targets); checked++ {
			index := r.next % len(r.targets)
			r.next = (r.next + 1) % len(r.targets)
			candidate := r.targets[index].target
			if _, skip := excluded[candidate.Name()]; skip || !candidate.Available() {
				continue
			}
			return candidate, nil
		}
	case StrategyWeightedRoundRobin:
		selected := -1
		var total int64
		for index := range r.targets {
			candidate := &r.targets[index]
			if _, skip := excluded[candidate.target.Name()]; skip || !candidate.target.Available() {
				continue
			}
			candidate.currentWeight += candidate.weight
			total += candidate.weight
			if selected == -1 || candidate.currentWeight > r.targets[selected].currentWeight {
				selected = index
			}
		}
		if selected >= 0 {
			r.targets[selected].currentWeight -= total
			return r.targets[selected].target, nil
		}
	}
	return nil, ErrNoHealthyProviders
}

func unavailable(operation string, cause error) error {
	return &provider.UpstreamError{
		Provider: "router", Operation: operation, Kind: provider.ErrorKindUnavailable,
		Cause: cause,
	}
}

type namedStream struct {
	name  string
	inner provider.Stream
}

func (s *namedStream) ProviderName() string {
	return s.name
}

func (s *namedStream) Recv() (*provider.ChatStreamChunk, error) {
	return s.inner.Recv()
}

func (s *namedStream) Close() error {
	return s.inner.Close()
}

var _ io.Closer = (*namedStream)(nil)
