package governance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	providerconcurrency "github.com/theHerta27/ModelGate/internal/concurrency"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/retry"
)

type Provider struct {
	name      string
	inner     provider.Provider
	retry     *retry.Policy
	breaker   *circuitbreaker.Breaker
	semaphore *providerconcurrency.Semaphore
}

func NewProvider(
	name string,
	inner provider.Provider,
	retryPolicy *retry.Policy,
	breaker *circuitbreaker.Breaker,
	semaphore *providerconcurrency.Semaphore,
) (*Provider, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("provider name is required")
	}
	if inner == nil || retryPolicy == nil || breaker == nil || semaphore == nil {
		return nil, fmt.Errorf("provider, retry policy, breaker, and semaphore are required")
	}
	return &Provider{
		name: name, inner: inner, retry: retryPolicy,
		breaker: breaker, semaphore: semaphore,
	}, nil
}

func (p *Provider) Name() string {
	return p.name
}

func (p *Provider) Available() bool {
	return p.breaker.Available()
}

func (p *Provider) CircuitState() circuitbreaker.State {
	return p.breaker.State()
}

func (p *Provider) Chat(ctx context.Context, request *provider.ChatRequest) (*provider.ChatResponse, error) {
	concurrencyPermit, err := p.semaphore.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer concurrencyPermit.Release()

	circuitPermit, err := p.breaker.Allow()
	if err != nil {
		return nil, err
	}

	var response *provider.ChatResponse
	err = p.retry.Do(ctx, provider.IsRetryable, func(attemptCtx context.Context) error {
		var callErr error
		response, callErr = p.inner.Chat(attemptCtx, request)
		if callErr == nil && response == nil {
			return &provider.UpstreamError{
				Provider: p.name, Operation: "chat", Kind: provider.ErrorKindInvalidResponse,
				Cause: fmt.Errorf("provider returned a nil response"),
			}
		}
		return callErr
	})
	circuitPermit.Done(circuitOutcome(err))
	if err != nil {
		return nil, err
	}
	response.Provider = p.name
	return response, nil
}

func (p *Provider) ChatStream(ctx context.Context, request *provider.ChatRequest) (provider.Stream, error) {
	concurrencyPermit, err := p.semaphore.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	circuitPermit, err := p.breaker.Allow()
	if err != nil {
		concurrencyPermit.Release()
		return nil, err
	}

	var stream provider.Stream
	attemptCancel, err := p.retry.DoRetained(ctx, provider.IsRetryable, func(attemptCtx context.Context) error {
		var callErr error
		stream, callErr = p.inner.ChatStream(attemptCtx, request)
		if callErr == nil && stream == nil {
			return &provider.UpstreamError{
				Provider: p.name, Operation: "chat stream", Kind: provider.ErrorKindInvalidResponse,
				Cause: fmt.Errorf("provider returned a nil stream"),
			}
		}
		return callErr
	})
	if err != nil {
		circuitPermit.Done(circuitOutcome(err))
		concurrencyPermit.Release()
		return nil, err
	}
	return &managedStream{
		providerName: p.name,
		inner:        stream,
		circuit:      circuitPermit,
		concurrency:  concurrencyPermit,
		cancel:       attemptCancel,
	}, nil
}

func circuitOutcome(err error) circuitbreaker.Outcome {
	switch {
	case err == nil:
		return circuitbreaker.OutcomeSuccess
	case provider.IsCircuitFailure(err):
		return circuitbreaker.OutcomeFailure
	default:
		return circuitbreaker.OutcomeIgnored
	}
}

type managedStream struct {
	providerName string
	inner        provider.Stream
	circuit      *circuitbreaker.Permit
	concurrency  *providerconcurrency.Permit
	cancel       context.CancelFunc
	once         sync.Once
}

func (s *managedStream) ProviderName() string {
	return s.providerName
}

func (s *managedStream) Recv() (*provider.ChatStreamChunk, error) {
	chunk, err := s.inner.Recv()
	switch {
	case errors.Is(err, io.EOF):
		s.finish(circuitbreaker.OutcomeSuccess)
	case err != nil:
		s.finish(circuitOutcome(err))
	}
	return chunk, err
}

func (s *managedStream) Close() error {
	err := s.inner.Close()
	if err != nil {
		s.finish(circuitbreaker.OutcomeFailure)
	} else {
		s.finish(circuitbreaker.OutcomeIgnored)
	}
	return err
}

func (s *managedStream) finish(outcome circuitbreaker.Outcome) {
	s.once.Do(func() {
		s.cancel()
		s.circuit.Done(outcome)
		s.concurrency.Release()
	})
}
