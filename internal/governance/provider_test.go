package governance

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	providerconcurrency "github.com/theHerta27/ModelGate/internal/concurrency"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/retry"
)

type fakeProvider struct {
	chatCalls   int
	streamCalls int
	chatErrors  []error
	stream      provider.Stream
}

func (p *fakeProvider) Chat(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	index := p.chatCalls
	p.chatCalls++
	if index < len(p.chatErrors) && p.chatErrors[index] != nil {
		return nil, p.chatErrors[index]
	}
	return &provider.ChatResponse{ID: "ok"}, nil
}

func (p *fakeProvider) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	p.streamCalls++
	return p.stream, nil
}

func TestProviderRetriesAndNamesResponse(t *testing.T) {
	retryable := &provider.UpstreamError{
		Provider: "upstream", Kind: provider.ErrorKindHTTPStatus,
		StatusCode: 503, Retryable: true,
	}
	inner := &fakeProvider{chatErrors: []error{retryable, retryable}}
	managed := newTestProvider(t, inner, 1, circuitbreaker.Options{
		WindowSize: 2, MinimumRequests: 2, FailureRatio: 1,
	})

	response, err := managed.Chat(context.Background(), &provider.ChatRequest{})
	if err != nil || inner.chatCalls != 3 || response.Provider != "primary" {
		t.Fatalf("response/error/calls = %#v/%v/%d", response, err, inner.chatCalls)
	}
	if managed.CircuitState() != circuitbreaker.StateClosed {
		t.Fatalf("state = %s", managed.CircuitState())
	}
}

func TestProviderCircuitOpensAfterFinalFailures(t *testing.T) {
	retryable := &provider.UpstreamError{
		Provider: "upstream", Kind: provider.ErrorKindHTTPStatus,
		StatusCode: 503, Retryable: true,
	}
	inner := &fakeProvider{chatErrors: []error{retryable, retryable, retryable, retryable, retryable, retryable}}
	managed := newTestProvider(t, inner, 1, circuitbreaker.Options{
		WindowSize: 2, MinimumRequests: 2, FailureRatio: 1,
	})
	for range 2 {
		if _, err := managed.Chat(context.Background(), &provider.ChatRequest{}); err == nil {
			t.Fatal("Chat() error = nil")
		}
	}
	if managed.Available() || managed.CircuitState() != circuitbreaker.StateOpen {
		t.Fatalf("available/state = %v/%s", managed.Available(), managed.CircuitState())
	}
	if _, err := managed.Chat(context.Background(), &provider.ChatRequest{}); !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("Chat() error = %v, want ErrOpen", err)
	}
}

func TestProviderStreamHoldsConcurrencyUntilClosed(t *testing.T) {
	innerStream := &fakeStream{}
	inner := &fakeProvider{stream: innerStream}
	managed := newTestProvider(t, inner, 1, circuitbreaker.Options{
		WindowSize: 2, MinimumRequests: 2, FailureRatio: 1,
	})
	stream, err := managed.ChatStream(context.Background(), &provider.ChatRequest{Stream: true})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	if provider.StreamProviderName(stream) != "primary" {
		t.Fatalf("provider name = %q", provider.StreamProviderName(stream))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := managed.Chat(ctx, &provider.ChatRequest{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent Chat() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := managed.Chat(context.Background(), &provider.ChatRequest{}); err != nil {
		t.Fatalf("Chat() after Close error = %v", err)
	}
}

func TestManagedStreamEOFCompletesOnce(t *testing.T) {
	inner := &fakeProvider{stream: &fakeStream{recvErr: io.EOF}}
	managed := newTestProvider(t, inner, 1, circuitbreaker.Options{
		WindowSize: 1, MinimumRequests: 1, FailureRatio: 1,
	})
	stream, err := managed.ChatStream(context.Background(), &provider.ChatRequest{Stream: true})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if managed.CircuitState() != circuitbreaker.StateClosed {
		t.Fatalf("state = %s", managed.CircuitState())
	}
}

func TestNewProviderRejectsMissingDependencies(t *testing.T) {
	if _, err := NewProvider("", nil, nil, nil, nil); err == nil {
		t.Fatal("NewProvider() error = nil")
	}
}

type fakeStream struct {
	recvErr error
}

func (s *fakeStream) Recv() (*provider.ChatStreamChunk, error) {
	return nil, s.recvErr
}

func (*fakeStream) Close() error { return nil }

func newTestProvider(
	t *testing.T,
	inner provider.Provider,
	maxConcurrent int,
	breakerOptions circuitbreaker.Options,
) *Provider {
	t.Helper()
	retryPolicy, err := retry.New(retry.Options{
		MaxAttempts: 3, AttemptTimeout: time.Second,
		BaseBackoff: time.Millisecond, MaxBackoff: time.Second,
		Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatalf("retry.New() error = %v", err)
	}
	if breakerOptions.OpenTimeout == 0 {
		breakerOptions.OpenTimeout = time.Hour
	}
	if breakerOptions.HalfOpenMaxRequests == 0 {
		breakerOptions.HalfOpenMaxRequests = 1
	}
	breaker, err := circuitbreaker.New(breakerOptions)
	if err != nil {
		t.Fatalf("circuitbreaker.New() error = %v", err)
	}
	semaphore, err := providerconcurrency.New(maxConcurrent)
	if err != nil {
		t.Fatalf("concurrency.New() error = %v", err)
	}
	managed, err := NewProvider("primary", inner, retryPolicy, breaker, semaphore)
	if err != nil {
		t.Fatalf("NewProvider() error = %v", err)
	}
	return managed
}
