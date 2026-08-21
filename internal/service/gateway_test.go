package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/theHerta27/ModelGate/internal/cache"
	"github.com/theHerta27/ModelGate/internal/idempotency"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/storage"
)

type gatewayProvider struct {
	response    *provider.ChatResponse
	err         error
	stream      provider.Stream
	chatCalls   int
	streamCalls int
}

func (p *gatewayProvider) Chat(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	p.chatCalls++
	return p.response, p.err
}

func (p *gatewayProvider) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	p.streamCalls++
	return p.stream, p.err
}

type fakeLimiter struct {
	decision ratelimit.Decision
	err      error
	calls    int
}

func (l *fakeLimiter) Allow(context.Context, string) (ratelimit.Decision, error) {
	l.calls++
	return l.decision, l.err
}

type fakeIdempotencyStore struct {
	beginResult   idempotency.Result
	beginErr      error
	completeErr   error
	releaseErr    error
	beginCalls    int
	completeCalls int
	releaseCalls  int
	payload       []byte
}

func (s *fakeIdempotencyStore) Begin(context.Context, string, string) (idempotency.Result, error) {
	s.beginCalls++
	return s.beginResult, s.beginErr
}

func (s *fakeIdempotencyStore) Complete(_ context.Context, _, _ string, response []byte) error {
	s.completeCalls++
	s.payload = append([]byte(nil), response...)
	return s.completeErr
}

func (s *fakeIdempotencyStore) Release(context.Context, string, string) error {
	s.releaseCalls++
	return s.releaseErr
}

type fakeCache struct {
	payload  []byte
	found    bool
	getErr   error
	setErr   error
	getCalls int
	setCalls int
}

func (c *fakeCache) Get(context.Context, string) ([]byte, bool, error) {
	c.getCalls++
	return c.payload, c.found, c.getErr
}

func (c *fakeCache) Set(_ context.Context, _ string, response []byte) error {
	c.setCalls++
	c.payload = append([]byte(nil), response...)
	return c.setErr
}

type fakeRecorder struct {
	records []storage.RequestRecord
	err     error
}

func (r *fakeRecorder) Record(_ context.Context, record storage.RequestRecord) error {
	r.records = append(r.records, record)
	return r.err
}

func TestGatewayChatWithoutInfrastructure(t *testing.T) {
	upstream := &gatewayProvider{response: gatewayResponse("fresh")}
	gateway := newTestGateway(upstream, GatewayOptions{})

	result, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{ClientIdentity: "client"})
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if result.Response.ID != "fresh" || result.CacheStatus != cache.StatusBypass || upstream.chatCalls != 1 {
		t.Fatalf("result = %#v, calls = %d", result, upstream.chatCalls)
	}
	if result.RequestID == "" {
		t.Fatal("request ID is empty")
	}
}

func TestGatewayRateLimitDeniedBeforeProvider(t *testing.T) {
	upstream := &gatewayProvider{response: gatewayResponse("unused")}
	limiter := &fakeLimiter{decision: ratelimit.Decision{
		Allowed: false, Limit: 1, Remaining: 0, RetryAfter: time.Second,
	}}
	recorder := &fakeRecorder{}
	gateway := newTestGateway(upstream, GatewayOptions{Limiter: limiter, Recorder: recorder})

	_, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{ClientIdentity: "client"})
	var rateLimitErr *RateLimitError
	if !errors.As(err, &rateLimitErr) {
		t.Fatalf("Chat() error = %v, want RateLimitError", err)
	}
	if upstream.chatCalls != 0 || len(recorder.records) != 1 || recorder.records[0].Status != storage.StatusRateLimited {
		t.Fatalf("provider calls/records = %d/%#v", upstream.chatCalls, recorder.records)
	}
}

func TestGatewayRateLimiterFailurePolicy(t *testing.T) {
	for _, test := range []struct {
		name     string
		failOpen bool
		wantCall bool
	}{
		{name: "open", failOpen: true, wantCall: true},
		{name: "closed", failOpen: false, wantCall: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := &gatewayProvider{response: gatewayResponse("fresh")}
			limiter := &fakeLimiter{err: errors.New("redis unavailable")}
			errorsSeen := 0
			gateway := newTestGateway(upstream, GatewayOptions{
				Limiter: limiter, RateLimitFailOpen: test.failOpen,
				ErrorSink: func(error) { errorsSeen++ },
			})
			_, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{ClientIdentity: "client"})
			if test.wantCall && err != nil {
				t.Fatalf("Chat() error = %v", err)
			}
			if !test.wantCall {
				var dependencyErr *DependencyError
				if !errors.As(err, &dependencyErr) {
					t.Fatalf("Chat() error = %v, want DependencyError", err)
				}
			}
			if (upstream.chatCalls == 1) != test.wantCall || errorsSeen != 1 {
				t.Fatalf("provider calls/errors = %d/%d", upstream.chatCalls, errorsSeen)
			}
		})
	}
}

func TestGatewayIdempotencyReplaySkipsProvider(t *testing.T) {
	replayed := gatewayResponse("replayed")
	payload, _ := json.Marshal(replayed)
	store := &fakeIdempotencyStore{beginResult: idempotency.Result{
		Status: idempotency.StatusCompleted, Response: payload,
	}}
	upstream := &gatewayProvider{response: gatewayResponse("unused")}
	gateway := newTestGateway(upstream, GatewayOptions{Idempotency: store})

	result, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{
		ClientIdentity: "client", IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if !result.IdempotencyReplayed || result.Response.ID != "replayed" || upstream.chatCalls != 0 {
		t.Fatalf("result/calls = %#v/%d", result, upstream.chatCalls)
	}
}

func TestGatewayIdempotencyPendingConflictAndUnavailable(t *testing.T) {
	tests := []struct {
		name  string
		store idempotency.Store
		code  string
	}{
		{name: "pending", store: &fakeIdempotencyStore{beginResult: idempotency.Result{Status: idempotency.StatusPending}}, code: "idempotency_request_in_progress"},
		{name: "conflict", store: &fakeIdempotencyStore{beginResult: idempotency.Result{Status: idempotency.StatusConflict}}, code: "idempotency_key_conflict"},
		{name: "unavailable", store: nil, code: "idempotency_unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := &gatewayProvider{response: gatewayResponse("unused")}
			gateway := newTestGateway(upstream, GatewayOptions{Idempotency: test.store})
			_, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{
				ClientIdentity: "client", IdempotencyKey: "request-1",
			})
			if gatewayErrorCode(err) != test.code || upstream.chatCalls != 0 {
				t.Fatalf("error/calls = %v/%d", err, upstream.chatCalls)
			}
		})
	}
}

func TestGatewayCompletesAndReleasesIdempotency(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		store := &fakeIdempotencyStore{beginResult: idempotency.Result{Status: idempotency.StatusAcquired}}
		upstream := &gatewayProvider{response: gatewayResponse("fresh")}
		gateway := newTestGateway(upstream, GatewayOptions{Idempotency: store})
		_, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{
			ClientIdentity: "client", IdempotencyKey: "request-1",
		})
		if err != nil || store.completeCalls != 1 || store.releaseCalls != 0 {
			t.Fatalf("error/complete/release = %v/%d/%d", err, store.completeCalls, store.releaseCalls)
		}
	})
	t.Run("provider failure", func(t *testing.T) {
		store := &fakeIdempotencyStore{beginResult: idempotency.Result{Status: idempotency.StatusAcquired}}
		upstream := &gatewayProvider{err: errors.New("upstream failed")}
		gateway := newTestGateway(upstream, GatewayOptions{Idempotency: store})
		_, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{
			ClientIdentity: "client", IdempotencyKey: "request-1",
		})
		if err == nil || store.releaseCalls != 1 || store.completeCalls != 0 {
			t.Fatalf("error/complete/release = %v/%d/%d", err, store.completeCalls, store.releaseCalls)
		}
	})
}

func TestGatewayCachePolicyHitMissAndBypass(t *testing.T) {
	zero := 0.0
	t.Run("hit", func(t *testing.T) {
		cached := gatewayResponse("cached")
		payload, _ := json.Marshal(cached)
		responseCache := &fakeCache{payload: payload, found: true}
		upstream := &gatewayProvider{response: gatewayResponse("unused")}
		recorder := &fakeRecorder{}
		gateway := newTestGateway(upstream, GatewayOptions{Cache: responseCache, Recorder: recorder})
		result, err := gateway.Chat(context.Background(), gatewayRequest(&zero), RequestMetadata{ClientIdentity: "client"})
		if err != nil || result.CacheStatus != cache.StatusHit || upstream.chatCalls != 0 {
			t.Fatalf("result/error/calls = %#v/%v/%d", result, err, upstream.chatCalls)
		}
		if recorder.records[0].TotalTokens != 0 || recorder.records[0].Status != storage.StatusCacheHit {
			t.Fatalf("cache hit usage record = %#v", recorder.records[0])
		}
	})
	t.Run("miss", func(t *testing.T) {
		responseCache := &fakeCache{}
		upstream := &gatewayProvider{response: gatewayResponse("fresh")}
		gateway := newTestGateway(upstream, GatewayOptions{Cache: responseCache})
		result, err := gateway.Chat(context.Background(), gatewayRequest(&zero), RequestMetadata{ClientIdentity: "client"})
		if err != nil || result.CacheStatus != cache.StatusMiss || responseCache.setCalls != 1 {
			t.Fatalf("result/error/set = %#v/%v/%d", result, err, responseCache.setCalls)
		}
	})
	t.Run("read failure acts as miss", func(t *testing.T) {
		responseCache := &fakeCache{getErr: errors.New("redis unavailable")}
		upstream := &gatewayProvider{response: gatewayResponse("fresh")}
		errorsSeen := 0
		gateway := newTestGateway(upstream, GatewayOptions{
			Cache: responseCache, ErrorSink: func(error) { errorsSeen++ },
		})
		result, err := gateway.Chat(context.Background(), gatewayRequest(&zero), RequestMetadata{ClientIdentity: "client"})
		if err != nil || result.CacheStatus != cache.StatusMiss || upstream.chatCalls != 1 || responseCache.setCalls != 1 || errorsSeen != 1 {
			t.Fatalf("result/error/calls/set/errors = %#v/%v/%d/%d/%d", result, err, upstream.chatCalls, responseCache.setCalls, errorsSeen)
		}
	})
	t.Run("bypass", func(t *testing.T) {
		responseCache := &fakeCache{}
		upstream := &gatewayProvider{response: gatewayResponse("fresh")}
		gateway := newTestGateway(upstream, GatewayOptions{Cache: responseCache})
		result, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{ClientIdentity: "client"})
		if err != nil || result.CacheStatus != cache.StatusBypass || responseCache.getCalls != 0 {
			t.Fatalf("result/error/get = %#v/%v/%d", result, err, responseCache.getCalls)
		}
	})
}

func TestGatewayRecorderFailureDoesNotReplaceSuccess(t *testing.T) {
	upstream := &gatewayProvider{response: gatewayResponse("fresh")}
	recorder := &fakeRecorder{err: errors.New("database unavailable")}
	errorsSeen := 0
	gateway := newTestGateway(upstream, GatewayOptions{
		Recorder: recorder, ErrorSink: func(error) { errorsSeen++ },
	})
	result, err := gateway.Chat(context.Background(), gatewayRequest(nil), RequestMetadata{ClientIdentity: "client"})
	if err != nil || result.Response.ID != "fresh" || errorsSeen != 1 {
		t.Fatalf("result/error/errors = %#v/%v/%d", result, err, errorsSeen)
	}
}

func TestGatewayStreamRejectsIdempotencyAndRecordsUsage(t *testing.T) {
	request := gatewayRequest(nil)
	request.Stream = true
	upstream := &gatewayProvider{stream: &gatewayStream{chunks: []*provider.ChatStreamChunk{{
		ID: "chunk", Object: "chat.completion.chunk", Model: "model",
		Usage: &provider.ChatUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
	}}}}
	recorder := &fakeRecorder{}
	gateway := newTestGateway(upstream, GatewayOptions{Recorder: recorder})

	if _, err := gateway.ChatStream(context.Background(), request, RequestMetadata{
		ClientIdentity: "client", IdempotencyKey: "request-1",
	}); gatewayErrorCode(err) != "idempotency_not_supported_for_stream" {
		t.Fatalf("ChatStream() error = %v", err)
	}
	result, err := gateway.ChatStream(context.Background(), request, RequestMetadata{ClientIdentity: "client"})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	if _, err := result.Stream.Recv(); err != nil {
		t.Fatalf("first Recv() error = %v", err)
	}
	if _, err := result.Stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Recv() error = %v", err)
	}
	if len(recorder.records) != 1 || recorder.records[0].TotalTokens != 5 || recorder.records[0].Status != storage.StatusSucceeded {
		t.Fatalf("stream usage record = %#v", recorder.records)
	}
}

func TestGatewayStreamCloseRecordsFailureOnce(t *testing.T) {
	request := gatewayRequest(nil)
	request.Stream = true
	upstream := &gatewayProvider{stream: &gatewayStream{}}
	recorder := &fakeRecorder{}
	gateway := newTestGateway(upstream, GatewayOptions{Recorder: recorder})

	result, err := gateway.ChatStream(context.Background(), request, RequestMetadata{ClientIdentity: "client"})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	if err := result.Stream.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := result.Stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if len(recorder.records) != 1 || recorder.records[0].Status != storage.StatusFailed || recorder.records[0].ErrorCode != "stream_closed" {
		t.Fatalf("stream close record = %#v", recorder.records)
	}
}

type gatewayStream struct {
	chunks []*provider.ChatStreamChunk
	index  int
}

func (s *gatewayStream) Recv() (*provider.ChatStreamChunk, error) {
	if s.index >= len(s.chunks) {
		return nil, io.EOF
	}
	chunk := s.chunks[s.index]
	s.index++
	return chunk, nil
}

func (*gatewayStream) Close() error { return nil }

func newTestGateway(upstream provider.Provider, options GatewayOptions) *GatewayService {
	if options.ProviderName == "" {
		options.ProviderName = "mock"
	}
	if options.OperationTimeout == 0 {
		options.OperationTimeout = time.Second
	}
	return NewGatewayService(NewChatService(upstream), options)
}

func gatewayRequest(temperature *float64) *provider.ChatRequest {
	return &provider.ChatRequest{
		Model: "model", Messages: []provider.ChatMessage{{Role: "user", Content: "hello"}},
		Temperature: temperature,
	}
}

func gatewayResponse(id string) *provider.ChatResponse {
	return &provider.ChatResponse{
		ID: id, Object: "chat.completion", Model: "model",
		Choices: []provider.ChatChoice{{
			Index: 0, Message: provider.ChatMessage{Role: "assistant", Content: "hello"},
			FinishReason: "stop",
		}},
		Usage: provider.ChatUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
	}
}
