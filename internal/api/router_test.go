package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/theHerta27/ModelGate/internal/cache"
	gatewaymetrics "github.com/theHerta27/ModelGate/internal/metrics"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/service"
)

func TestHealth(t *testing.T) {
	router := testRouter(provider.NewMockProvider())
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := strings.TrimSpace(recorder.Body.String()); got != `{"status":"ok"}` {
		t.Fatalf("body = %s", got)
	}
}

func TestChatCompletionsWithMockProvider(t *testing.T) {
	router := testRouter(provider.NewMockProvider())
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}]
    }`)
	recorder := performJSONRequest(router, body)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var resp provider.ChatResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Choices[0].Message.Content != "Mock response: hello" {
		t.Fatalf("content = %q", resp.Choices[0].Message.Content)
	}
	if recorder.Header().Get("X-Request-ID") == "" || recorder.Header().Get("X-ModelGate-Cache") != cache.StatusBypass {
		t.Fatalf("gateway headers = %#v", recorder.Header())
	}
	if recorder.Header().Get("X-ModelGate-Provider") != "test" {
		t.Fatalf("provider header = %q, want test", recorder.Header().Get("X-ModelGate-Provider"))
	}
}

func TestRequestIDMiddlewareReplacesUntrustedClientValue(t *testing.T) {
	recorder := performJSONRequestWithHeaders(testRouter(provider.NewMockProvider()), []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}]
    }`), map[string]string{"X-Request-ID": "client-controlled"})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if requestID := recorder.Header().Get("X-Request-ID"); requestID == "" || requestID == "client-controlled" {
		t.Fatalf("server request ID = %q", requestID)
	}
}

func TestChatCompletionsRejectsInvalidJSON(t *testing.T) {
	recorder := performJSONRequest(testRouter(provider.NewMockProvider()), []byte(`{"model":`))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"invalid_json"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	if recorder.Header().Get("X-Request-ID") == "" {
		t.Fatal("invalid request is missing a server request ID")
	}
}

func TestMetricsEndpointIsExposedWhenConfigured(t *testing.T) {
	gin.SetMode(gin.TestMode)
	telemetry := gatewaymetrics.New()
	gateway := service.NewGatewayService(
		service.NewChatService(provider.NewMockProvider()),
		service.GatewayOptions{ProviderName: "test", CachePolicy: cache.Policy{}},
	)
	router := NewRouter(NewHandler(gateway), RouterOptions{
		Metrics: telemetry,
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	health := httptest.NewRecorder()
	router.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	if !strings.Contains(response.Body.String(), "modelgate_requests_total") {
		t.Fatalf("metrics body = %s", response.Body.String())
	}
}

func TestChatCompletionsStreamsSSE(t *testing.T) {
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}],
        "stream":true
    }`)
	recorder := performJSONRequest(testRouter(provider.NewMockProvider()), body)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q", got)
	}
	if !recorder.Flushed {
		t.Fatal("stream response was not flushed")
	}
	if !strings.Contains(recorder.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("body = %s", recorder.Body.String())
	}

	var content string
	for _, event := range strings.Split(recorder.Body.String(), "\n\n") {
		data := strings.TrimPrefix(event, "data: ")
		if data == event || data == "" || data == "[DONE]" {
			continue
		}
		var chunk provider.ChatStreamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("decode chunk: %v", err)
		}
		for _, choice := range chunk.Choices {
			content += choice.Delta.Content
		}
	}
	if content != "Mock response: hello" {
		t.Fatalf("stream content = %q", content)
	}
}

func TestChatCompletionsHidesProviderError(t *testing.T) {
	failing := failingProvider{err: errors.New("secret upstream detail")}
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}]
    }`)
	recorder := performJSONRequest(testRouter(failing), body)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "secret upstream detail") {
		t.Fatalf("provider error leaked: %s", recorder.Body.String())
	}
}

func TestChatCompletionsMapsStreamSetupError(t *testing.T) {
	failing := failingProvider{err: errors.New("secret stream setup detail")}
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}],
        "stream":true
    }`)
	recorder := performJSONRequest(testRouter(failing), body)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "secret stream setup detail") {
		t.Fatalf("provider error leaked: %s", recorder.Body.String())
	}
}

func TestChatCompletionsClosesStreamAfterMidStreamError(t *testing.T) {
	stream := &trackingStream{
		chunks: []*provider.ChatStreamChunk{{
			ID:      "chunk-1",
			Object:  "chat.completion.chunk",
			Created: 1,
			Model:   "mock-model",
		}},
		err: errors.New("mid-stream failure"),
	}
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}],
        "stream":true
    }`)
	recorder := performJSONRequest(testRouter(streamOnlyProvider{stream: stream}), body)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "[DONE]") {
		t.Fatalf("failed stream must not emit DONE: %s", recorder.Body.String())
	}
	if !stream.closed {
		t.Fatal("stream was not closed")
	}
}

func TestChatCompletionsClosesStreamThatEndsBeforeFirstChunk(t *testing.T) {
	stream := &trackingStream{}
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}],
        "stream":true
    }`)
	recorder := performJSONRequest(testRouter(streamOnlyProvider{stream: stream}), body)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if !stream.closed {
		t.Fatal("empty stream was not closed")
	}
	if !strings.Contains(recorder.Body.String(), `"code":"provider_request_failed"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestChatCompletionsMapsRateLimitError(t *testing.T) {
	limiter := &apiLimiter{decision: ratelimit.Decision{
		Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 1500 * time.Millisecond,
	}}
	router := testRouterWithOptions(provider.NewMockProvider(), service.GatewayOptions{Limiter: limiter})
	recorder := performJSONRequest(router, []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}]
    }`))

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
	if recorder.Header().Get("Retry-After") != "2" || recorder.Header().Get("X-RateLimit-Limit") != "10" {
		t.Fatalf("rate limit headers = %#v", recorder.Header())
	}
	if !strings.Contains(recorder.Body.String(), `"code":"rate_limit_exceeded"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestChatCompletionsFailsClosedWhenIdempotencyIsUnavailable(t *testing.T) {
	router := testRouter(provider.NewMockProvider())
	recorder := performJSONRequestWithHeaders(router, []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}]
    }`), map[string]string{"Idempotency-Key": "request-1"})

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"idempotency_unavailable"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestChatCompletionsRejectsIdempotencyForStreams(t *testing.T) {
	router := testRouter(provider.NewMockProvider())
	recorder := performJSONRequestWithHeaders(router, []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}],
        "stream":true
    }`), map[string]string{"Idempotency-Key": "request-1"})

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"idempotency_not_supported_for_stream"`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

type failingProvider struct {
	err error
}

func (p failingProvider) Chat(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, p.err
}

func (p failingProvider) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	return nil, p.err
}

type streamOnlyProvider struct {
	stream provider.Stream
}

func (streamOnlyProvider) Chat(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, errors.New("unexpected Chat call")
}

func (p streamOnlyProvider) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	return p.stream, nil
}

type trackingStream struct {
	chunks []*provider.ChatStreamChunk
	err    error
	index  int
	closed bool
}

func (s *trackingStream) Recv() (*provider.ChatStreamChunk, error) {
	if s.index < len(s.chunks) {
		chunk := s.chunks[s.index]
		s.index++
		return chunk, nil
	}
	if s.err != nil {
		return nil, s.err
	}
	return nil, io.EOF
}

func (s *trackingStream) Close() error {
	s.closed = true
	return nil
}

func testRouter(chatProvider provider.Provider) http.Handler {
	return testRouterWithOptions(chatProvider, service.GatewayOptions{})
}

func testRouterWithOptions(
	chatProvider provider.Provider,
	options service.GatewayOptions,
) http.Handler {
	gin.SetMode(gin.TestMode)
	chatService := service.NewChatService(chatProvider)
	options.ProviderName = "test"
	options.RateLimitFailOpen = true
	options.CachePolicy = cache.Policy{}
	gateway := service.NewGatewayService(chatService, options)
	return NewRouter(NewHandler(gateway))
}

func performJSONRequest(router http.Handler, body []byte) *httptest.ResponseRecorder {
	return performJSONRequestWithHeaders(router, body, nil)
}

func performJSONRequestWithHeaders(
	router http.Handler,
	body []byte,
	headers map[string]string,
) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	router.ServeHTTP(recorder, req)
	return recorder
}

type apiLimiter struct {
	decision ratelimit.Decision
}

func (l *apiLimiter) Allow(context.Context, string) (ratelimit.Decision, error) {
	return l.decision, nil
}
