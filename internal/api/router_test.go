package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/theHerta27/ModelGate/internal/provider"
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
}

func TestChatCompletionsRejectsInvalidJSON(t *testing.T) {
	recorder := performJSONRequest(testRouter(provider.NewMockProvider()), []byte(`{"model":`))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"invalid_json"`) {
		t.Fatalf("body = %s", recorder.Body.String())
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
	gin.SetMode(gin.TestMode)
	chatService := service.NewChatService(chatProvider)
	return NewRouter(NewHandler(chatService))
}

func performJSONRequest(router http.Handler, body []byte) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)
	return recorder
}
