package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func TestChatCompletionsRejectsStreaming(t *testing.T) {
	body := []byte(`{
        "model":"mock-model",
        "messages":[{"role":"user","content":"hello"}],
        "stream":true
    }`)
	recorder := performJSONRequest(testRouter(provider.NewMockProvider()), body)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"streaming_not_supported"`) {
		t.Fatalf("body = %s", recorder.Body.String())
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

type failingProvider struct {
	err error
}

func (p failingProvider) Chat(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
	return nil, p.err
}

func (failingProvider) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	return nil, provider.ErrStreamingNotSupported
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
