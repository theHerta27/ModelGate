package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOpenAICompatibleProviderChat(t *testing.T) {
	var received ChatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %s, want /v1/chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
            "id":"chatcmpl-upstream",
            "object":"chat.completion",
            "created":1,
            "model":"test-model",
            "choices":[{
                "index":0,
                "message":{"role":"assistant","content":"hello"},
                "finish_reason":"stop"
            }],
            "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
        }`))
	}))
	defer server.Close()

	p, err := NewOpenAICompatibleProvider(
		"test",
		server.URL+"/v1",
		"test-key",
		server.Client(),
	)
	if err != nil {
		t.Fatalf("NewOpenAICompatibleProvider() error = %v", err)
	}

	req := &ChatRequest{
		Model:    "test-model",
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	}
	resp, err := p.Chat(context.Background(), req)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if received.Model != req.Model {
		t.Fatalf("upstream model = %q, want %q", received.Model, req.Model)
	}
	if resp.ID != "chatcmpl-upstream" {
		t.Fatalf("response ID = %q", resp.ID)
	}
}

func TestOpenAICompatibleProviderRejectsUpstreamError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"do not expose me"}}`))
	}))
	defer server.Close()

	p, err := NewOpenAICompatibleProvider("test", server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("NewOpenAICompatibleProvider() error = %v", err)
	}

	_, err = p.Chat(context.Background(), &ChatRequest{})
	if err == nil {
		t.Fatal("Chat() error = nil, want upstream status error")
	}
}

func TestOpenAICompatibleProviderPropagatesCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	client := server.Client()
	client.Timeout = time.Second
	p, err := NewOpenAICompatibleProvider("test", server.URL, "test-key", client)
	if err != nil {
		t.Fatalf("NewOpenAICompatibleProvider() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = p.Chat(ctx, &ChatRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Chat() error = %v, want context.Canceled", err)
	}
}

func TestOpenAICompatibleProviderValidatesConstructor(t *testing.T) {
	_, err := NewOpenAICompatibleProvider("test", "not-a-url", "key", http.DefaultClient)
	if err == nil {
		t.Fatal("constructor error = nil, want invalid URL error")
	}
}
