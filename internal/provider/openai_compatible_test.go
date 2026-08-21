package provider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestOpenAICompatibleProviderChatStream(t *testing.T) {
	var received ChatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("Accept = %q, want text/event-stream", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = io.WriteString(w, ": keep-alive\n\n")
		_, _ = io.WriteString(w, "data: {\"id\":\"stream-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"id\":\"stream-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	p, err := NewOpenAICompatibleProvider("test", server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("NewOpenAICompatibleProvider() error = %v", err)
	}
	stream, err := p.ChatStream(context.Background(), &ChatRequest{
		Model:    "test-model",
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	defer stream.Close()

	first, err := stream.Recv()
	if err != nil || first.Choices[0].Delta.Role != "assistant" {
		t.Fatalf("first Recv() = %#v, %v", first, err)
	}
	second, err := stream.Recv()
	if err != nil || second.Choices[0].Delta.Content != "hello" {
		t.Fatalf("second Recv() = %#v, %v", second, err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("final Recv() error = %v, want io.EOF", err)
	}
	if !received.Stream {
		t.Fatal("upstream request stream = false, want true")
	}
}

func TestOpenAICompatibleProviderChatStreamPropagatesCancellation(t *testing.T) {
	requestCanceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"stream-cancel\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"test-model\",\"choices\":[]}\n\n")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test response writer does not support flushing")
			return
		}
		flusher.Flush()
		<-r.Context().Done()
		close(requestCanceled)
	}))
	defer server.Close()

	p, err := NewOpenAICompatibleProvider("test", server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("NewOpenAICompatibleProvider() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := p.ChatStream(ctx, &ChatRequest{Model: "test-model"})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	defer stream.Close()

	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv() error = %v", err)
	}
	cancel()
	if _, err := stream.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Recv() error = %v, want context.Canceled", err)
	}
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("upstream request context was not canceled")
	}
}

func TestOpenAICompatibleProviderRejectsNonSSEStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()

	p, err := NewOpenAICompatibleProvider("test", server.URL, "test-key", server.Client())
	if err != nil {
		t.Fatalf("NewOpenAICompatibleProvider() error = %v", err)
	}
	if _, err := p.ChatStream(context.Background(), &ChatRequest{}); err == nil {
		t.Fatal("ChatStream() error = nil, want non-SSE error")
	}
}
