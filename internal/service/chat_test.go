package service

import (
	"context"
	"errors"
	"testing"

	"github.com/theHerta27/ModelGate/internal/provider"
)

type stubProvider struct {
	chat func(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error)
}

func (p stubProvider) Chat(
	ctx context.Context,
	req *provider.ChatRequest,
) (*provider.ChatResponse, error) {
	return p.chat(ctx, req)
}

func (stubProvider) ChatStream(context.Context, *provider.ChatRequest) (provider.Stream, error) {
	return nil, provider.ErrStreamingNotSupported
}

func TestChatServiceCallsProviderWithContext(t *testing.T) {
	type contextKey string
	const key contextKey = "request-id"

	called := false
	fake := stubProvider{chat: func(ctx context.Context, req *provider.ChatRequest) (*provider.ChatResponse, error) {
		called = true
		if got := ctx.Value(key); got != "request-1" {
			t.Fatalf("context value = %v", got)
		}
		return &provider.ChatResponse{ID: "ok"}, nil
	}}
	service := NewChatService(fake)
	ctx := context.WithValue(context.Background(), key, "request-1")

	resp, err := service.Chat(ctx, validRequest())
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if !called || resp.ID != "ok" {
		t.Fatalf("provider called = %v, response = %#v", called, resp)
	}
}

func TestChatServiceRejectsStreamingBeforeProvider(t *testing.T) {
	called := false
	fake := stubProvider{chat: func(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
		called = true
		return nil, nil
	}}
	service := NewChatService(fake)
	req := validRequest()
	req.Stream = true

	_, err := service.Chat(context.Background(), req)
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) || validationErr.Code != "streaming_not_supported" {
		t.Fatalf("Chat() error = %v, want streaming validation error", err)
	}
	if called {
		t.Fatal("provider was called for an invalid streaming request")
	}
}

func TestChatServiceValidatesMessages(t *testing.T) {
	tests := []struct {
		name string
		req  *provider.ChatRequest
		code string
	}{
		{name: "missing model", req: &provider.ChatRequest{Messages: validRequest().Messages}, code: "invalid_model"},
		{name: "missing messages", req: &provider.ChatRequest{Model: "mock-model"}, code: "invalid_messages"},
		{name: "unsupported role", req: &provider.ChatRequest{Model: "mock-model", Messages: []provider.ChatMessage{{Role: "tool", Content: "result"}}}, code: "invalid_message_role"},
		{name: "empty content", req: &provider.ChatRequest{Model: "mock-model", Messages: []provider.ChatMessage{{Role: "user"}}}, code: "invalid_message_content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := stubProvider{chat: func(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
				t.Fatal("provider called for invalid request")
				return nil, nil
			}}
			_, err := NewChatService(fake).Chat(context.Background(), tt.req)
			var validationErr *ValidationError
			if !errors.As(err, &validationErr) || validationErr.Code != tt.code {
				t.Fatalf("Chat() error = %v, want code %q", err, tt.code)
			}
		})
	}
}

func validRequest() *provider.ChatRequest {
	return &provider.ChatRequest{
		Model:    "mock-model",
		Messages: []provider.ChatMessage{{Role: "user", Content: "hello"}},
	}
}
