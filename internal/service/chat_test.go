package service

import (
	"context"
	"errors"
	"testing"

	"github.com/theHerta27/ModelGate/internal/provider"
)

type stubProvider struct {
	chat       func(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error)
	chatStream func(context.Context, *provider.ChatRequest) (provider.Stream, error)
}

func (p stubProvider) Chat(
	ctx context.Context,
	req *provider.ChatRequest,
) (*provider.ChatResponse, error) {
	return p.chat(ctx, req)
}

func (p stubProvider) ChatStream(
	ctx context.Context,
	req *provider.ChatRequest,
) (provider.Stream, error) {
	if p.chatStream == nil {
		return nil, errors.New("unexpected ChatStream call")
	}
	return p.chatStream(ctx, req)
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

func TestChatServiceRejectsStreamingInNonStreamingMethod(t *testing.T) {
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
	if !errors.As(err, &validationErr) || validationErr.Code != "invalid_stream_mode" {
		t.Fatalf("Chat() error = %v, want streaming validation error", err)
	}
	if called {
		t.Fatal("provider was called for an invalid streaming request")
	}
}

func TestChatServiceCallsStreamingProviderWithContext(t *testing.T) {
	type contextKey string
	const key contextKey = "request-id"

	expectedStream := &stubStream{}
	fake := stubProvider{
		chat: func(context.Context, *provider.ChatRequest) (*provider.ChatResponse, error) {
			t.Fatal("non-streaming provider method was called")
			return nil, nil
		},
		chatStream: func(ctx context.Context, req *provider.ChatRequest) (provider.Stream, error) {
			if got := ctx.Value(key); got != "stream-1" {
				t.Fatalf("context value = %v", got)
			}
			if !req.Stream {
				t.Fatal("stream request flag = false")
			}
			return expectedStream, nil
		},
	}
	service := NewChatService(fake)
	req := validRequest()
	req.Stream = true
	ctx := context.WithValue(context.Background(), key, "stream-1")

	stream, err := service.ChatStream(ctx, req)
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	if stream != expectedStream {
		t.Fatalf("stream = %T, want expected stub", stream)
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

type stubStream struct{}

func (*stubStream) Recv() (*provider.ChatStreamChunk, error) {
	return nil, errors.New("not implemented")
}

func (*stubStream) Close() error {
	return nil
}
