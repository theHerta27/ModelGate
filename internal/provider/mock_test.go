package provider

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
)

func TestMockProviderChat(t *testing.T) {
	p := NewMockProvider()
	req := &ChatRequest{
		Model: "mock-model",
		Messages: []ChatMessage{
			{Role: "system", Content: "Be concise."},
			{Role: "user", Content: "hello"},
		},
	}

	resp, err := p.Chat(context.Background(), req)
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if !strings.HasPrefix(resp.ID, "chatcmpl-mock-") {
		t.Fatalf("response ID = %q, want mock prefix", resp.ID)
	}
	if resp.Model != req.Model {
		t.Fatalf("response model = %q, want %q", resp.Model, req.Model)
	}
	if got := resp.Choices[0].Message.Content; got != "Mock response: hello" {
		t.Fatalf("response content = %q", got)
	}
}

func TestMockProviderHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewMockProvider().Chat(ctx, &ChatRequest{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Chat() error = %v, want context.Canceled", err)
	}
}

func TestMockProviderChatStream(t *testing.T) {
	p := NewMockProvider()
	req := &ChatRequest{
		Model:    "mock-model",
		Messages: []ChatMessage{{Role: "user", Content: "hello stream"}},
		Stream:   true,
	}

	stream, err := p.ChatStream(context.Background(), req)
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}

	var content string
	var role string
	var finishReason string
	ids := make(map[string]struct{})
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv() error = %v", err)
		}
		ids[chunk.ID] = struct{}{}
		for _, choice := range chunk.Choices {
			if choice.Delta.Role != "" {
				role = choice.Delta.Role
			}
			content += choice.Delta.Content
			if choice.FinishReason != nil {
				finishReason = *choice.FinishReason
			}
		}
	}

	if role != "assistant" {
		t.Fatalf("stream role = %q", role)
	}
	if content != "Mock response: hello stream" {
		t.Fatalf("stream content = %q", content)
	}
	if finishReason != "stop" {
		t.Fatalf("finish reason = %q", finishReason)
	}
	if len(ids) != 1 {
		t.Fatalf("stream IDs = %d, want 1", len(ids))
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv() after Close error = %v, want io.EOF", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestMockProviderStreamHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := NewMockProvider().ChatStream(ctx, &ChatRequest{Model: "mock-model"})
	if err != nil {
		t.Fatalf("ChatStream() error = %v", err)
	}
	cancel()

	if _, err := stream.Recv(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recv() error = %v, want context.Canceled", err)
	}
}

func TestMockProviderGeneratesUniqueIDsConcurrently(t *testing.T) {
	const requests = 100

	p := NewMockProvider()
	req := &ChatRequest{
		Model:    "mock-model",
		Messages: []ChatMessage{{Role: "user", Content: "hello"}},
	}

	ids := make(chan string, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := p.Chat(context.Background(), req)
			if err != nil {
				t.Errorf("Chat() error = %v", err)
				return
			}
			ids <- resp.ID
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, requests)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate response ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != requests {
		t.Fatalf("unique IDs = %d, want %d", len(seen), requests)
	}
}
