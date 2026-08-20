package provider

import (
	"context"
	"errors"
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

func TestMockProviderStreamingIsDeferred(t *testing.T) {
	_, err := NewMockProvider().ChatStream(context.Background(), &ChatRequest{})
	if !errors.Is(err, ErrStreamingNotSupported) {
		t.Fatalf("ChatStream() error = %v, want ErrStreamingNotSupported", err)
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
