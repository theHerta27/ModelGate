package provider

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type MockProvider struct {
	sequence atomic.Uint64
}

func NewMockProvider() *MockProvider {
	return &MockProvider{}
}

func (p *MockProvider) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	content := mockContent(req.Messages)

	return &ChatResponse{
		ID:      fmt.Sprintf("chatcmpl-mock-%d", p.sequence.Add(1)),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []ChatChoice{
			{
				Index: 0,
				Message: ChatMessage{
					Role:    "assistant",
					Content: content,
				},
				FinishReason: "stop",
			},
		},
		Usage: ChatUsage{},
	}, nil
}

func (p *MockProvider) ChatStream(ctx context.Context, req *ChatRequest) (Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	content := mockContent(req.Messages)
	id := fmt.Sprintf("chatcmpl-mock-%d", p.sequence.Add(1))
	created := time.Now().Unix()
	chunks := []ChatStreamChunk{
		{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   req.Model,
			Choices: []ChatStreamChoice{{
				Index:        0,
				Delta:        ChatDelta{Role: "assistant"},
				FinishReason: nil,
			}},
		},
	}

	for _, part := range splitMockContent(content) {
		chunks = append(chunks, ChatStreamChunk{
			ID:      id,
			Object:  "chat.completion.chunk",
			Created: created,
			Model:   req.Model,
			Choices: []ChatStreamChoice{{
				Index:        0,
				Delta:        ChatDelta{Content: part},
				FinishReason: nil,
			}},
		})
	}

	finishReason := "stop"
	chunks = append(chunks, ChatStreamChunk{
		ID:      id,
		Object:  "chat.completion.chunk",
		Created: created,
		Model:   req.Model,
		Choices: []ChatStreamChoice{{
			Index:        0,
			Delta:        ChatDelta{},
			FinishReason: &finishReason,
		}},
	})

	return &mockStream{ctx: ctx, chunks: chunks}, nil
}

func mockContent(messages []ChatMessage) string {
	content := "ModelGate mock response"
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			return "Mock response: " + strings.TrimSpace(messages[i].Content)
		}
	}
	return content
}

func splitMockContent(content string) []string {
	words := strings.Fields(content)
	parts := make([]string, 0, len(words))
	for index, word := range words {
		if index > 0 {
			word = " " + word
		}
		parts = append(parts, word)
	}
	return parts
}

type mockStream struct {
	ctx    context.Context
	chunks []ChatStreamChunk
	mu     sync.Mutex
	index  int
	closed bool
}

func (s *mockStream) Recv() (*ChatStreamChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.index >= len(s.chunks) {
		return nil, io.EOF
	}
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	default:
	}

	chunk := s.chunks[s.index]
	s.index++
	return &chunk, nil
}

func (s *mockStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
