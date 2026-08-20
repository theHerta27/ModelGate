package provider

import (
	"context"
	"fmt"
	"strings"
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

	content := "ModelGate mock response"
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			content = "Mock response: " + strings.TrimSpace(req.Messages[i].Content)
			break
		}
	}

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

func (p *MockProvider) ChatStream(context.Context, *ChatRequest) (Stream, error) {
	return nil, ErrStreamingNotSupported
}
