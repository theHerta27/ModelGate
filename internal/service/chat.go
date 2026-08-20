package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/theHerta27/ModelGate/internal/provider"
)

type ValidationError struct {
	Code    string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

type ChatService struct {
	provider provider.Provider
}

func NewChatService(chatProvider provider.Provider) *ChatService {
	return &ChatService{provider: chatProvider}
}

func (s *ChatService) Chat(
	ctx context.Context,
	req *provider.ChatRequest,
) (*provider.ChatResponse, error) {
	if err := validateChatRequest(req); err != nil {
		return nil, err
	}
	return s.provider.Chat(ctx, req)
}

func validateChatRequest(req *provider.ChatRequest) error {
	if req == nil {
		return invalid("invalid_request", "request body is required")
	}
	if strings.TrimSpace(req.Model) == "" {
		return invalid("invalid_model", "model is required")
	}
	if len(req.Messages) == 0 {
		return invalid("invalid_messages", "messages must contain at least one item")
	}

	validRoles := map[string]struct{}{
		"developer": {},
		"system":    {},
		"user":      {},
		"assistant": {},
	}
	for index, message := range req.Messages {
		if _, ok := validRoles[message.Role]; !ok {
			return invalid(
				"invalid_message_role",
				fmt.Sprintf("messages[%d].role is not supported in V1", index),
			)
		}
		if strings.TrimSpace(message.Content) == "" {
			return invalid(
				"invalid_message_content",
				fmt.Sprintf("messages[%d].content is required", index),
			)
		}
	}

	if req.Temperature != nil && (*req.Temperature < 0 || *req.Temperature > 2) {
		return invalid("invalid_temperature", "temperature must be between 0 and 2")
	}
	if req.TopP != nil && (*req.TopP < 0 || *req.TopP > 1) {
		return invalid("invalid_top_p", "top_p must be between 0 and 1")
	}
	if req.MaxTokens != nil && *req.MaxTokens <= 0 {
		return invalid("invalid_max_tokens", "max_tokens must be greater than zero")
	}
	if req.Stream {
		return invalid("streaming_not_supported", "streaming is planned for V1.5")
	}

	return nil
}

func invalid(code, message string) *ValidationError {
	return &ValidationError{Code: code, Message: message}
}
