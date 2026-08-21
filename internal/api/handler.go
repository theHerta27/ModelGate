package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/service"
)

const maxRequestBodyBytes = 1 << 20

type Handler struct {
	chatService *service.ChatService
}

type errorEnvelope struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

func NewHandler(chatService *service.ChatService) *Handler {
	return &Handler{chatService: chatService}
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) ChatCompletions(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBodyBytes)

	var req provider.ChatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(
			c,
			http.StatusBadRequest,
			"invalid_request_error",
			"invalid_json",
			"request body must be valid JSON and no larger than 1 MiB",
		)
		return
	}
	if req.Stream {
		h.streamChatCompletions(c, &req)
		return
	}

	resp, err := h.chatService.Chat(c.Request.Context(), &req)
	if err == nil {
		c.JSON(http.StatusOK, resp)
		return
	}
	writeMappedError(c, err)
}

func (h *Handler) streamChatCompletions(c *gin.Context, req *provider.ChatRequest) {
	stream, err := h.chatService.ChatStream(c.Request.Context(), req)
	if err != nil {
		writeMappedError(c, err)
		return
	}
	defer stream.Close()

	firstChunk, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("provider stream ended before the first chunk")
		}
		writeMappedError(c, err)
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)

	if err := writeSSEChunk(c, firstChunk); err != nil {
		return
	}

	for {
		chunk, err := stream.Recv()
		switch {
		case err == nil:
			if err := writeSSEChunk(c, chunk); err != nil {
				return
			}
		case errors.Is(err, io.EOF):
			_, _ = c.Writer.WriteString("data: [DONE]\n\n")
			c.Writer.Flush()
			return
		default:
			return
		}
	}
}

func writeSSEChunk(c *gin.Context, chunk *provider.ChatStreamChunk) error {
	payload, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("encode stream chunk: %w", err)
	}
	if _, err := c.Writer.WriteString("data: " + string(payload) + "\n\n"); err != nil {
		return fmt.Errorf("write stream chunk: %w", err)
	}
	c.Writer.Flush()
	return nil
}

func writeMappedError(c *gin.Context, err error) {
	var validationErr *service.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeError(
			c,
			http.StatusBadRequest,
			"invalid_request_error",
			validationErr.Code,
			validationErr.Message,
		)
	case errors.Is(err, context.DeadlineExceeded):
		writeError(
			c,
			http.StatusGatewayTimeout,
			"upstream_error",
			"upstream_timeout",
			"provider request timed out",
		)
	case errors.Is(err, context.Canceled):
		writeError(
			c,
			http.StatusRequestTimeout,
			"request_error",
			"request_canceled",
			"request was canceled",
		)
	default:
		writeError(
			c,
			http.StatusBadGateway,
			"upstream_error",
			"provider_request_failed",
			"provider request failed",
		)
	}
}

func writeError(c *gin.Context, status int, errorType, code, message string) {
	c.JSON(status, errorEnvelope{
		Error: errorDetail{
			Message: message,
			Type:    errorType,
			Code:    code,
		},
	})
}
