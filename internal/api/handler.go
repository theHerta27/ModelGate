package api

import (
	"context"
	"errors"
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

	resp, err := h.chatService.Chat(c.Request.Context(), &req)
	if err == nil {
		c.JSON(http.StatusOK, resp)
		return
	}

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
