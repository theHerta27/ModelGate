package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/service"
)

const maxRequestBodyBytes = 1 << 20

type Handler struct {
	gateway *service.GatewayService
}

type errorEnvelope struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

func NewHandler(gateway *service.GatewayService) *Handler {
	return &Handler{gateway: gateway}
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

	result, err := h.gateway.Chat(c.Request.Context(), &req, requestMetadata(c))
	if err == nil {
		applyResultHeaders(c, result)
		c.JSON(http.StatusOK, result.Response)
		return
	}
	writeMappedError(c, err)
}

func (h *Handler) streamChatCompletions(c *gin.Context, req *provider.ChatRequest) {
	result, err := h.gateway.ChatStream(c.Request.Context(), req, requestMetadata(c))
	if err != nil {
		writeMappedError(c, err)
		return
	}
	defer result.Stream.Close()
	applyRateLimitHeaders(c, result.RateLimit)
	c.Header("X-Request-ID", result.RequestID)

	firstChunk, err := result.Stream.Recv()
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
		chunk, err := result.Stream.Recv()
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
	var rateLimitErr *service.RateLimitError
	var idempotencyErr *service.IdempotencyError
	var dependencyErr *service.DependencyError
	switch {
	case errors.As(err, &validationErr):
		writeError(
			c,
			http.StatusBadRequest,
			"invalid_request_error",
			validationErr.Code,
			validationErr.Message,
		)
	case errors.As(err, &rateLimitErr):
		applyRateLimitHeaders(c, rateLimitErr.Decision)
		retryAfter := max(1, int64((rateLimitErr.Decision.RetryAfter+time.Second-1)/time.Second))
		c.Header("Retry-After", strconv.FormatInt(retryAfter, 10))
		writeError(
			c,
			http.StatusTooManyRequests,
			"rate_limit_error",
			"rate_limit_exceeded",
			"request rate limit exceeded",
		)
	case errors.As(err, &idempotencyErr):
		message := "a request with this Idempotency-Key is already in progress"
		if idempotencyErr.Code == "idempotency_key_conflict" {
			message = "Idempotency-Key was already used with a different request"
		}
		c.Header("Retry-After", "1")
		writeError(
			c,
			http.StatusConflict,
			"invalid_request_error",
			idempotencyErr.Code,
			message,
		)
	case errors.As(err, &dependencyErr):
		writeError(
			c,
			http.StatusServiceUnavailable,
			"service_unavailable_error",
			dependencyErr.Code,
			"a required gateway dependency is unavailable",
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

func requestMetadata(c *gin.Context) service.RequestMetadata {
	identity := c.Request.RemoteAddr
	if host, _, err := net.SplitHostPort(identity); err == nil {
		identity = host
	}
	if strings.TrimSpace(identity) == "" {
		identity = "unknown"
	}
	return service.RequestMetadata{
		ClientIdentity: identity,
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
	}
}

func applyResultHeaders(c *gin.Context, result *service.ChatResult) {
	c.Header("X-Request-ID", result.RequestID)
	c.Header("X-ModelGate-Cache", result.CacheStatus)
	if result.IdempotencyReplayed {
		c.Header("Idempotency-Replayed", "true")
	}
	applyRateLimitHeaders(c, result.RateLimit)
}

func applyRateLimitHeaders(c *gin.Context, decision ratelimit.Decision) {
	if decision.Limit <= 0 {
		return
	}
	c.Header("X-RateLimit-Limit", strconv.Itoa(decision.Limit))
	c.Header("X-RateLimit-Remaining", strconv.Itoa(max(0, decision.Remaining)))
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
