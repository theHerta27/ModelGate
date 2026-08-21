package metrics

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/retry"
)

func TestMetricsObservers(t *testing.T) {
	metrics := New()
	metrics.ObserveRateLimited()
	metrics.ObserveCacheHit()
	metrics.ObserveTokens("mock", provider.ChatUsage{
		PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5,
	})
	metrics.ObserveUpstreamAttempt("mock", retry.Attempt{})
	metrics.ObserveUpstreamAttempt("mock", retry.Attempt{Err: &provider.UpstreamError{
		Provider: "mock", StatusCode: 503, Retryable: true,
	}})
	metrics.SetCircuitState("mock", circuitbreaker.StateOpen)

	if got := testutil.ToFloat64(metrics.rateLimited); got != 1 {
		t.Fatalf("rate limited = %v", got)
	}
	if got := testutil.ToFloat64(metrics.cacheHits); got != 1 {
		t.Fatalf("cache hits = %v", got)
	}
	if got := testutil.ToFloat64(metrics.tokens.WithLabelValues("mock", "total")); got != 5 {
		t.Fatalf("total tokens = %v", got)
	}
	if got := testutil.ToFloat64(metrics.upstream.WithLabelValues("mock", "success")); got != 1 {
		t.Fatalf("successful upstream attempts = %v", got)
	}
	if got := testutil.ToFloat64(metrics.providerErrors.WithLabelValues("mock", "provider_unavailable")); got != 1 {
		t.Fatalf("provider errors = %v", got)
	}
	if got := testutil.ToFloat64(metrics.circuitState.WithLabelValues("mock")); got != float64(circuitbreaker.StateOpen) {
		t.Fatalf("circuit state = %v", got)
	}
}

func TestHTTPMiddlewareMetricsAndStructuredLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics := New()
	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuffer, nil))
	router := gin.New()
	router.Use(metrics.HTTPMiddleware(logger))
	router.GET("/test", func(c *gin.Context) {
		SetModel(c, "model")
		c.Header("X-Request-ID", "request-1")
		c.Header("X-ModelGate-Provider", "mock")
		c.Status(http.StatusCreated)
	})

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/test", nil))
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d", response.Code)
	}
	if got := testutil.ToFloat64(metrics.requests.WithLabelValues("/test", "201")); got != 1 {
		t.Fatalf("requests = %v", got)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logBuffer.Bytes()), &record); err != nil {
		t.Fatalf("decode structured log: %v", err)
	}
	for key, value := range map[string]string{
		"request_id": "request-1", "provider": "mock", "model": "model", "route": "/test",
	} {
		if record[key] != value {
			t.Fatalf("log %s = %#v, want %q", key, record[key], value)
		}
	}
}

func TestMetricsHandlerExposesRequiredFamilies(t *testing.T) {
	metrics := New()
	metrics.requests.WithLabelValues("/test", "200").Inc()
	metrics.requestDuration.WithLabelValues("/test", "200").Observe(0.01)
	metrics.ObserveTokens("mock", provider.ChatUsage{TotalTokens: 1})
	metrics.ObserveUpstreamAttempt("mock", retry.Attempt{Err: &provider.UpstreamError{
		Provider: "mock", StatusCode: 503, Retryable: true,
	}})
	metrics.ObserveRateLimited()
	metrics.ObserveCacheHit()
	metrics.SetCircuitState("mock", circuitbreaker.StateClosed)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	for _, name := range []string{
		"modelgate_requests_total",
		"modelgate_request_duration_seconds",
		"modelgate_tokens_total",
		"modelgate_provider_errors_total",
		"modelgate_rate_limited_total",
		"modelgate_cache_hits_total",
		"modelgate_upstream_requests_total",
		"modelgate_circuit_breaker_state",
	} {
		if !strings.Contains(response.Body.String(), name) {
			t.Fatalf("metrics output does not contain %s", name)
		}
	}
}
