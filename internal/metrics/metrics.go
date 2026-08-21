package metrics

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/theHerta27/ModelGate/internal/circuitbreaker"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/retry"
)

const modelContextKey = "modelgate.model"

type Metrics struct {
	registry *prometheus.Registry

	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	tokens          *prometheus.CounterVec
	providerErrors  *prometheus.CounterVec
	rateLimited     prometheus.Counter
	cacheHits       prometheus.Counter
	upstream        *prometheus.CounterVec
	circuitState    *prometheus.GaugeVec
}

func New() *Metrics {
	metrics := &Metrics{
		registry: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "modelgate", Name: "requests_total",
			Help: "Total HTTP requests handled by ModelGate.",
		}, []string{"route", "status"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "modelgate", Name: "request_duration_seconds",
			Help:    "ModelGate HTTP request duration in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"route", "status"}),
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "modelgate", Name: "tokens_total",
			Help: "Total LLM tokens reported by upstream providers.",
		}, []string{"provider", "type"}),
		providerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "modelgate", Name: "provider_errors_total",
			Help: "Total failed upstream provider attempts.",
		}, []string{"provider", "code"}),
		rateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "modelgate", Name: "rate_limited_total",
			Help: "Total requests rejected by rate limiting.",
		}),
		cacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "modelgate", Name: "cache_hits_total",
			Help: "Total response-cache hits.",
		}),
		upstream: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "modelgate", Name: "upstream_requests_total",
			Help: "Total individual upstream provider attempts.",
		}, []string{"provider", "result"}),
		circuitState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "modelgate", Name: "circuit_breaker_state",
			Help: "Circuit breaker state: 0=closed, 1=open, 2=half_open.",
		}, []string{"provider"}),
	}
	metrics.registry.MustRegister(
		metrics.requests,
		metrics.requestDuration,
		metrics.tokens,
		metrics.providerErrors,
		metrics.rateLimited,
		metrics.cacheHits,
		metrics.upstream,
		metrics.circuitState,
	)
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry})
}

func (m *Metrics) HTTPMiddleware(logger *slog.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *gin.Context) {
		startedAt := time.Now()
		c.Next()
		if c.Request.URL.Path == "/metrics" {
			return
		}

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		status := strconv.Itoa(c.Writer.Status())
		duration := time.Since(startedAt)
		m.requests.WithLabelValues(route, status).Inc()
		m.requestDuration.WithLabelValues(route, status).Observe(duration.Seconds())

		level := slog.LevelInfo
		if c.Writer.Status() >= http.StatusInternalServerError {
			level = slog.LevelError
		}
		logger.LogAttrs(
			c.Request.Context(), level, "request completed",
			slog.String("method", c.Request.Method),
			slog.String("route", route),
			slog.Int("status", c.Writer.Status()),
			slog.Int64("latency_ms", duration.Milliseconds()),
			slog.String("request_id", c.Writer.Header().Get("X-Request-ID")),
			slog.String("provider", c.Writer.Header().Get("X-ModelGate-Provider")),
			slog.String("model", c.GetString(modelContextKey)),
		)
	}
}

func SetModel(c *gin.Context, model string) {
	c.Set(modelContextKey, model)
}

func (m *Metrics) ObserveRateLimited() {
	m.rateLimited.Inc()
}

func (m *Metrics) ObserveCacheHit() {
	m.cacheHits.Inc()
}

func (m *Metrics) ObserveTokens(providerName string, usage provider.ChatUsage) {
	if providerName == "" {
		providerName = "unknown"
	}
	m.tokens.WithLabelValues(providerName, "input").Add(float64(usage.PromptTokens))
	m.tokens.WithLabelValues(providerName, "output").Add(float64(usage.CompletionTokens))
	m.tokens.WithLabelValues(providerName, "total").Add(float64(usage.TotalTokens))
}

func (m *Metrics) ObserveUpstreamAttempt(providerName string, attempt retry.Attempt) {
	result := "success"
	if attempt.Err != nil {
		if provider.IsRetryable(attempt.Err) {
			result = "retryable_error"
		} else {
			result = "error"
		}
		m.providerErrors.WithLabelValues(providerName, provider.ErrorCode(attempt.Err)).Inc()
	}
	m.upstream.WithLabelValues(providerName, result).Inc()
}

func (m *Metrics) SetCircuitState(providerName string, state circuitbreaker.State) {
	m.circuitState.WithLabelValues(providerName).Set(float64(state))
}
