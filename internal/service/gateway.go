package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/theHerta27/ModelGate/internal/cache"
	"github.com/theHerta27/ModelGate/internal/idempotency"
	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/ratelimit"
	"github.com/theHerta27/ModelGate/internal/storage"
)

type GatewayOptions struct {
	Limiter           ratelimit.Limiter
	RateLimitFailOpen bool
	Idempotency       idempotency.Store
	Cache             cache.Store
	CachePolicy       cache.Policy
	Recorder          storage.Recorder
	ProviderName      string
	RequestTimeout    time.Duration
	OperationTimeout  time.Duration
	ErrorSink         func(error)
	Observer          GatewayObserver
}

type GatewayObserver interface {
	ObserveRateLimited()
	ObserveCacheHit()
	ObserveTokens(string, provider.ChatUsage)
}

type noopGatewayObserver struct{}

func (noopGatewayObserver) ObserveRateLimited()                      {}
func (noopGatewayObserver) ObserveCacheHit()                         {}
func (noopGatewayObserver) ObserveTokens(string, provider.ChatUsage) {}

type GatewayService struct {
	chat              *ChatService
	limiter           ratelimit.Limiter
	rateLimitFailOpen bool
	idempotency       idempotency.Store
	cache             cache.Store
	cachePolicy       cache.Policy
	recorder          storage.Recorder
	providerName      string
	requestTimeout    time.Duration
	operationTimeout  time.Duration
	errorSink         func(error)
	observer          GatewayObserver
}

type RequestMetadata struct {
	ClientIdentity string
	IdempotencyKey string
	RequestID      string
}

type ChatResult struct {
	Response            *provider.ChatResponse
	RequestID           string
	Provider            string
	CacheStatus         string
	IdempotencyReplayed bool
	RateLimit           ratelimit.Decision
}

type StreamResult struct {
	Stream    provider.Stream
	RequestID string
	Provider  string
	RateLimit ratelimit.Decision
}

func NewGatewayService(chat *ChatService, options GatewayOptions) *GatewayService {
	if options.ProviderName == "" {
		options.ProviderName = "unknown"
	}
	if options.OperationTimeout <= 0 {
		options.OperationTimeout = 2 * time.Second
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = 30 * time.Second
	}
	if options.ErrorSink == nil {
		options.ErrorSink = func(error) {}
	}
	if options.Observer == nil {
		options.Observer = noopGatewayObserver{}
	}
	return &GatewayService{
		chat:              chat,
		limiter:           options.Limiter,
		rateLimitFailOpen: options.RateLimitFailOpen,
		idempotency:       options.Idempotency,
		cache:             options.Cache,
		cachePolicy:       options.CachePolicy,
		recorder:          options.Recorder,
		providerName:      options.ProviderName,
		requestTimeout:    options.RequestTimeout,
		operationTimeout:  options.OperationTimeout,
		errorSink:         options.ErrorSink,
		observer:          options.Observer,
	}
}

func (g *GatewayService) Chat(
	ctx context.Context,
	req *provider.ChatRequest,
	metadata RequestMetadata,
) (*ChatResult, error) {
	ctx, cancel := context.WithTimeout(ctx, g.requestTimeout)
	defer cancel()

	if err := ValidateChatRequest(req); err != nil {
		return nil, err
	}
	if req.Stream {
		return nil, invalid("invalid_stream_mode", "streaming requests must use ChatStream")
	}
	idempotencyKey, err := normalizeIdempotencyKey(metadata.IdempotencyKey)
	if err != nil {
		return nil, err
	}

	requestID := strings.TrimSpace(metadata.RequestID)
	if requestID == "" {
		generatedRequestID, err := NewRequestID()
		if err != nil {
			return nil, &DependencyError{Code: "request_id_unavailable", Cause: err}
		}
		requestID = generatedRequestID
	}
	startedAt := time.Now()
	rateDecision, err := g.enforceRateLimit(ctx, metadata.ClientIdentity)
	if err != nil {
		g.record(ctx, storage.RequestRecord{
			RequestID: requestID, Provider: g.providerName, Model: req.Model,
			Status: statusForGatewayError(err), Latency: time.Since(startedAt),
			CacheStatus: cache.StatusBypass, ErrorCode: gatewayErrorCode(err),
		})
		return nil, err
	}

	var fingerprint string
	var scopedIdempotencyKey string
	acquiredIdempotency := false
	if idempotencyKey != "" {
		if g.idempotency == nil {
			return nil, &DependencyError{Code: "idempotency_unavailable"}
		}
		fingerprint, err = requestFingerprint(req)
		if err != nil {
			return nil, &DependencyError{Code: "request_fingerprint_failed", Cause: err}
		}
		scopedIdempotencyKey = idempotency.ScopedKey(metadata.ClientIdentity, idempotencyKey)
		begin, err := g.idempotency.Begin(ctx, scopedIdempotencyKey, fingerprint)
		if err != nil {
			return nil, &DependencyError{Code: "idempotency_unavailable", Cause: err}
		}
		switch begin.Status {
		case idempotency.StatusAcquired:
			acquiredIdempotency = true
		case idempotency.StatusPending:
			err := &IdempotencyError{Code: "idempotency_request_in_progress"}
			g.recordRejected(ctx, requestID, req.Model, startedAt, err)
			return nil, err
		case idempotency.StatusConflict:
			err := &IdempotencyError{Code: "idempotency_key_conflict"}
			g.recordRejected(ctx, requestID, req.Model, startedAt, err)
			return nil, err
		case idempotency.StatusCompleted:
			var replay provider.ChatResponse
			if err := json.Unmarshal(begin.Response, &replay); err != nil {
				return nil, &DependencyError{Code: "idempotency_result_corrupt", Cause: err}
			}
			g.record(ctx, storage.RequestRecord{
				RequestID: requestID, Provider: "idempotency", Model: req.Model,
				Status: storage.StatusIdempotencyReplay, Latency: time.Since(startedAt),
				CacheStatus: cache.StatusBypass,
			})
			return &ChatResult{
				Response: &replay, RequestID: requestID, CacheStatus: cache.StatusBypass,
				Provider: "idempotency", IdempotencyReplayed: true, RateLimit: rateDecision,
			}, nil
		}
	}

	cacheKey, cacheable, err := g.cachePolicy.Key(metadata.ClientIdentity, req)
	if err != nil {
		g.releaseIdempotency(scopedIdempotencyKey, fingerprint, acquiredIdempotency)
		return nil, &DependencyError{Code: "cache_fingerprint_failed", Cause: err}
	}
	cacheStatus := cache.StatusBypass
	if cacheable && g.cache != nil {
		cacheStatus = cache.StatusMiss
		payload, found, err := g.cache.Get(ctx, cacheKey)
		if err != nil {
			g.errorSink(err)
		} else if found {
			var cached provider.ChatResponse
			if err := json.Unmarshal(payload, &cached); err != nil {
				g.errorSink(fmt.Errorf("decode cached response: %w", err))
			} else {
				cacheStatus = cache.StatusHit
				g.observer.ObserveCacheHit()
				g.completeIdempotency(scopedIdempotencyKey, fingerprint, payload, acquiredIdempotency)
				g.record(ctx, storage.RequestRecord{
					RequestID: requestID, Provider: "cache", Model: req.Model,
					Status: storage.StatusCacheHit, Latency: time.Since(startedAt),
					CacheStatus: cacheStatus,
				})
				return &ChatResult{
					Response: &cached, RequestID: requestID, CacheStatus: cacheStatus,
					Provider: "cache", RateLimit: rateDecision,
				}, nil
			}
		}
	}

	response, err := g.chat.Chat(ctx, req)
	if err != nil {
		selectedProvider := providerNameFromError(err, g.providerName)
		g.releaseIdempotency(scopedIdempotencyKey, fingerprint, acquiredIdempotency)
		g.record(ctx, storage.RequestRecord{
			RequestID: requestID, Provider: selectedProvider, Model: req.Model,
			Status: storage.StatusFailed, Latency: time.Since(startedAt),
			CacheStatus: cacheStatus, ErrorCode: gatewayErrorCode(err),
		})
		return nil, err
	}
	selectedProvider := response.Provider
	if selectedProvider == "" {
		selectedProvider = g.providerName
	}

	payload, marshalErr := json.Marshal(response)
	if marshalErr != nil {
		g.errorSink(fmt.Errorf("encode response for Redis: %w", marshalErr))
	} else {
		if cacheable && g.cache != nil {
			cacheCtx, cancel := context.WithTimeout(context.Background(), g.operationTimeout)
			if err := g.cache.Set(cacheCtx, cacheKey, payload); err != nil {
				g.errorSink(err)
			}
			cancel()
		}
		g.completeIdempotency(scopedIdempotencyKey, fingerprint, payload, acquiredIdempotency)
	}

	g.record(ctx, storage.RequestRecord{
		RequestID: requestID, Provider: selectedProvider, Model: req.Model,
		Status: storage.StatusSucceeded, Latency: time.Since(startedAt),
		InputTokens: response.Usage.PromptTokens, OutputTokens: response.Usage.CompletionTokens,
		TotalTokens: response.Usage.TotalTokens, CacheStatus: cacheStatus,
	})
	g.observer.ObserveTokens(selectedProvider, response.Usage)
	return &ChatResult{
		Response: response, RequestID: requestID, CacheStatus: cacheStatus,
		Provider: selectedProvider, RateLimit: rateDecision,
	}, nil
}

func (g *GatewayService) ChatStream(
	ctx context.Context,
	req *provider.ChatRequest,
	metadata RequestMetadata,
) (*StreamResult, error) {
	streamCtx, cancel := context.WithTimeout(ctx, g.requestTimeout)

	if err := ValidateChatRequest(req); err != nil {
		cancel()
		return nil, err
	}
	if !req.Stream {
		cancel()
		return nil, invalid("invalid_stream_mode", "stream must be true for ChatStream")
	}
	if strings.TrimSpace(metadata.IdempotencyKey) != "" {
		cancel()
		return nil, invalid(
			"idempotency_not_supported_for_stream",
			"Idempotency-Key is only supported for non-streaming requests",
		)
	}
	requestID := strings.TrimSpace(metadata.RequestID)
	if requestID == "" {
		generatedRequestID, err := NewRequestID()
		if err != nil {
			cancel()
			return nil, &DependencyError{Code: "request_id_unavailable", Cause: err}
		}
		requestID = generatedRequestID
	}
	startedAt := time.Now()
	rateDecision, err := g.enforceRateLimit(streamCtx, metadata.ClientIdentity)
	if err != nil {
		g.record(streamCtx, storage.RequestRecord{
			RequestID: requestID, Provider: g.providerName, Model: req.Model,
			Status: statusForGatewayError(err), Latency: time.Since(startedAt),
			CacheStatus: cache.StatusBypass, ErrorCode: gatewayErrorCode(err),
		})
		cancel()
		return nil, err
	}
	stream, err := g.chat.ChatStream(streamCtx, req)
	if err != nil {
		selectedProvider := providerNameFromError(err, g.providerName)
		g.record(streamCtx, storage.RequestRecord{
			RequestID: requestID, Provider: selectedProvider, Model: req.Model,
			Status: storage.StatusFailed, Latency: time.Since(startedAt),
			CacheStatus: cache.StatusBypass, ErrorCode: gatewayErrorCode(err),
		})
		cancel()
		return nil, err
	}
	selectedProvider := provider.StreamProviderName(stream)
	if selectedProvider == "unknown" {
		selectedProvider = g.providerName
	}

	metered := &meteredStream{
		inner:  stream,
		cancel: cancel,
		finish: func(status, errorCode string, usage provider.ChatUsage) {
			g.record(ctx, storage.RequestRecord{
				RequestID: requestID, Provider: selectedProvider, Model: req.Model,
				Status: status, Latency: time.Since(startedAt),
				InputTokens: usage.PromptTokens, OutputTokens: usage.CompletionTokens,
				TotalTokens: usage.TotalTokens, CacheStatus: cache.StatusBypass,
				ErrorCode: errorCode,
			})
			g.observer.ObserveTokens(selectedProvider, usage)
		},
	}
	return &StreamResult{
		Stream: metered, RequestID: requestID, Provider: selectedProvider, RateLimit: rateDecision,
	}, nil
}

func (g *GatewayService) enforceRateLimit(
	ctx context.Context,
	identity string,
) (ratelimit.Decision, error) {
	if g.limiter == nil {
		return ratelimit.Decision{Allowed: true}, nil
	}
	decision, err := g.limiter.Allow(ctx, identity)
	if err != nil {
		g.errorSink(err)
		if g.rateLimitFailOpen {
			return ratelimit.Decision{Allowed: true}, nil
		}
		return ratelimit.Decision{}, &DependencyError{Code: "rate_limiter_unavailable", Cause: err}
	}
	if !decision.Allowed {
		g.observer.ObserveRateLimited()
		return decision, &RateLimitError{Decision: decision}
	}
	return decision, nil
}

func (g *GatewayService) record(ctx context.Context, record storage.RequestRecord) {
	if g.recorder == nil {
		return
	}
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.operationTimeout)
	defer cancel()
	if err := g.recorder.Record(recordCtx, record); err != nil {
		g.errorSink(fmt.Errorf("record request usage: %w", err))
	}
}

func (g *GatewayService) recordRejected(
	ctx context.Context,
	requestID, model string,
	startedAt time.Time,
	err error,
) {
	g.record(ctx, storage.RequestRecord{
		RequestID: requestID, Provider: g.providerName, Model: model,
		Status: storage.StatusRejected, Latency: time.Since(startedAt),
		CacheStatus: cache.StatusBypass, ErrorCode: gatewayErrorCode(err),
	})
}

func (g *GatewayService) completeIdempotency(
	key, fingerprint string,
	payload []byte,
	acquired bool,
) {
	if !acquired {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.operationTimeout)
	defer cancel()
	if err := g.idempotency.Complete(ctx, key, fingerprint, payload); err != nil {
		g.errorSink(err)
	}
}

func (g *GatewayService) releaseIdempotency(key, fingerprint string, acquired bool) {
	if !acquired {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.operationTimeout)
	defer cancel()
	if err := g.idempotency.Release(ctx, key, fingerprint); err != nil {
		g.errorSink(err)
	}
}

func normalizeIdempotencyKey(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if len(value) > 128 {
		return "", invalid("invalid_idempotency_key", "Idempotency-Key must not exceed 128 bytes")
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') &&
			!(character >= 'A' && character <= 'Z') &&
			!(character >= '0' && character <= '9') &&
			!strings.ContainsRune("._:-", character) {
			return "", invalid(
				"invalid_idempotency_key",
				"Idempotency-Key contains unsupported characters",
			)
		}
	}
	return value, nil
}

func requestFingerprint(req *provider.ChatRequest) (string, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("encode request fingerprint: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func NewRequestID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate request id: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		hex.EncodeToString(value[0:4]),
		hex.EncodeToString(value[4:6]),
		hex.EncodeToString(value[6:8]),
		hex.EncodeToString(value[8:10]),
		hex.EncodeToString(value[10:16]),
	), nil
}

type RateLimitError struct {
	Decision ratelimit.Decision
}

func (e *RateLimitError) Error() string {
	return "rate limit exceeded"
}

type IdempotencyError struct {
	Code string
}

func (e *IdempotencyError) Error() string {
	return e.Code
}

type DependencyError struct {
	Code  string
	Cause error
}

func (e *DependencyError) Error() string {
	return e.Code
}

func (e *DependencyError) Unwrap() error {
	return e.Cause
}

func statusForGatewayError(err error) string {
	var rateLimitErr *RateLimitError
	if errors.As(err, &rateLimitErr) {
		return storage.StatusRateLimited
	}
	return storage.StatusFailed
}

func gatewayErrorCode(err error) string {
	var validationErr *ValidationError
	if errors.As(err, &validationErr) {
		return validationErr.Code
	}
	var idempotencyErr *IdempotencyError
	if errors.As(err, &idempotencyErr) {
		return idempotencyErr.Code
	}
	var dependencyErr *DependencyError
	if errors.As(err, &dependencyErr) {
		return dependencyErr.Code
	}
	var rateLimitErr *RateLimitError
	if errors.As(err, &rateLimitErr) {
		return "rate_limit_exceeded"
	}
	var upstreamErr *provider.UpstreamError
	if errors.As(err, &upstreamErr) {
		return provider.ErrorCode(err)
	}
	if errors.Is(err, context.Canceled) {
		return "request_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "upstream_timeout"
	}
	return "provider_request_failed"
}

func providerNameFromError(err error, fallback string) string {
	var upstreamErr *provider.UpstreamError
	if errors.As(err, &upstreamErr) && upstreamErr.Provider != "" {
		return upstreamErr.Provider
	}
	return fallback
}

type meteredStream struct {
	inner  provider.Stream
	cancel context.CancelFunc
	finish func(status, errorCode string, usage provider.ChatUsage)
	once   sync.Once
	usage  provider.ChatUsage
}

func (s *meteredStream) Recv() (*provider.ChatStreamChunk, error) {
	chunk, err := s.inner.Recv()
	if chunk != nil && chunk.Usage != nil {
		s.usage = *chunk.Usage
	}
	switch {
	case errors.Is(err, io.EOF):
		s.finishOnce(storage.StatusSucceeded, "")
	case err != nil:
		s.finishOnce(storage.StatusFailed, gatewayErrorCode(err))
	}
	return chunk, err
}

func (s *meteredStream) Close() error {
	err := s.inner.Close()
	errorCode := "stream_closed"
	if err != nil {
		errorCode = "stream_close_failed"
	}
	s.finishOnce(storage.StatusFailed, errorCode)
	return err
}

func (s *meteredStream) finishOnce(status, errorCode string) {
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.finish(status, errorCode, s.usage)
	})
}
