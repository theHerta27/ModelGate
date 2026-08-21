package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
)

type ErrorKind string

const (
	ErrorKindHTTPStatus      ErrorKind = "http_status"
	ErrorKindNetwork         ErrorKind = "network"
	ErrorKindInvalidResponse ErrorKind = "invalid_response"
	ErrorKindUnavailable     ErrorKind = "unavailable"
)

// UpstreamError preserves machine-readable provider failure semantics across
// retry, circuit-breaker, HTTP mapping, metrics, and structured logging.
type UpstreamError struct {
	Provider   string
	Operation  string
	Kind       ErrorKind
	StatusCode int
	Retryable  bool
	Cause      error
}

func (e *UpstreamError) Error() string {
	switch {
	case e == nil:
		return "<nil>"
	case e.StatusCode > 0:
		return fmt.Sprintf("%s provider returned HTTP %d", e.Provider, e.StatusCode)
	case e.Cause != nil:
		return fmt.Sprintf("%s %s failed: %v", e.Provider, e.Operation, e.Cause)
	default:
		return fmt.Sprintf("%s %s failed", e.Provider, e.Operation)
	}
}

func (e *UpstreamError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func IsRetryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var upstreamErr *UpstreamError
	return errors.As(err, &upstreamErr) && upstreamErr.Retryable
}

func IsCircuitFailure(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var upstreamErr *UpstreamError
	if !errors.As(err, &upstreamErr) {
		return true
	}
	return upstreamErr.Kind == ErrorKindNetwork ||
		upstreamErr.Kind == ErrorKindInvalidResponse ||
		upstreamErr.StatusCode == 429 ||
		upstreamErr.StatusCode >= 500
}

func ErrorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "upstream_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "request_canceled"
	}
	var upstreamErr *UpstreamError
	if !errors.As(err, &upstreamErr) {
		return "provider_request_failed"
	}
	switch {
	case upstreamErr.StatusCode == 429:
		return "provider_rate_limited"
	case upstreamErr.StatusCode == 503:
		return "provider_unavailable"
	case upstreamErr.Kind == ErrorKindNetwork && errors.Is(err, context.DeadlineExceeded):
		return "upstream_timeout"
	case upstreamErr.Kind == ErrorKindNetwork:
		return "provider_network_error"
	case upstreamErr.Kind == ErrorKindInvalidResponse:
		return "invalid_provider_response"
	case upstreamErr.Kind == ErrorKindUnavailable:
		return "no_healthy_provider"
	default:
		return "provider_request_failed"
	}
}

func retryableTransportError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) &&
		(opErr.Op == "dial" || opErr.Op == "read" || opErr.Op == "write")
}
