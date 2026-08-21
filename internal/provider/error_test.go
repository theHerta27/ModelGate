package provider

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestUpstreamErrorClassification(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantRetry     bool
		wantCircuit   bool
		wantErrorCode string
	}{
		{
			name:      "rate limited",
			err:       &UpstreamError{Provider: "test", Kind: ErrorKindHTTPStatus, StatusCode: 429, Retryable: true},
			wantRetry: true, wantCircuit: true, wantErrorCode: "provider_rate_limited",
		},
		{
			name:      "service unavailable",
			err:       &UpstreamError{Provider: "test", Kind: ErrorKindHTTPStatus, StatusCode: 503, Retryable: true},
			wantRetry: true, wantCircuit: true, wantErrorCode: "provider_unavailable",
		},
		{
			name:      "unauthorized",
			err:       &UpstreamError{Provider: "test", Kind: ErrorKindHTTPStatus, StatusCode: 401},
			wantRetry: false, wantCircuit: false, wantErrorCode: "provider_request_failed",
		},
		{
			name:      "deadline",
			err:       context.DeadlineExceeded,
			wantRetry: true, wantCircuit: true, wantErrorCode: "upstream_timeout",
		},
		{
			name:      "canceled",
			err:       context.Canceled,
			wantRetry: false, wantCircuit: false, wantErrorCode: "request_canceled",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsRetryable(test.err); got != test.wantRetry {
				t.Fatalf("IsRetryable() = %v, want %v", got, test.wantRetry)
			}
			if got := IsCircuitFailure(test.err); got != test.wantCircuit {
				t.Fatalf("IsCircuitFailure() = %v, want %v", got, test.wantCircuit)
			}
			if got := ErrorCode(test.err); got != test.wantErrorCode {
				t.Fatalf("ErrorCode() = %q, want %q", got, test.wantErrorCode)
			}
		})
	}
}

func TestUpstreamErrorUnwrapsCause(t *testing.T) {
	cause := errors.New("network failure")
	err := &UpstreamError{Provider: "test", Operation: "chat", Kind: ErrorKindNetwork, Cause: cause}
	if !errors.Is(err, cause) {
		t.Fatalf("errors.Is(%v, cause) = false", err)
	}
}

func TestRetryableTransportErrorUsesExplicitSignals(t *testing.T) {
	if !retryableTransportError(&net.DNSError{IsTimeout: true}) {
		t.Fatal("DNS timeout was not classified as retryable")
	}
	if retryableTransportError(&net.DNSError{IsNotFound: true}) {
		t.Fatal("DNS not-found must not be classified as retryable")
	}
	if !retryableTransportError(&net.OpError{Op: "dial", Err: errors.New("connection refused")}) {
		t.Fatal("dial failure was not classified as retryable")
	}
}
