package ratelimit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeEvaluator struct {
	result any
	err    error
	script string
	keys   []string
	args   []any
}

func (e *fakeEvaluator) Eval(
	_ context.Context,
	script string,
	keys []string,
	args ...any,
) (any, error) {
	e.script = script
	e.keys = append([]string(nil), keys...)
	e.args = append([]any(nil), args...)
	return e.result, e.err
}

func TestRedisLimiterAllowsAndHashesIdentity(t *testing.T) {
	evaluator := &fakeEvaluator{result: []any{int64(1), int64(4), int64(0)}}
	limiter, err := NewRedisLimiter(evaluator, 60, 5)
	if err != nil {
		t.Fatalf("NewRedisLimiter() error = %v", err)
	}

	decision, err := limiter.Allow(context.Background(), "192.0.2.10")
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	if !decision.Allowed || decision.Limit != 5 || decision.Remaining != 4 {
		t.Fatalf("decision = %#v", decision)
	}
	if len(evaluator.keys) != 1 || strings.Contains(evaluator.keys[0], "192.0.2.10") {
		t.Fatalf("Redis key exposes identity: %v", evaluator.keys)
	}
	for _, token := range []string{"TIME", "HMGET", "HSET", "PEXPIRE"} {
		if !strings.Contains(evaluator.script, token) {
			t.Fatalf("Lua script does not contain %s", token)
		}
	}
}

func TestRedisLimiterReturnsRetryAfter(t *testing.T) {
	evaluator := &fakeEvaluator{result: []any{int64(0), int64(0), int64(1250)}}
	limiter, _ := NewRedisLimiter(evaluator, 60, 1)

	decision, err := limiter.Allow(context.Background(), "client")
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	if decision.Allowed || decision.RetryAfter != 1250*time.Millisecond {
		t.Fatalf("decision = %#v", decision)
	}
}

func TestRedisLimiterRejectsBackendAndMalformedResults(t *testing.T) {
	tests := []struct {
		name   string
		result any
		err    error
	}{
		{name: "backend", err: errors.New("redis unavailable")},
		{name: "shape", result: []any{int64(1)}},
		{name: "type", result: []any{"yes", int64(1), int64(0)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limiter, _ := NewRedisLimiter(&fakeEvaluator{result: test.result, err: test.err}, 60, 1)
			if _, err := limiter.Allow(context.Background(), "client"); err == nil {
				t.Fatal("Allow() error = nil")
			}
		})
	}
}

func TestNewRedisLimiterValidatesConfiguration(t *testing.T) {
	if _, err := NewRedisLimiter(nil, 60, 1); err == nil {
		t.Fatal("nil evaluator error = nil")
	}
	if _, err := NewRedisLimiter(&fakeEvaluator{}, 0, 1); err == nil {
		t.Fatal("zero rpm error = nil")
	}
}
