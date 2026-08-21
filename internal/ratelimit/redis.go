package ratelimit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

const tokenBucketScript = `
local time = redis.call('TIME')
local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)
local rate_per_ms = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'timestamp_ms')
local tokens = tonumber(state[1]) or capacity
local timestamp_ms = tonumber(state[2]) or now_ms
local elapsed = math.max(0, now_ms - timestamp_ms)
tokens = math.min(capacity, tokens + (elapsed * rate_per_ms))

local allowed = 0
local retry_after_ms = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
else
  retry_after_ms = math.ceil((requested - tokens) / rate_per_ms)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'timestamp_ms', now_ms)
local fill_time_ms = math.ceil(capacity / rate_per_ms)
redis.call('PEXPIRE', KEYS[1], math.max(1000, fill_time_ms * 2))
return {allowed, math.floor(tokens), retry_after_ms}
`

type Evaluator interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

type Limiter interface {
	Allow(ctx context.Context, identity string) (Decision, error)
}

type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
}

type RedisLimiter struct {
	evaluator Evaluator
	rpm       int
	burst     int
}

func NewRedisLimiter(evaluator Evaluator, requestsPerMinute, burst int) (*RedisLimiter, error) {
	if evaluator == nil {
		return nil, fmt.Errorf("rate limiter evaluator is required")
	}
	if requestsPerMinute <= 0 || burst <= 0 {
		return nil, fmt.Errorf("rate limiter rpm and burst must be positive")
	}
	return &RedisLimiter{evaluator: evaluator, rpm: requestsPerMinute, burst: burst}, nil
}

func (l *RedisLimiter) Allow(ctx context.Context, identity string) (Decision, error) {
	digest := sha256.Sum256([]byte(identity))
	key := "modelgate:ratelimit:" + hex.EncodeToString(digest[:])
	ratePerMillisecond := float64(l.rpm) / float64(time.Minute/time.Millisecond)

	result, err := l.evaluator.Eval(
		ctx,
		tokenBucketScript,
		[]string{key},
		ratePerMillisecond,
		l.burst,
		1,
	)
	if err != nil {
		return Decision{}, fmt.Errorf("evaluate token bucket: %w", err)
	}
	values, ok := result.([]any)
	if !ok || len(values) != 3 {
		return Decision{}, fmt.Errorf("token bucket returned an invalid result")
	}
	allowed, err := integer(values[0])
	if err != nil {
		return Decision{}, err
	}
	remaining, err := integer(values[1])
	if err != nil {
		return Decision{}, err
	}
	retryMilliseconds, err := integer(values[2])
	if err != nil {
		return Decision{}, err
	}

	return Decision{
		Allowed:    allowed == 1,
		Limit:      l.burst,
		Remaining:  int(remaining),
		RetryAfter: time.Duration(retryMilliseconds) * time.Millisecond,
	}, nil
}

func integer(value any) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed, nil
		}
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err == nil {
			return parsed, nil
		}
	}
	return 0, fmt.Errorf("token bucket returned a non-integer value")
}
