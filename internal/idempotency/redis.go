package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

const beginScript = `
local current = redis.call('HMGET', KEYS[1], 'fingerprint', 'state', 'response')
if not current[1] then
  redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'state', 'pending')
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return {'acquired'}
end
if current[1] ~= ARGV[1] then
  return {'conflict'}
end
if current[2] == 'completed' then
  return {'completed', current[3] or ''}
end
return {'pending'}
`

const completeScript = `
local current = redis.call('HMGET', KEYS[1], 'fingerprint', 'state')
if current[1] ~= ARGV[1] or current[2] ~= 'pending' then
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'completed', 'response', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`

const releaseScript = `
local current = redis.call('HMGET', KEYS[1], 'fingerprint', 'state')
if current[1] == ARGV[1] and current[2] == 'pending' then
  return redis.call('DEL', KEYS[1])
end
return 0
`

type Evaluator interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

type Store interface {
	Begin(ctx context.Context, key, fingerprint string) (Result, error)
	Complete(ctx context.Context, key, fingerprint string, response []byte) error
	Release(ctx context.Context, key, fingerprint string) error
}

type Status string

const (
	StatusAcquired  Status = "acquired"
	StatusPending   Status = "pending"
	StatusCompleted Status = "completed"
	StatusConflict  Status = "conflict"
)

type Result struct {
	Status   Status
	Response []byte
}

type RedisStore struct {
	evaluator Evaluator
	lockTTL   time.Duration
	resultTTL time.Duration
}

func NewRedisStore(evaluator Evaluator, lockTTL, resultTTL time.Duration) (*RedisStore, error) {
	if evaluator == nil {
		return nil, fmt.Errorf("idempotency evaluator is required")
	}
	if lockTTL <= 0 || resultTTL <= 0 {
		return nil, fmt.Errorf("idempotency TTLs must be positive")
	}
	return &RedisStore{evaluator: evaluator, lockTTL: lockTTL, resultTTL: resultTTL}, nil
}

func ScopedKey(identity, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(identity + "\x00" + idempotencyKey))
	return hex.EncodeToString(digest[:])
}

func (s *RedisStore) Begin(ctx context.Context, key, fingerprint string) (Result, error) {
	result, err := s.evaluator.Eval(
		ctx,
		beginScript,
		[]string{s.redisKey(key)},
		fingerprint,
		s.lockTTL.Milliseconds(),
	)
	if err != nil {
		return Result{}, fmt.Errorf("begin idempotency request: %w", err)
	}
	values, ok := result.([]any)
	if !ok || len(values) == 0 {
		return Result{}, fmt.Errorf("idempotency begin returned an invalid result")
	}
	status, ok := text(values[0])
	if !ok {
		return Result{}, fmt.Errorf("idempotency begin returned an invalid status")
	}
	parsed := Result{Status: Status(status)}
	switch parsed.Status {
	case StatusAcquired, StatusPending, StatusConflict:
		return parsed, nil
	case StatusCompleted:
		if len(values) != 2 {
			return Result{}, fmt.Errorf("completed idempotency result is missing its response")
		}
		response, ok := bytes(values[1])
		if !ok {
			return Result{}, fmt.Errorf("completed idempotency response is invalid")
		}
		parsed.Response = response
		return parsed, nil
	default:
		return Result{}, fmt.Errorf("idempotency begin returned unknown status %q", status)
	}
}

func (s *RedisStore) Complete(
	ctx context.Context,
	key, fingerprint string,
	response []byte,
) error {
	result, err := s.evaluator.Eval(
		ctx,
		completeScript,
		[]string{s.redisKey(key)},
		fingerprint,
		response,
		s.resultTTL.Milliseconds(),
	)
	if err != nil {
		return fmt.Errorf("complete idempotency request: %w", err)
	}
	updated, err := integer(result)
	if err != nil || updated != 1 {
		return fmt.Errorf("idempotency request is no longer pending")
	}
	return nil
}

func (s *RedisStore) Release(ctx context.Context, key, fingerprint string) error {
	if _, err := s.evaluator.Eval(
		ctx,
		releaseScript,
		[]string{s.redisKey(key)},
		fingerprint,
	); err != nil {
		return fmt.Errorf("release idempotency request: %w", err)
	}
	return nil
}

func (s *RedisStore) redisKey(key string) string {
	return "modelgate:idempotency:" + key
}

func text(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

func bytes(value any) ([]byte, bool) {
	switch value := value.(type) {
	case string:
		return []byte(value), true
	case []byte:
		return value, true
	default:
		return nil, false
	}
}

func integer(value any) (int64, error) {
	if value, ok := value.(int64); ok {
		return value, nil
	}
	return 0, fmt.Errorf("idempotency script returned a non-integer value")
}
