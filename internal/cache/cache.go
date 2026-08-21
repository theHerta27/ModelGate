package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/redisstore"
)

const (
	StatusBypass = "BYPASS"
	StatusMiss   = "MISS"
	StatusHit    = "HIT"
)

type KeyValueStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

type Store interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, response []byte) error
}

type RedisCache struct {
	store KeyValueStore
	ttl   time.Duration
}

func NewRedisCache(store KeyValueStore, ttl time.Duration) (*RedisCache, error) {
	if store == nil {
		return nil, fmt.Errorf("cache store is required")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("cache TTL must be positive")
	}
	return &RedisCache{store: store, ttl: ttl}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := c.store.Get(ctx, "modelgate:cache:"+key)
	if errors.Is(err, redisstore.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get cached response: %w", err)
	}
	return value, true, nil
}

func (c *RedisCache) Set(ctx context.Context, key string, response []byte) error {
	if err := c.store.Set(ctx, "modelgate:cache:"+key, response, c.ttl); err != nil {
		return fmt.Errorf("set cached response: %w", err)
	}
	return nil
}

type Policy struct{}

func (Policy) Key(identity string, req *provider.ChatRequest) (string, bool, error) {
	if req == nil || req.Stream || req.Temperature == nil || *req.Temperature != 0 {
		return "", false, nil
	}
	canonical, err := json.Marshal(req)
	if err != nil {
		return "", false, fmt.Errorf("encode cache fingerprint: %w", err)
	}
	digest := sha256.Sum256(append([]byte(identity+"\x00"), canonical...))
	return hex.EncodeToString(digest[:]), true, nil
}
