package cache

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/theHerta27/ModelGate/internal/provider"
	"github.com/theHerta27/ModelGate/internal/redisstore"
)

type fakeKeyValue struct {
	value  []byte
	getErr error
	setErr error
	key    string
	ttl    time.Duration
}

func (s *fakeKeyValue) Get(context.Context, string) ([]byte, error) {
	return s.value, s.getErr
}

func (s *fakeKeyValue) Set(_ context.Context, key string, _ []byte, ttl time.Duration) error {
	s.key = key
	s.ttl = ttl
	return s.setErr
}

func TestRedisCacheHitMissAndSet(t *testing.T) {
	store := &fakeKeyValue{value: []byte("response")}
	responseCache, _ := NewRedisCache(store, time.Minute)

	value, found, err := responseCache.Get(context.Background(), "key")
	if err != nil || !found || string(value) != "response" {
		t.Fatalf("Get() = %q, %v, %v", value, found, err)
	}
	store.getErr = redisstore.ErrNotFound
	if _, found, err := responseCache.Get(context.Background(), "missing"); err != nil || found {
		t.Fatalf("missing Get() found=%v err=%v", found, err)
	}
	if err := responseCache.Set(context.Background(), "key", []byte("response")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if store.key != "modelgate:cache:key" || store.ttl != time.Minute {
		t.Fatalf("set key/ttl = %q/%s", store.key, store.ttl)
	}
}

func TestRedisCachePropagatesBackendErrors(t *testing.T) {
	store := &fakeKeyValue{getErr: errors.New("get failed"), setErr: errors.New("set failed")}
	responseCache, _ := NewRedisCache(store, time.Minute)
	if _, _, err := responseCache.Get(context.Background(), "key"); err == nil {
		t.Fatal("Get() error = nil")
	}
	if err := responseCache.Set(context.Background(), "key", nil); err == nil {
		t.Fatal("Set() error = nil")
	}
}

func TestPolicyOnlyCachesExplicitDeterministicRequests(t *testing.T) {
	zero := 0.0
	request := &provider.ChatRequest{
		Model:       "model",
		Messages:    []provider.ChatMessage{{Role: "system", Content: "secret"}, {Role: "user", Content: "hello"}},
		Temperature: &zero,
	}
	key, cacheable, err := (Policy{}).Key("client-a", request)
	if err != nil || !cacheable || key == "" {
		t.Fatalf("Key() = %q, %v, %v", key, cacheable, err)
	}
	if strings.Contains(key, "secret") || strings.Contains(key, "hello") {
		t.Fatalf("cache key exposes prompt: %q", key)
	}
	other, _, _ := (Policy{}).Key("client-b", request)
	if other == key {
		t.Fatal("cache key is not client scoped")
	}
	request.Temperature = nil
	if _, cacheable, _ := (Policy{}).Key("client-a", request); cacheable {
		t.Fatal("nil temperature request is cacheable")
	}
	request.Temperature = &zero
	request.Stream = true
	if _, cacheable, _ := (Policy{}).Key("client-a", request); cacheable {
		t.Fatal("streaming request is cacheable")
	}
}

func TestNewRedisCacheValidatesConfiguration(t *testing.T) {
	if _, err := NewRedisCache(nil, time.Minute); err == nil {
		t.Fatal("nil store error = nil")
	}
	if _, err := NewRedisCache(&fakeKeyValue{}, 0); err == nil {
		t.Fatal("zero TTL error = nil")
	}
}
