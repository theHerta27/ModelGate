package redisstore

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNotFound = errors.New("redis key not found")

type Client struct {
	inner *redis.Client
}

func New(addr, password string, database int, timeout time.Duration) *Client {
	return &Client{inner: redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           database,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	})}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.inner.Ping(ctx).Err()
}

func (c *Client) Eval(
	ctx context.Context,
	script string,
	keys []string,
	args ...any,
) (any, error) {
	return c.inner.Eval(ctx, script, keys, args...).Result()
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := c.inner.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	return value, err
}

func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.inner.Set(ctx, key, value, ttl).Err()
}

func (c *Client) Close() error {
	return c.inner.Close()
}
