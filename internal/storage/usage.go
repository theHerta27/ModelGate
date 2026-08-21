package storage

import (
	"context"
	"fmt"
	"time"
)

const (
	StatusSucceeded         = "succeeded"
	StatusFailed            = "failed"
	StatusRateLimited       = "rate_limited"
	StatusIdempotencyReplay = "idempotency_replay"
	StatusCacheHit          = "cache_hit"
	StatusRejected          = "rejected"
)

type RequestRecord struct {
	RequestID     string
	Provider      string
	Model         string
	Status        string
	InputTokens   int
	OutputTokens  int
	TotalTokens   int
	Latency       time.Duration
	EstimatedCost float64
	CacheStatus   string
	ErrorCode     string
	CreatedAt     time.Time
}

type Recorder interface {
	Record(ctx context.Context, record RequestRecord) error
}

type UsageRepository struct {
	database Beginner
}

func NewUsageRepository(database Beginner) (*UsageRepository, error) {
	if database == nil {
		return nil, fmt.Errorf("usage database is required")
	}
	return &UsageRepository{database: database}, nil
}

func (r *UsageRepository) Record(ctx context.Context, record RequestRecord) error {
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.Provider == "" || record.Model == "" || record.RequestID == "" {
		return fmt.Errorf("request id, provider, and model are required")
	}
	if record.Latency < 0 || record.InputTokens < 0 || record.OutputTokens < 0 || record.TotalTokens < 0 {
		return fmt.Errorf("usage counters and latency must be non-negative")
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin usage transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	if err := tx.Exec(
		ctx,
		"INSERT INTO providers (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
		record.Provider,
	); err != nil {
		return fmt.Errorf("ensure provider: %w", err)
	}
	if err := tx.Exec(ctx, `
INSERT INTO requests (
    id, provider, model, status, latency_ms, cache_status, error_code, created_at
) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8)`,
		record.RequestID,
		record.Provider,
		record.Model,
		record.Status,
		record.Latency.Milliseconds(),
		record.CacheStatus,
		record.ErrorCode,
		record.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert request record: %w", err)
	}
	if err := tx.Exec(ctx, `
INSERT INTO usage_records (
    request_id, input_tokens, output_tokens, total_tokens, estimated_cost, created_at
) VALUES ($1, $2, $3, $4, $5, $6)`,
		record.RequestID,
		record.InputTokens,
		record.OutputTokens,
		record.TotalTokens,
		record.EstimatedCost,
		record.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert usage record: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit usage transaction: %w", err)
	}
	return nil
}
