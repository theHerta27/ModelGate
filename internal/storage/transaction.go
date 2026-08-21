package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Row interface {
	Scan(dest ...any) error
}

type Transaction interface {
	Exec(ctx context.Context, sql string, arguments ...any) error
	QueryRow(ctx context.Context, sql string, arguments ...any) Row
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Beginner interface {
	Begin(ctx context.Context) (Transaction, error)
}

type PoolAdapter struct {
	pool *pgxpool.Pool
}

func NewPoolAdapter(pool *pgxpool.Pool) *PoolAdapter {
	return &PoolAdapter{pool: pool}
}

func (a *PoolAdapter) Begin(ctx context.Context) (Transaction, error) {
	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return pgxTransaction{tx: tx}, nil
}

type pgxTransaction struct {
	tx pgx.Tx
}

func (t pgxTransaction) Exec(ctx context.Context, sql string, arguments ...any) error {
	_, err := t.tx.Exec(ctx, sql, arguments...)
	return err
}

func (t pgxTransaction) QueryRow(
	ctx context.Context,
	sql string,
	arguments ...any,
) Row {
	return t.tx.QueryRow(ctx, sql, arguments...)
}

func (t pgxTransaction) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t pgxTransaction) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}
