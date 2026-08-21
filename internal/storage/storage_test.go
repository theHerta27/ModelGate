package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

type fakeBeginner struct {
	tx    *fakeTransaction
	err   error
	calls int
}

func (b *fakeBeginner) Begin(context.Context) (Transaction, error) {
	b.calls++
	return b.tx, b.err
}

type fakeTransaction struct {
	executed      []string
	execCalls     int
	execErrorAt   int
	queryValues   []bool
	queryErr      error
	queryCalls    int
	commitErr     error
	commitCalls   int
	rollbackCalls int
}

func (t *fakeTransaction) Exec(_ context.Context, sql string, _ ...any) error {
	t.execCalls++
	t.executed = append(t.executed, sql)
	if t.execErrorAt == t.execCalls {
		return errors.New("exec failed")
	}
	return nil
}

func (t *fakeTransaction) QueryRow(context.Context, string, ...any) Row {
	value := false
	if t.queryCalls < len(t.queryValues) {
		value = t.queryValues[t.queryCalls]
	}
	t.queryCalls++
	return fakeRow{value: value, err: t.queryErr}
}

func (t *fakeTransaction) Commit(context.Context) error {
	t.commitCalls++
	return t.commitErr
}

func (t *fakeTransaction) Rollback(context.Context) error {
	t.rollbackCalls++
	return nil
}

type fakeRow struct {
	value bool
	err   error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(*bool)) = r.value
	return nil
}

func TestUsageRepositoryRecordsRequestAndUsageTransactionally(t *testing.T) {
	tx := &fakeTransaction{}
	repository, err := NewUsageRepository(&fakeBeginner{tx: tx})
	if err != nil {
		t.Fatalf("NewUsageRepository() error = %v", err)
	}
	record := RequestRecord{
		RequestID: "00000000-0000-4000-8000-000000000001",
		Provider:  "mock", Model: "mock-model", Status: StatusSucceeded,
		InputTokens: 3, OutputTokens: 2, TotalTokens: 5,
		Latency: 20 * time.Millisecond, CacheStatus: "MISS",
	}

	if err := repository.Record(context.Background(), record); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if tx.execCalls != 3 || tx.commitCalls != 1 || tx.rollbackCalls != 1 {
		t.Fatalf("transaction calls exec/commit/rollback = %d/%d/%d", tx.execCalls, tx.commitCalls, tx.rollbackCalls)
	}
	if !strings.Contains(tx.executed[0], "INSERT INTO providers") ||
		!strings.Contains(tx.executed[1], "INSERT INTO requests") ||
		!strings.Contains(tx.executed[2], "INSERT INTO usage_records") {
		t.Fatalf("unexpected SQL sequence: %v", tx.executed)
	}
}

func TestUsageRepositoryRollsBackOnFailure(t *testing.T) {
	tx := &fakeTransaction{execErrorAt: 2}
	repository, _ := NewUsageRepository(&fakeBeginner{tx: tx})
	err := repository.Record(context.Background(), RequestRecord{
		RequestID: "00000000-0000-4000-8000-000000000001",
		Provider:  "mock", Model: "model", Status: StatusFailed,
	})
	if err == nil || tx.commitCalls != 0 || tx.rollbackCalls != 1 {
		t.Fatalf("Record() error/commit/rollback = %v/%d/%d", err, tx.commitCalls, tx.rollbackCalls)
	}
}

func TestUsageRepositoryValidatesBeforeBeginning(t *testing.T) {
	beginner := &fakeBeginner{tx: &fakeTransaction{}}
	repository, _ := NewUsageRepository(beginner)
	if err := repository.Record(context.Background(), RequestRecord{}); err == nil {
		t.Fatal("Record() error = nil")
	}
	if beginner.calls != 0 {
		t.Fatal("database transaction began for invalid record")
	}
}

func TestApplyMigrationsAppliesPendingFilesInTransaction(t *testing.T) {
	tx := &fakeTransaction{queryValues: []bool{false, false}}
	database := &fakeBeginner{tx: tx}
	migrationFS := fstest.MapFS{
		"002_second.sql": {Data: []byte("CREATE TABLE second_table (id BIGINT);")},
		"001_first.sql":  {Data: []byte("CREATE TABLE first_table (id BIGINT);")},
	}

	if err := ApplyMigrations(context.Background(), database, migrationFS); err != nil {
		t.Fatalf("ApplyMigrations() error = %v", err)
	}
	if tx.commitCalls != 1 || tx.rollbackCalls != 1 || tx.queryCalls != 2 {
		t.Fatalf("migration transaction calls = commit %d rollback %d query %d", tx.commitCalls, tx.rollbackCalls, tx.queryCalls)
	}
	joined := strings.Join(tx.executed, "\n")
	first := strings.Index(joined, "CREATE TABLE first_table")
	second := strings.Index(joined, "CREATE TABLE second_table")
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("migrations not applied in order: %v", tx.executed)
	}
}

func TestApplyMigrationsSkipsAppliedFile(t *testing.T) {
	tx := &fakeTransaction{queryValues: []bool{true}}
	migrationFS := fstest.MapFS{"001_init.sql": {Data: []byte("SHOULD NOT RUN")}}
	if err := ApplyMigrations(context.Background(), &fakeBeginner{tx: tx}, migrationFS); err != nil {
		t.Fatalf("ApplyMigrations() error = %v", err)
	}
	if strings.Contains(strings.Join(tx.executed, "\n"), "SHOULD NOT RUN") {
		t.Fatal("already applied migration executed")
	}
}

func TestApplyMigrationsRollsBackOnSQLFailure(t *testing.T) {
	tx := &fakeTransaction{execErrorAt: 3, queryValues: []bool{false}}
	migrationFS := fstest.MapFS{"001_init.sql": {Data: []byte("BROKEN SQL")}}
	err := ApplyMigrations(context.Background(), &fakeBeginner{tx: tx}, migrationFS)
	if err == nil || tx.commitCalls != 0 || tx.rollbackCalls != 1 {
		t.Fatalf("ApplyMigrations() error/commit/rollback = %v/%d/%d", err, tx.commitCalls, tx.rollbackCalls)
	}
}

func TestNewUsageRepositoryRequiresDatabase(t *testing.T) {
	if _, err := NewUsageRepository(nil); err == nil {
		t.Fatal("nil database error = nil")
	}
}
