package idempotency

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type evaluation struct {
	result any
	err    error
}

type fakeEvaluator struct {
	evaluations []evaluation
	keys        [][]string
	scripts     []string
}

func (e *fakeEvaluator) Eval(
	_ context.Context,
	script string,
	keys []string,
	_ ...any,
) (any, error) {
	e.scripts = append(e.scripts, script)
	e.keys = append(e.keys, append([]string(nil), keys...))
	result := e.evaluations[0]
	e.evaluations = e.evaluations[1:]
	return result.result, result.err
}

func TestRedisStoreBeginStatuses(t *testing.T) {
	tests := []struct {
		name     string
		result   []any
		status   Status
		response string
	}{
		{name: "acquired", result: []any{"acquired"}, status: StatusAcquired},
		{name: "pending", result: []any{"pending"}, status: StatusPending},
		{name: "conflict", result: []any{"conflict"}, status: StatusConflict},
		{name: "completed", result: []any{"completed", "response"}, status: StatusCompleted, response: "response"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evaluator := &fakeEvaluator{evaluations: []evaluation{{result: test.result}}}
			store, _ := NewRedisStore(evaluator, time.Second, time.Hour)
			result, err := store.Begin(context.Background(), "key", "fingerprint")
			if err != nil {
				t.Fatalf("Begin() error = %v", err)
			}
			if result.Status != test.status || string(result.Response) != test.response {
				t.Fatalf("result = %#v", result)
			}
		})
	}
}

func TestRedisStoreCompletesAndReleases(t *testing.T) {
	evaluator := &fakeEvaluator{evaluations: []evaluation{
		{result: int64(1)},
		{result: int64(1)},
	}}
	store, _ := NewRedisStore(evaluator, time.Second, time.Hour)

	if err := store.Complete(context.Background(), "key", "fingerprint", []byte("response")); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if err := store.Release(context.Background(), "key", "fingerprint"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if !strings.Contains(evaluator.scripts[0], "state', 'completed") || !strings.Contains(evaluator.scripts[1], "DEL") {
		t.Fatal("unexpected scripts used")
	}
}

func TestRedisStoreRejectsBackendAndInvalidResults(t *testing.T) {
	tests := []evaluation{
		{err: errors.New("redis unavailable")},
		{result: []any{}},
		{result: []any{"unknown"}},
		{result: []any{"completed"}},
	}
	for index, testCase := range tests {
		evaluator := &fakeEvaluator{evaluations: []evaluation{testCase}}
		store, _ := NewRedisStore(evaluator, time.Second, time.Hour)
		if _, err := store.Begin(context.Background(), "key", "fingerprint"); err == nil {
			t.Fatalf("case %d Begin() error = nil", index)
		}
	}
}

func TestScopedKeyIsStableAndDoesNotExposeInputs(t *testing.T) {
	first := ScopedKey("client", "request-key")
	second := ScopedKey("client", "request-key")
	other := ScopedKey("other", "request-key")
	if first != second || first == other {
		t.Fatalf("scoped keys = %q, %q, %q", first, second, other)
	}
	if strings.Contains(first, "client") || strings.Contains(first, "request-key") {
		t.Fatalf("scoped key exposes input: %q", first)
	}
}

func TestNewRedisStoreValidatesConfiguration(t *testing.T) {
	if _, err := NewRedisStore(nil, time.Second, time.Hour); err == nil {
		t.Fatal("nil evaluator error = nil")
	}
	if _, err := NewRedisStore(&fakeEvaluator{}, 0, time.Hour); err == nil {
		t.Fatal("zero TTL error = nil")
	}
}
