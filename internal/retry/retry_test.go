package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPolicyRetriesRetryableError(t *testing.T) {
	retryable := errors.New("retryable")
	attempts := 0
	events := make([]Attempt, 0, 3)
	policy := newTestPolicy(t, Options{
		MaxAttempts: 3,
		Jitter:      func(time.Duration) time.Duration { return 0 },
		OnAttempt:   func(event Attempt) { events = append(events, event) },
	})

	err := policy.Do(context.Background(), func(err error) bool {
		return errors.Is(err, retryable)
	}, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return retryable
		}
		return nil
	})
	if err != nil || attempts != 3 || len(events) != 3 || events[2].Err != nil {
		t.Fatalf("error/attempts/events = %v/%d/%#v", err, attempts, events)
	}
}

func TestPolicyDoesNotRetryNonRetryableError(t *testing.T) {
	wantErr := errors.New("permanent")
	attempts := 0
	policy := newTestPolicy(t, Options{MaxAttempts: 3})

	err := policy.Do(context.Background(), func(error) bool { return false }, func(context.Context) error {
		attempts++
		return wantErr
	})
	if !errors.Is(err, wantErr) || attempts != 1 {
		t.Fatalf("error/attempts = %v/%d", err, attempts)
	}
}

func TestPolicyAttemptTimeoutCanRetry(t *testing.T) {
	attempts := 0
	policy := newTestPolicy(t, Options{
		MaxAttempts:    2,
		AttemptTimeout: 10 * time.Millisecond,
		Jitter:         func(time.Duration) time.Duration { return 0 },
	})

	err := policy.Do(context.Background(), func(err error) bool {
		return errors.Is(err, context.DeadlineExceeded)
	}, func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err != nil || attempts != 2 {
		t.Fatalf("error/attempts = %v/%d", err, attempts)
	}
}

func TestPolicyParentCancellationStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := newTestPolicy(t, Options{
		MaxAttempts: 3,
		Jitter:      func(time.Duration) time.Duration { return time.Hour },
	})
	attempts := 0
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- policy.Do(ctx, func(error) bool { return true }, func(context.Context) error {
			attempts++
			close(started)
			return errors.New("retryable")
		})
	}()

	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || attempts != 1 {
		t.Fatalf("error/attempts = %v/%d", err, attempts)
	}
}

func TestPolicyRetainsSuccessfulAttemptContext(t *testing.T) {
	policy := newTestPolicy(t, Options{MaxAttempts: 1})
	var attemptContext context.Context
	cancel, err := policy.DoRetained(
		context.Background(),
		func(error) bool { return false },
		func(ctx context.Context) error {
			attemptContext = ctx
			return nil
		},
	)
	if err != nil || cancel == nil || attemptContext.Err() != nil {
		t.Fatalf("cancel/error/context = %v/%v/%v", cancel, err, attemptContext.Err())
	}
	cancel()
	if !errors.Is(attemptContext.Err(), context.Canceled) {
		t.Fatalf("context error = %v, want canceled", attemptContext.Err())
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	for _, options := range []Options{
		{},
		{MaxAttempts: 1},
		{MaxAttempts: 1, AttemptTimeout: time.Second, BaseBackoff: time.Second, MaxBackoff: time.Millisecond},
	} {
		if _, err := New(options); err == nil {
			t.Fatalf("New(%#v) error = nil", options)
		}
	}
}

func newTestPolicy(t *testing.T, options Options) *Policy {
	t.Helper()
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 1
	}
	if options.AttemptTimeout == 0 {
		options.AttemptTimeout = time.Second
	}
	if options.BaseBackoff == 0 {
		options.BaseBackoff = time.Millisecond
	}
	if options.MaxBackoff == 0 {
		options.MaxBackoff = time.Second
	}
	policy, err := New(options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return policy
}
