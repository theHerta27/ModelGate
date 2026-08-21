package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

func TestBreakerClosedOpenHalfOpenClosed(t *testing.T) {
	now := time.Unix(100, 0)
	states := []State{}
	breaker := newTestBreaker(t, Options{
		WindowSize: 2, MinimumRequests: 2, FailureRatio: 0.5,
		OpenTimeout: time.Second, HalfOpenMaxRequests: 1,
		Now:           func() time.Time { return now },
		OnStateChange: func(state State) { states = append(states, state) },
	})

	permit, _ := breaker.Allow()
	permit.Done(OutcomeSuccess)
	permit, _ = breaker.Allow()
	permit.Done(OutcomeFailure)
	if state := breaker.State(); state != StateOpen {
		t.Fatalf("state = %s, want open", state)
	}
	if _, err := breaker.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow() error = %v, want ErrOpen", err)
	}

	now = now.Add(time.Second)
	probe, err := breaker.Allow()
	if err != nil || breaker.State() != StateHalfOpen {
		t.Fatalf("half-open Allow/state = %v/%s", err, breaker.State())
	}
	probe.Done(OutcomeSuccess)
	if state := breaker.State(); state != StateClosed {
		t.Fatalf("state = %s, want closed", state)
	}
	if len(states) != 3 || states[0] != StateOpen || states[1] != StateHalfOpen || states[2] != StateClosed {
		t.Fatalf("state changes = %#v", states)
	}
}

func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	now := time.Unix(100, 0)
	breaker := newTestBreaker(t, Options{
		WindowSize: 1, MinimumRequests: 1, FailureRatio: 1,
		OpenTimeout: time.Second, HalfOpenMaxRequests: 1,
		Now: func() time.Time { return now },
	})
	permit, _ := breaker.Allow()
	permit.Done(OutcomeFailure)
	now = now.Add(time.Second)
	probe, err := breaker.Allow()
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}
	probe.Done(OutcomeFailure)
	if breaker.State() != StateOpen {
		t.Fatalf("state = %s, want open", breaker.State())
	}
	now = now.Add(time.Second - time.Nanosecond)
	if _, err := breaker.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow() error = %v, want ErrOpen before second timeout", err)
	}
}

func TestBreakerHalfOpenProbeLimit(t *testing.T) {
	now := time.Unix(100, 0)
	breaker := newTestBreaker(t, Options{
		WindowSize: 1, MinimumRequests: 1, FailureRatio: 1,
		OpenTimeout: time.Second, HalfOpenMaxRequests: 2,
		Now: func() time.Time { return now },
	})
	permit, _ := breaker.Allow()
	permit.Done(OutcomeFailure)
	now = now.Add(time.Second)
	first, _ := breaker.Allow()
	second, _ := breaker.Allow()
	if breaker.Available() {
		t.Fatal("breaker reports available with all half-open probes active")
	}
	if _, err := breaker.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("Allow() error = %v, want ErrOpen", err)
	}
	first.Done(OutcomeSuccess)
	second.Done(OutcomeSuccess)
	if breaker.State() != StateClosed {
		t.Fatalf("state = %s, want closed", breaker.State())
	}
}

func TestPermitDoneOnlyCountsOnce(t *testing.T) {
	breaker := newTestBreaker(t, Options{
		WindowSize: 2, MinimumRequests: 2, FailureRatio: 1,
	})
	first, _ := breaker.Allow()
	first.Done(OutcomeFailure)
	first.Done(OutcomeFailure)
	second, _ := breaker.Allow()
	second.Done(OutcomeSuccess)
	if breaker.State() != StateClosed {
		t.Fatalf("state = %s, duplicate Done was counted", breaker.State())
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	for _, options := range []Options{
		{},
		{WindowSize: 1},
		{WindowSize: 1, MinimumRequests: 2},
		{WindowSize: 1, MinimumRequests: 1, FailureRatio: 2},
		{WindowSize: 1, MinimumRequests: 1, FailureRatio: 1, OpenTimeout: time.Second},
	} {
		if _, err := New(options); err == nil {
			t.Fatalf("New(%#v) error = nil", options)
		}
	}
}

func newTestBreaker(t *testing.T, options Options) *Breaker {
	t.Helper()
	if options.OpenTimeout == 0 {
		options.OpenTimeout = time.Second
	}
	if options.HalfOpenMaxRequests == 0 {
		options.HalfOpenMaxRequests = 1
	}
	breaker, err := New(options)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return breaker
}
