package retry

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

type Attempt struct {
	Number   int
	Duration time.Duration
	Err      error
}

type Options struct {
	MaxAttempts    int
	AttemptTimeout time.Duration
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	Jitter         func(time.Duration) time.Duration
	OnAttempt      func(Attempt)
}

type Policy struct {
	options Options
}

func New(options Options) (*Policy, error) {
	if options.MaxAttempts <= 0 {
		return nil, fmt.Errorf("max attempts must be positive")
	}
	if options.AttemptTimeout <= 0 {
		return nil, fmt.Errorf("attempt timeout must be positive")
	}
	if options.BaseBackoff <= 0 || options.MaxBackoff < options.BaseBackoff {
		return nil, fmt.Errorf("backoff values must be positive and max must not be below base")
	}
	if options.Jitter == nil {
		options.Jitter = fullJitter
	}
	if options.OnAttempt == nil {
		options.OnAttempt = func(Attempt) {}
	}
	return &Policy{options: options}, nil
}

func (p *Policy) Do(
	ctx context.Context,
	shouldRetry func(error) bool,
	operation func(context.Context) error,
) error {
	cancel, err := p.do(ctx, shouldRetry, operation, false)
	if cancel != nil {
		cancel()
	}
	return err
}

// DoRetained transfers the successful attempt's CancelFunc to the caller.
// It is intended for streams whose lifetime continues after establishment.
func (p *Policy) DoRetained(
	ctx context.Context,
	shouldRetry func(error) bool,
	operation func(context.Context) error,
) (context.CancelFunc, error) {
	return p.do(ctx, shouldRetry, operation, true)
}

func (p *Policy) do(
	ctx context.Context,
	shouldRetry func(error) bool,
	operation func(context.Context) error,
	retainSuccess bool,
) (context.CancelFunc, error) {
	for attempt := 1; attempt <= p.options.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		attemptCtx, cancel := context.WithTimeout(ctx, p.options.AttemptTimeout)
		startedAt := time.Now()
		err := operation(attemptCtx)
		p.options.OnAttempt(Attempt{
			Number: attempt, Duration: time.Since(startedAt), Err: err,
		})
		if err == nil {
			if retainSuccess {
				return cancel, nil
			}
			cancel()
			return nil, nil
		}
		cancel()
		if parentErr := ctx.Err(); parentErr != nil {
			return nil, parentErr
		}
		if attempt == p.options.MaxAttempts || shouldRetry == nil || !shouldRetry(err) {
			return nil, err
		}

		delay := p.options.Jitter(p.backoff(attempt))
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, nil
}

func (p *Policy) backoff(failedAttempt int) time.Duration {
	delay := p.options.BaseBackoff
	for step := 1; step < failedAttempt; step++ {
		if delay >= p.options.MaxBackoff/2 {
			return p.options.MaxBackoff
		}
		delay *= 2
	}
	if delay > p.options.MaxBackoff {
		return p.options.MaxBackoff
	}
	return delay
}

func fullJitter(maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(maximum) + 1))
}
