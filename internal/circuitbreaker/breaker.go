package circuitbreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrOpen = errors.New("circuit breaker is open")

type State uint8

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

type Outcome uint8

const (
	OutcomeIgnored Outcome = iota
	OutcomeSuccess
	OutcomeFailure
)

type Options struct {
	WindowSize          int
	MinimumRequests     int
	FailureRatio        float64
	OpenTimeout         time.Duration
	HalfOpenMaxRequests int
	Now                 func() time.Time
	OnStateChange       func(State)
}

type Breaker struct {
	mu sync.Mutex

	options Options
	state   State

	outcomes []bool
	next     int
	failures int

	openedAt       time.Time
	halfOpenActive int
	halfOpenPassed int
	generation     uint64
}

type Permit struct {
	breaker    *Breaker
	state      State
	generation uint64
	once       sync.Once
}

func New(options Options) (*Breaker, error) {
	if options.WindowSize <= 0 {
		return nil, fmt.Errorf("window size must be positive")
	}
	if options.MinimumRequests <= 0 || options.MinimumRequests > options.WindowSize {
		return nil, fmt.Errorf("minimum requests must be between one and window size")
	}
	if options.FailureRatio <= 0 || options.FailureRatio > 1 {
		return nil, fmt.Errorf("failure ratio must be in (0, 1]")
	}
	if options.OpenTimeout <= 0 {
		return nil, fmt.Errorf("open timeout must be positive")
	}
	if options.HalfOpenMaxRequests <= 0 {
		return nil, fmt.Errorf("half-open max requests must be positive")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.OnStateChange == nil {
		options.OnStateChange = func(State) {}
	}
	return &Breaker{options: options, state: StateClosed}, nil
}

func (b *Breaker) Allow() (*Permit, error) {
	b.mu.Lock()
	changed := b.advanceLocked()
	if b.state == StateOpen ||
		(b.state == StateHalfOpen && b.halfOpenActive >= b.options.HalfOpenMaxRequests) {
		state := b.state
		b.mu.Unlock()
		if changed {
			b.options.OnStateChange(state)
		}
		return nil, ErrOpen
	}
	if b.state == StateHalfOpen {
		b.halfOpenActive++
	}
	permit := &Permit{breaker: b, state: b.state, generation: b.generation}
	state := b.state
	b.mu.Unlock()
	if changed {
		b.options.OnStateChange(state)
	}
	return permit, nil
}

func (b *Breaker) State() State {
	b.mu.Lock()
	changed := b.advanceLocked()
	state := b.state
	b.mu.Unlock()
	if changed {
		b.options.OnStateChange(state)
	}
	return state
}

func (b *Breaker) Available() bool {
	b.mu.Lock()
	changed := b.advanceLocked()
	available := b.state == StateClosed ||
		(b.state == StateHalfOpen && b.halfOpenActive < b.options.HalfOpenMaxRequests)
	state := b.state
	b.mu.Unlock()
	if changed {
		b.options.OnStateChange(state)
	}
	return available
}

func (p *Permit) Done(outcome Outcome) {
	if p == nil || p.breaker == nil {
		return
	}
	p.once.Do(func() {
		p.breaker.complete(p.state, p.generation, outcome)
	})
}

func (b *Breaker) complete(permitState State, generation uint64, outcome Outcome) {
	b.mu.Lock()
	if generation != b.generation || permitState != b.state {
		b.mu.Unlock()
		return
	}

	changed := false
	switch b.state {
	case StateClosed:
		if outcome != OutcomeIgnored {
			b.recordLocked(outcome == OutcomeSuccess)
			if len(b.outcomes) >= b.options.MinimumRequests &&
				float64(b.failures)/float64(len(b.outcomes)) >= b.options.FailureRatio {
				b.transitionLocked(StateOpen)
				changed = true
			}
		}
	case StateHalfOpen:
		if b.halfOpenActive > 0 {
			b.halfOpenActive--
		}
		switch outcome {
		case OutcomeFailure:
			b.transitionLocked(StateOpen)
			changed = true
		case OutcomeSuccess:
			b.halfOpenPassed++
			if b.halfOpenPassed >= b.options.HalfOpenMaxRequests {
				b.transitionLocked(StateClosed)
				changed = true
			}
		}
	}
	state := b.state
	b.mu.Unlock()
	if changed {
		b.options.OnStateChange(state)
	}
}

func (b *Breaker) advanceLocked() bool {
	if b.state != StateOpen || b.options.Now().Sub(b.openedAt) < b.options.OpenTimeout {
		return false
	}
	b.transitionLocked(StateHalfOpen)
	return true
}

func (b *Breaker) transitionLocked(state State) {
	b.state = state
	b.generation++
	b.halfOpenActive = 0
	b.halfOpenPassed = 0
	switch state {
	case StateClosed:
		b.outcomes = nil
		b.next = 0
		b.failures = 0
	case StateOpen:
		b.openedAt = b.options.Now()
	}
}

func (b *Breaker) recordLocked(success bool) {
	if len(b.outcomes) < b.options.WindowSize {
		b.outcomes = append(b.outcomes, success)
		if !success {
			b.failures++
		}
		return
	}
	if !b.outcomes[b.next] {
		b.failures--
	}
	b.outcomes[b.next] = success
	if !success {
		b.failures++
	}
	b.next = (b.next + 1) % b.options.WindowSize
}
