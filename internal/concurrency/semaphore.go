package concurrency

import (
	"context"
	"fmt"
	"sync"
)

type Semaphore struct {
	slots chan struct{}
}

type Permit struct {
	semaphore *Semaphore
	once      sync.Once
}

func New(maxConcurrent int) (*Semaphore, error) {
	if maxConcurrent <= 0 {
		return nil, fmt.Errorf("max concurrent requests must be positive")
	}
	return &Semaphore{slots: make(chan struct{}, maxConcurrent)}, nil
}

func (s *Semaphore) Acquire(ctx context.Context) (*Permit, error) {
	select {
	case s.slots <- struct{}{}:
		return &Permit{semaphore: s}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Permit) Release() {
	if p == nil || p.semaphore == nil {
		return
	}
	p.once.Do(func() {
		<-p.semaphore.slots
	})
}

func (s *Semaphore) InFlight() int {
	return len(s.slots)
}

func (s *Semaphore) Capacity() int {
	return cap(s.slots)
}
