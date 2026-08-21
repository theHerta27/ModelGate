package concurrency

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSemaphoreCapacityAndReleaseOnce(t *testing.T) {
	semaphore, err := New(1)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	permit, err := semaphore.Acquire(context.Background())
	if err != nil || semaphore.InFlight() != 1 || semaphore.Capacity() != 1 {
		t.Fatalf("Acquire()/counts = %v/%d/%d", err, semaphore.InFlight(), semaphore.Capacity())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := semaphore.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked Acquire() error = %v", err)
	}

	permit.Release()
	permit.Release()
	if semaphore.InFlight() != 0 {
		t.Fatalf("in flight = %d after release", semaphore.InFlight())
	}
	second, err := semaphore.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second Acquire() error = %v", err)
	}
	second.Release()
}

func TestNewRejectsInvalidCapacity(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("New(0) error = nil")
	}
}
