package concurrency

import (
	"context"
	"testing"
)

var benchmarkInFlight int

func BenchmarkProviderConcurrencyLimiter(b *testing.B) {
	semaphore, err := New(64)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		permit, err := semaphore.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		permit.Release()
	}
	benchmarkInFlight = semaphore.InFlight()
}

func BenchmarkProviderConcurrencyLimiterParallel(b *testing.B) {
	semaphore, err := New(64)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			permit, err := semaphore.Acquire(ctx)
			if err != nil {
				b.Error(err)
				return
			}
			permit.Release()
		}
	})
	benchmarkInFlight = semaphore.InFlight()
}
