package kelyk

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// testTimeout bounds every wait in the tests. A correct run never reaches it.
const testTimeout = 5 * time.Second

// waitFor polls cond until it holds. It fails the test after testTimeout. Call it only from the
// test goroutine.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for n := 0; !cond(); n++ {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		if n < 100 {
			runtime.Gosched()
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

// receive returns the next value from ch. It fails the test after testTimeout. Call it only from
// the test goroutine.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// runVoid calls Run for a task without a result.
func runVoid(q *Queue, ctx context.Context, key string, task func(context.Context) error) error {
	_, err := Run(q, ctx, key, func(c context.Context) (struct{}, error) {
		return struct{}{}, task(c)
	})
	return err
}

// startAsync calls runVoid in a goroutine. The returned channel receives its error.
func startAsync(q *Queue, ctx context.Context, key string, task func(context.Context) error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- runVoid(q, ctx, key, task) }()
	return ch
}

// noop is a task that returns nil at once.
func noop(context.Context) error { return nil }
