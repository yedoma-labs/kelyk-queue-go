package kelyk

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInvalidOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  Option
	}{
		{"zero concurrency", WithConcurrency(0)},
		{"negative concurrency", WithConcurrency(-1)},
		{"negative task timeout", WithTaskTimeout(-time.Second)},
		{"zero max pending", WithMaxPending(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := New(tt.opt)
			ran := false
			err := runVoid(q, context.Background(), "k", func(context.Context) error {
				ran = true
				return nil
			})
			if !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("Run error = %v, want ErrInvalidOption", err)
			}
			if ran {
				t.Fatal("task ran with an invalid option")
			}
			if err := q.Close(context.Background()); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("Close error = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestRunReturnsTaskResult(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name    string
		task    func(context.Context) (int, error)
		wantVal int
		wantErr error
	}{
		{"value", func(context.Context) (int, error) { return 42, nil }, 42, nil},
		{"error is passed through", func(context.Context) (int, error) { return 7, boom }, 7, boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := New(WithConcurrency(2), WithTaskTimeout(time.Second), WithMaxPending(5))
			got, err := Run(q, context.Background(), "k", tt.task)
			if err != tt.wantErr {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.wantVal {
				t.Fatalf("value = %d, want %d", got, tt.wantVal)
			}
		})
	}
}

func TestInvalidKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"empty", "", true},
		{"257 characters", strings.Repeat("a", 257), true},
		{"256 characters", strings.Repeat("a", 256), false},
		{"256 runes of 2 bytes each", strings.Repeat("ä", 256), false},
		{"257 runes of 2 bytes each", strings.Repeat("ä", 257), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := New()
			ran := false
			err := runVoid(q, context.Background(), tt.key, func(context.Context) error {
				ran = true
				return nil
			})
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidKey) {
					t.Fatalf("error = %v, want ErrInvalidKey", err)
				}
				if ran {
					t.Fatal("task ran with an invalid key")
				}
				if n := q.keyCount(); n != 0 {
					t.Fatalf("keyCount = %d after invalid key, want 0", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ran {
				t.Fatal("task did not run")
			}
		})
	}
}

func TestFIFOPerKey(t *testing.T) {
	for _, n := range []int{1, 2, 10, 100} {
		t.Run(fmt.Sprintf("tasks=%d", n), func(t *testing.T) {
			ctx := context.Background()
			q := New()
			gate := make(chan struct{})

			var mu sync.Mutex
			var order []int
			var inFlight atomic.Int32
			var overlap atomic.Bool
			record := func(i int) func(context.Context) error {
				return func(context.Context) error {
					if inFlight.Add(1) != 1 {
						overlap.Store(true)
					}
					mu.Lock()
					order = append(order, i)
					mu.Unlock()
					inFlight.Add(-1)
					return nil
				}
			}

			errs := make([]<-chan error, n)
			errs[0] = startAsync(q, ctx, "order", func(c context.Context) error {
				<-gate
				return record(0)(c)
			})
			waitFor(t, "first task running", func() bool { return q.Stats().Active == 1 })
			for i := 1; i < n; i++ {
				errs[i] = startAsync(q, ctx, "order", record(i))
				waitFor(t, fmt.Sprintf("task %d queued", i), func() bool { return q.Stats().Pending == i })
			}

			close(gate)
			for i, ch := range errs {
				if err := receive(t, ch, fmt.Sprintf("task %d", i)); err != nil {
					t.Fatalf("task %d: unexpected error: %v", i, err)
				}
			}

			want := make([]int, n)
			for i := range want {
				want[i] = i
			}
			if !slices.Equal(order, want) {
				t.Fatalf("execution order = %v, want %v", order, want)
			}
			if overlap.Load() {
				t.Fatal("two tasks of the same key ran at the same time")
			}
		})
	}
}

func TestDifferentKeysRunInParallel(t *testing.T) {
	for _, n := range []int{2, 5, 10} {
		t.Run(fmt.Sprintf("keys=%d", n), func(t *testing.T) {
			ctx := context.Background()
			q := New()
			arrived := make(chan struct{}, n)
			release := make(chan struct{})

			errs := make([]<-chan error, n)
			for i := range n {
				errs[i] = startAsync(q, ctx, fmt.Sprintf("key-%d", i), func(context.Context) error {
					arrived <- struct{}{}
					<-release
					return nil
				})
			}
			// Every task must reach the barrier while the others still wait on it.
			for range n {
				receive(t, arrived, "all keys running at the same time")
			}
			close(release)
			for i, ch := range errs {
				if err := receive(t, ch, fmt.Sprintf("key-%d", i)); err != nil {
					t.Fatalf("key-%d: unexpected error: %v", i, err)
				}
			}
		})
	}
}

func TestConcurrencyLimitNeverExceeded(t *testing.T) {
	const tasks = 30
	for _, limit := range []int{1, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			ctx := context.Background()
			q := New(WithConcurrency(limit))
			var running, peak atomic.Int32
			errc := make(chan error, tasks)

			for i := range tasks {
				go func() {
					errc <- runVoid(q, ctx, fmt.Sprintf("key-%d", i), func(context.Context) error {
						cur := running.Add(1)
						for {
							p := peak.Load()
							if cur <= p || peak.CompareAndSwap(p, cur) {
								break
							}
						}
						time.Sleep(time.Millisecond)
						running.Add(-1)
						return nil
					})
				}()
			}
			for range tasks {
				if err := receive(t, errc, "task result"); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			if got := peak.Load(); got > int32(limit) {
				t.Fatalf("peak concurrency = %d, limit %d", got, limit)
			}
			if peak.Load() < 1 {
				t.Fatal("no task ran")
			}
		})
	}
}

func TestConcurrencyLimitDefersExtraTasks(t *testing.T) {
	ctx := context.Background()
	q := New(WithConcurrency(2))
	gate := make(chan struct{})
	block := func(context.Context) error {
		<-gate
		return nil
	}

	errA := startAsync(q, ctx, "a", block)
	errB := startAsync(q, ctx, "b", block)
	waitFor(t, "two tasks running", func() bool { return q.Stats().Active == 2 })

	var started atomic.Bool
	errC := startAsync(q, ctx, "c", func(context.Context) error {
		started.Store(true)
		return nil
	})
	waitFor(t, "third task queued", func() bool { return q.Stats().Pending == 1 })
	if started.Load() {
		t.Fatal("third task started while the limit of 2 was reached")
	}

	close(gate)
	for _, ch := range []<-chan error{errA, errB, errC} {
		if err := receive(t, ch, "task result"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if !started.Load() {
		t.Fatal("third task never started")
	}
}

func TestTaskErrorDoesNotBlockFollowers(t *testing.T) {
	ctx := context.Background()
	q := New()
	boom := errors.New("boom")
	gate := make(chan struct{})

	first := startAsync(q, ctx, "order", func(context.Context) error {
		<-gate
		return boom
	})
	waitFor(t, "first task running", func() bool { return q.Stats().Active == 1 })
	second := startAsync(q, ctx, "order", noop)
	waitFor(t, "second task queued", func() bool { return q.Stats().Pending == 1 })

	close(gate)
	if err := receive(t, first, "first task"); !errors.Is(err, boom) {
		t.Fatalf("first task error = %v, want boom", err)
	}
	if err := receive(t, second, "second task"); err != nil {
		t.Fatalf("second task error = %v, want nil", err)
	}
}

func TestTaskTimeout(t *testing.T) {
	q := New(WithTaskTimeout(10 * time.Millisecond))
	cause := make(chan error, 1)
	err := runVoid(q, context.Background(), "k", func(c context.Context) error {
		<-c.Done()
		cause <- context.Cause(c)
		return c.Err()
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Run error = %v, want ErrTimeout", err)
	}
	if got := receive(t, cause, "task context cancellation"); !errors.Is(got, ErrTimeout) {
		t.Fatalf("task context cause = %v, want ErrTimeout", got)
	}
}

func TestNoTimeoutByDefault(t *testing.T) {
	q := New()
	err := runVoid(q, context.Background(), "k", func(c context.Context) error {
		if _, ok := c.Deadline(); ok {
			return errors.New("task context has a deadline without WithTaskTimeout")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTimeoutKeepsSlotUntilTaskReturns(t *testing.T) {
	ctx := context.Background()
	q := New(WithTaskTimeout(10 * time.Millisecond))
	release := make(chan struct{})

	// The task ignores its context, so it keeps running after the caller got ErrTimeout.
	err := runVoid(q, ctx, "k", func(c context.Context) error {
		<-c.Done()
		<-release
		return nil
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Run error = %v, want ErrTimeout", err)
	}
	if s := q.Stats(); s.Active != 1 || s.Keys != 1 {
		t.Fatalf("Stats after timeout = %+v, want Active 1 and Keys 1", s)
	}

	var second atomic.Bool
	errc := startAsync(q, ctx, "k", func(context.Context) error {
		second.Store(true)
		return nil
	})
	waitFor(t, "second task queued", func() bool { return q.Stats().Pending == 1 })
	if second.Load() {
		t.Fatal("next task of the key started while the timed out task was still running")
	}

	close(release)
	if err := receive(t, errc, "second task"); err != nil {
		t.Fatalf("second task error = %v", err)
	}
}

func TestAbortBeforeStartRemovesTask(t *testing.T) {
	q := New()
	gate := make(chan struct{})
	first := startAsync(q, context.Background(), "k", func(context.Context) error {
		<-gate
		return nil
	})
	waitFor(t, "first task running", func() bool { return q.Stats().Active == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	var ran atomic.Bool
	second := startAsync(q, ctx, "k", func(context.Context) error {
		ran.Store(true)
		return nil
	})
	waitFor(t, "second task queued", func() bool { return q.Stats().Pending == 1 })

	cancel()
	err := receive(t, second, "aborted task")
	if !errors.Is(err, ErrAborted) {
		t.Fatalf("error = %v, want ErrAborted", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want it to match context.Canceled", err)
	}
	if s := q.Stats(); s.Pending != 0 {
		t.Fatalf("Stats().Pending = %d after abort, want 0", s.Pending)
	}

	close(gate)
	if err := receive(t, first, "first task"); err != nil {
		t.Fatalf("first task error = %v", err)
	}
	if ran.Load() {
		t.Fatal("aborted task ran")
	}
	waitFor(t, "key released", func() bool { return q.keyCount() == 0 })
}

func TestAbortCancelsRunningTask(t *testing.T) {
	q := New()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	cause := make(chan error, 1)

	errc := startAsync(q, ctx, "k", func(c context.Context) error {
		close(started)
		<-c.Done()
		cause <- context.Cause(c)
		return c.Err()
	})
	receive(t, started, "task start")
	cancel()

	err := receive(t, errc, "aborted task")
	if !errors.Is(err, ErrAborted) {
		t.Fatalf("error = %v, want ErrAborted", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want it to match context.Canceled", err)
	}
	if got := receive(t, cause, "task context cancellation"); !errors.Is(got, context.Canceled) {
		t.Fatalf("task context cause = %v, want context.Canceled", got)
	}
	waitFor(t, "task returned", func() bool { return q.Stats().Active == 0 })
}

func TestRunWithCancelledContext(t *testing.T) {
	q := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ran := false
	err := runVoid(q, ctx, "k", func(context.Context) error {
		ran = true
		return nil
	})
	if !errors.Is(err, ErrAborted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want ErrAborted matching context.Canceled", err)
	}
	if ran {
		t.Fatal("task ran with a cancelled context")
	}
	if n := q.keyCount(); n != 0 {
		t.Fatalf("keyCount = %d, want 0", n)
	}
}

func TestQueueFull(t *testing.T) {
	ctx := context.Background()
	q := New(WithMaxPending(1))
	gate := make(chan struct{})

	first := startAsync(q, ctx, "k", func(context.Context) error {
		<-gate
		return nil
	})
	waitFor(t, "first task running", func() bool { return q.Stats().Active == 1 })
	second := startAsync(q, ctx, "k", noop)
	waitFor(t, "second task queued", func() bool { return q.Stats().Pending == 1 })

	for _, key := range []string{"k", "other key"} {
		var ran atomic.Bool
		err := runVoid(q, ctx, key, func(context.Context) error {
			ran.Store(true)
			return nil
		})
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("key %q: error = %v, want ErrQueueFull", key, err)
		}
		if ran.Load() {
			t.Fatalf("key %q: rejected task ran", key)
		}
	}

	close(gate)
	for _, ch := range []<-chan error{first, second} {
		if err := receive(t, ch, "queued task"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := runVoid(q, ctx, "k", noop); err != nil {
		t.Fatalf("Run after the queue drained: %v", err)
	}
}

func TestCloseIdleQueue(t *testing.T) {
	q := New()
	if err := q.Close(context.Background()); err != nil {
		t.Fatalf("Close on idle queue: %v", err)
	}
	if err := runVoid(q, context.Background(), "k", noop); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run after Close: error = %v, want ErrClosed", err)
	}
	if err := q.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseRejectsQueuedAndWaitsForRunning(t *testing.T) {
	ctx := context.Background()
	q := New()
	gate := make(chan struct{})

	running := startAsync(q, ctx, "k", func(context.Context) error {
		<-gate
		return nil
	})
	waitFor(t, "task running", func() bool { return q.Stats().Active == 1 })
	queued := startAsync(q, ctx, "k", noop)
	waitFor(t, "task queued", func() bool { return q.Stats().Pending == 1 })

	closeErr := make(chan error, 1)
	go func() { closeErr <- q.Close(ctx) }()

	if err := receive(t, queued, "queued task"); !errors.Is(err, ErrClosed) {
		t.Fatalf("queued task error = %v, want ErrClosed", err)
	}
	if err := runVoid(q, ctx, "new", noop); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run after Close: error = %v, want ErrClosed", err)
	}
	select {
	case err := <-closeErr:
		t.Fatalf("Close returned before the running task finished: %v", err)
	default:
	}

	close(gate)
	if err := receive(t, running, "running task"); err != nil {
		t.Fatalf("running task error = %v, want nil", err)
	}
	if err := receive(t, closeErr, "Close"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := q.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseDeadline(t *testing.T) {
	q := New()
	gate := make(chan struct{})
	running := startAsync(q, context.Background(), "k", func(context.Context) error {
		<-gate
		return nil
	})
	waitFor(t, "task running", func() bool { return q.Stats().Active == 1 })

	ctx, cancel := context.WithCancel(context.Background())
	closeErr := make(chan error, 1)
	go func() { closeErr <- q.Close(ctx) }()
	cancel()
	if err := receive(t, closeErr, "Close"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close error = %v, want context.Canceled", err)
	}

	close(gate)
	if err := receive(t, running, "running task"); err != nil {
		t.Fatalf("running task returned %v, want nil", err)
	}
	if err := q.Close(context.Background()); err != nil {
		t.Fatalf("Close after the task finished: %v", err)
	}
}

func TestStats(t *testing.T) {
	ctx := context.Background()
	q := New()
	if s := q.Stats(); s != (Stats{}) {
		t.Fatalf("Stats of new queue = %+v, want zero", s)
	}

	gate := make(chan struct{})
	first := startAsync(q, ctx, "a", func(context.Context) error {
		<-gate
		return nil
	})
	waitFor(t, "first task running", func() bool { return q.Stats().Active == 1 })
	second := startAsync(q, ctx, "a", noop)
	waitFor(t, "second task queued", func() bool { return q.Stats().Pending == 1 })

	if s, want := q.Stats(), (Stats{Keys: 1, Pending: 1, Active: 1}); s != want {
		t.Fatalf("Stats = %+v, want %+v", s, want)
	}

	close(gate)
	if err := receive(t, first, "first task"); err != nil {
		t.Fatalf("first task returned %v, want nil", err)
	}
	if err := receive(t, second, "second task"); err != nil {
		t.Fatalf("second task returned %v, want nil", err)
	}
	if s := q.Stats(); s != (Stats{}) {
		t.Fatalf("Stats after drain = %+v, want zero", s)
	}
}

func TestKeyStateIsReleasedWhenIdle(t *testing.T) {
	ctx := context.Background()

	t.Run("after sequential tasks", func(t *testing.T) {
		q := New()
		for i := range 20 {
			if err := runVoid(q, ctx, fmt.Sprintf("key-%d", i%5), noop); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}
		if n := q.keyCount(); n != 0 {
			t.Fatalf("keyCount = %d after all tasks finished, want 0", n)
		}
	})

	t.Run("kept while busy, released after drain", func(t *testing.T) {
		q := New()
		gate := make(chan struct{})
		first := startAsync(q, ctx, "k", func(context.Context) error {
			<-gate
			return nil
		})
		waitFor(t, "task running", func() bool { return q.Stats().Active == 1 })
		second := startAsync(q, ctx, "k", noop)
		waitFor(t, "task queued", func() bool { return q.Stats().Pending == 1 })
		if n := q.keyCount(); n != 1 {
			t.Fatalf("keyCount while busy = %d, want 1", n)
		}
		close(gate)
		if err := receive(t, first, "first task"); err != nil {
			t.Fatalf("first task returned %v, want nil", err)
		}
		if err := receive(t, second, "second task"); err != nil {
			t.Fatalf("second task returned %v, want nil", err)
		}
		if n := q.keyCount(); n != 0 {
			t.Fatalf("keyCount after drain = %d, want 0", n)
		}
	})

	t.Run("released when the only queued task is aborted", func(t *testing.T) {
		q := New(WithConcurrency(1))
		gate := make(chan struct{})
		first := startAsync(q, ctx, "k", func(context.Context) error {
			<-gate
			return nil
		})
		waitFor(t, "task running", func() bool { return q.Stats().Active == 1 })

		// The limit of 1 keeps the task on the second key queued. It is the only task of that key.
		abortCtx, cancel := context.WithCancel(ctx)
		second := startAsync(q, abortCtx, "other", noop)
		waitFor(t, "task queued", func() bool { return q.Stats().Pending == 1 })
		cancel()
		if err := receive(t, second, "aborted task"); !errors.Is(err, ErrAborted) {
			t.Fatalf("aborted task returned %v, want ErrAborted", err)
		}
		if n := q.keyCount(); n != 1 {
			t.Fatalf("keyCount = %d, want 1 (only the running key)", n)
		}
		close(gate)
		receive(t, first, "first task")
		if n := q.keyCount(); n != 0 {
			t.Fatalf("keyCount after drain = %d, want 0", n)
		}
	})

	t.Run("released after a timed out task returns", func(t *testing.T) {
		q := New(WithTaskTimeout(10 * time.Millisecond))
		release := make(chan struct{})
		err := runVoid(q, ctx, "k", func(c context.Context) error {
			<-c.Done()
			<-release
			return nil
		})
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("Run error = %v, want ErrTimeout", err)
		}
		if n := q.keyCount(); n != 1 {
			t.Fatalf("keyCount while the timed out task runs = %d, want 1", n)
		}
		close(release)
		waitFor(t, "key released", func() bool { return q.keyCount() == 0 })
	})

	t.Run("released after close", func(t *testing.T) {
		q := New()
		gate := make(chan struct{})
		first := startAsync(q, ctx, "k", func(context.Context) error {
			<-gate
			return nil
		})
		waitFor(t, "task running", func() bool { return q.Stats().Active == 1 })
		queued := startAsync(q, ctx, "k", noop)
		waitFor(t, "task queued", func() bool { return q.Stats().Pending == 1 })

		closeErr := make(chan error, 1)
		go func() { closeErr <- q.Close(ctx) }()
		receive(t, queued, "queued task")
		close(gate)
		receive(t, first, "first task")
		if err := receive(t, closeErr, "Close"); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if n := q.keyCount(); n != 0 {
			t.Fatalf("keyCount after close = %d, want 0", n)
		}
	})
}
