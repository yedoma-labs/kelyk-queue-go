package kelyk

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestStressOrderAndNoOverlap queues 10000 tasks on 50 keys. Each key first runs a gate task that
// holds it busy. While the gate is closed, the test queues every task and waits until it is
// visible in Stats().Pending before it queues the next one. So the FIFO order per key is known
// exactly, and the test can check that it holds.
func TestStressOrderAndNoOverlap(t *testing.T) {
	const (
		keys    = 50
		perKey  = 200 // queued tasks per key; 10000 in total
		total   = keys * perKey
		maxWait = total + keys
	)
	ctx := context.Background()
	q := New(WithMaxPending(maxWait))
	gate := make(chan struct{})

	// seen[k] is written only by tasks of key k. Their execution is serialized by the queue, so
	// the race detector checks the ordering as well.
	seen := make([][]int, keys)
	inFlight := make([]atomic.Int32, keys)
	var overlap atomic.Bool
	task := func(k, seq int) func(context.Context) error {
		return func(context.Context) error {
			if inFlight[k].Add(1) != 1 {
				overlap.Store(true)
			}
			seen[k] = append(seen[k], seq)
			inFlight[k].Add(-1)
			return nil
		}
	}
	keyName := func(k int) string { return fmt.Sprintf("order-%d", k) }

	var wg sync.WaitGroup
	errc := make(chan error, keys+total)

	// Phase 1: a gate task per key. Sequence number 0 is the gate.
	for k := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errc <- runVoid(q, ctx, keyName(k), func(c context.Context) error {
				<-gate
				return task(k, 0)(c)
			})
		}()
	}
	waitFor(t, "all gate tasks running", func() bool { return q.Stats().Active == keys })

	// Phase 2: queue the rest. Each task is enqueued before the next one starts.
	for i := range total {
		k := i % keys
		seq := i/keys + 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			errc <- runVoid(q, ctx, keyName(k), task(k, seq))
		}()
		waitFor(t, fmt.Sprintf("task %d enqueued", i), func() bool { return q.Stats().Pending == i+1 })
	}

	// Phase 3: open the gates and wait for everything to finish.
	close(gate)
	wg.Wait()
	close(errc)

	for err := range errc {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if overlap.Load() {
		t.Fatal("two tasks of the same key ran at the same time")
	}
	for k := range keys {
		if len(seen[k]) != perKey+1 {
			t.Fatalf("key %d: %d tasks ran, want %d", k, len(seen[k]), perKey+1)
		}
		for j, seq := range seen[k] {
			if seq != j {
				t.Fatalf("key %d: execution position %d has sequence %d, want %d", k, j, seq, j)
			}
		}
	}
	if s := q.Stats(); s != (Stats{}) {
		t.Fatalf("Stats after drain = %+v, want zero", s)
	}
	if n := q.keyCount(); n != 0 {
		t.Fatalf("keyCount after drain = %d, want 0", n)
	}
	if err := q.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestConcurrentCallersAndStats runs many goroutines against one queue while another goroutine
// reads Stats. It checks that no task is lost and that the race detector stays quiet.
func TestConcurrentCallersAndStats(t *testing.T) {
	const (
		callers = 50
		perCall = 20
	)
	ctx := context.Background()
	q := New(WithConcurrency(4))

	stop := make(chan struct{})
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		for {
			select {
			case <-stop:
				return
			default:
				_ = q.Stats()
			}
		}
	}()

	var executed atomic.Int64
	errc := make(chan error, callers*perCall)
	var wg sync.WaitGroup
	for g := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range perCall {
				key := fmt.Sprintf("key-%d", (g+j)%7)
				errc <- runVoid(q, ctx, key, func(context.Context) error {
					executed.Add(1)
					return nil
				})
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-statsDone
	close(errc)

	for err := range errc {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if got := executed.Load(); got != callers*perCall {
		t.Fatalf("executed = %d, want %d", got, callers*perCall)
	}
	if s := q.Stats(); s != (Stats{}) {
		t.Fatalf("Stats after drain = %+v, want zero", s)
	}
}
