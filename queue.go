// Package kelyk runs tasks asynchronously with strict FIFO ordering per key.
//
// Tasks with the same key never overlap and run in the order they were queued. Tasks with
// different keys run in parallel, limited by WithConcurrency. The queue lives in one process
// and does not coordinate with other instances.
package kelyk

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Queue serializes tasks per key. Create one with New. A Queue is safe for concurrent use.
type Queue struct {
	cfg config // set by New, read-only afterwards

	mu      sync.Mutex
	keys    map[string]*keyState // keys with queued or running tasks
	ready   []*keyState          // keys with queued tasks that are not running; may hold stale entries
	pending int                  // tasks queued and not started
	active  int                  // tasks started and not yet returned
	closed  bool
	idle    chan struct{} // closed once the queue is closed and active drops to 0; nil otherwise
}

// Stats is a point-in-time snapshot of the queue, intended for monitoring.
type Stats struct {
	// Keys is the number of keys with queued or running tasks.
	Keys int
	// Pending is the number of tasks waiting to start.
	Pending int
	// Active is the number of tasks that have started and not yet returned.
	Active int
}

// keyState holds the FIFO of one key. It is removed from Queue.keys as soon as it is idle.
type keyState struct {
	key     string
	jobs    []*job // queued tasks that have not started, oldest first
	running bool   // a task of this key is running
}

// job is one call to Run. Its fields are guarded by Queue.mu, except exec, which is set once.
type job struct {
	key       string
	callerCtx context.Context
	exec      func(ctx context.Context) error

	st      *keyState
	started bool
	ctx     context.Context
	cancel  context.CancelCauseFunc
	timer   *time.Timer

	settled  bool  // done has been closed
	queueErr error // set when the caller gets a queue-level error (timeout, abort, close)
	taskErr  error // error returned by the task, valid if queueErr is nil
	done     chan struct{}
}

// New returns an empty queue configured by opts. Without options the queue has unlimited
// concurrency, no task timeout and room for 10000 pending tasks. Invalid options are reported by
// Run and Close with ErrInvalidOption.
func New(opts ...Option) *Queue {
	cfg := config{maxPending: defaultMaxPending}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return &Queue{cfg: cfg, keys: make(map[string]*keyState)}
}

// Run queues task under key and waits for its result.
//
// Tasks with the same key run one after another in FIFO order. Run returns the task's result
// and error. The task receives a context that is cancelled on timeout, when ctx ends, or when
// Close gives up. The task must respect that context.
//
// Run returns early with an error when:
//   - the key is invalid (ErrInvalidKey) or the queue options are invalid (ErrInvalidOption),
//   - the queue is full (ErrQueueFull) or closed (ErrClosed),
//   - the task times out (ErrTimeout), or
//   - ctx ends before the task finishes (ErrAborted, wrapping the context error).
//
// A queued task that is aborted is removed and never runs. A running task keeps its concurrency
// slot until it returns, even after an early return from Run.
//
// Run is a function and not a method because methods cannot have type parameters. Use
// struct{} as T for tasks that return nothing.
func Run[T any](q *Queue, ctx context.Context, key string, task func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if q.cfg.err != nil {
		return zero, q.cfg.err
	}
	if err := validateKey(key); err != nil {
		return zero, err
	}

	var result T
	j := &job{
		key:       key,
		callerCtx: ctx,
		done:      make(chan struct{}),
		exec: func(taskCtx context.Context) error {
			v, err := task(taskCtx)
			result = v
			return err
		},
	}
	if err := q.enqueue(j); err != nil {
		return zero, err
	}

	select {
	case <-j.done:
	case <-ctx.Done():
		q.abort(j, ctx.Err())
		<-j.done
	}

	// j.done is closed, so queueErr and taskErr are final. result is only read when the task
	// itself finished, because otherwise the task may still be writing it.
	if j.queueErr != nil {
		return zero, j.queueErr
	}
	return result, j.taskErr
}

// Stats returns a snapshot of the queue state.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return Stats{Keys: len(q.keys), Pending: q.pending, Active: q.active}
}

// Close rejects new tasks and tasks that have not started yet, then waits for running tasks to
// return. Such tasks fail with ErrClosed. If ctx ends first, Close returns ctx.Err() and the
// running tasks keep running. Close may be called more than once.
func (q *Queue) Close(ctx context.Context) error {
	if q.cfg.err != nil {
		return q.cfg.err
	}

	q.mu.Lock()
	if !q.closed {
		q.closed = true
		for key, st := range q.keys {
			for _, j := range st.jobs {
				q.settle(j, ErrClosed)
			}
			q.pending -= len(st.jobs)
			st.jobs = nil
			if !st.running {
				delete(q.keys, key)
			}
		}
		q.ready = nil
	}
	if q.active == 0 {
		q.mu.Unlock()
		return nil
	}
	if q.idle == nil {
		q.idle = make(chan struct{})
	}
	idle := q.idle
	q.mu.Unlock()

	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueue adds j to its key's FIFO and starts it if the queue has room.
func (q *Queue) enqueue(j *job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}
	if err := j.callerCtx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrAborted, err)
	}
	if q.pending >= q.cfg.maxPending {
		return ErrQueueFull
	}

	st := q.keys[j.key]
	if st == nil {
		st = &keyState{key: j.key}
		q.keys[j.key] = st
	}
	if !st.running && len(st.jobs) == 0 {
		q.ready = append(q.ready, st)
	}
	j.st = st
	st.jobs = append(st.jobs, j)
	q.pending++
	q.schedule()
	return nil
}

// schedule starts the first queued task of ready keys while there are free slots. The caller
// must hold q.mu. It never runs user code.
func (q *Queue) schedule() {
	for q.cfg.concurrency == 0 || q.active < q.cfg.concurrency {
		if len(q.ready) == 0 {
			return
		}
		st := q.ready[0]
		q.ready[0] = nil
		q.ready = q.ready[1:]
		if st.running || len(st.jobs) == 0 || q.keys[st.key] != st {
			continue // stale entry
		}

		j := st.jobs[0]
		st.jobs[0] = nil
		st.jobs = st.jobs[1:]
		st.running = true
		q.pending--
		q.active++
		q.start(j)
	}
}

// start prepares the task context and timer, then runs the task in its own goroutine. The
// caller must hold q.mu.
func (q *Queue) start(j *job) {
	j.started = true
	j.ctx, j.cancel = context.WithCancelCause(j.callerCtx)
	if q.cfg.taskTimeout > 0 {
		j.timer = time.AfterFunc(q.cfg.taskTimeout, func() { q.timeout(j) })
	}
	go q.execute(j)
}

// execute runs the task outside the lock and reports its result to finish.
func (q *Queue) execute(j *job) {
	q.finish(j, j.exec(j.ctx))
}

// finish records the task result, releases the key lock and the concurrency slot, and starts
// the next waiting work. If the caller has already received a queue-level error, the result is
// dropped.
func (q *Queue) finish(j *job, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	j.cancel(nil)
	if j.timer != nil {
		j.timer.Stop()
	}
	if !j.settled {
		j.settled = true
		j.taskErr = err
		close(j.done)
	}

	q.active--
	st := j.st
	st.running = false
	if len(st.jobs) > 0 {
		q.ready = append(q.ready, st)
	} else if q.keys[st.key] == st {
		delete(q.keys, st.key)
	}
	q.schedule()
	q.notifyIdle()
}

// timeout runs when the task timer fires. The caller gets ErrTimeout. The task's context is
// cancelled, and the slot stays held until the task returns.
func (q *Queue) timeout(j *job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if j.settled {
		return
	}
	q.settle(j, ErrTimeout)
	j.cancel(ErrTimeout)
}

// abort handles a caller whose context ended. A queued task is removed and never starts. A
// running task gets its context cancelled. In both cases the caller receives ErrAborted.
func (q *Queue) abort(j *job, cause error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if j.settled {
		return
	}
	abortErr := fmt.Errorf("%w: %w", ErrAborted, cause)
	if j.started {
		j.cancel(cause)
		q.settle(j, abortErr)
		return
	}
	q.removeQueued(j)
	q.settle(j, abortErr)
	q.notifyIdle()
}

// removeQueued takes a task that has not started out of its key's FIFO. The caller must hold
// q.mu.
func (q *Queue) removeQueued(j *job) {
	st := j.st
	for i, queued := range st.jobs {
		if queued == j {
			st.jobs = slices.Delete(st.jobs, i, i+1)
			q.pending--
			break
		}
	}
	if !st.running && len(st.jobs) == 0 && q.keys[st.key] == st {
		delete(q.keys, st.key)
	}
}

// settle completes the caller's Run with a queue-level error. The caller must hold q.mu and
// must make sure j is not settled yet.
func (q *Queue) settle(j *job, err error) {
	j.settled = true
	j.queueErr = err
	close(j.done)
}

// notifyIdle wakes Close once the queue is closed and no task is running. The caller must hold
// q.mu.
func (q *Queue) notifyIdle() {
	if q.closed && q.active == 0 && q.idle != nil {
		close(q.idle)
		q.idle = nil
	}
}

// keyCount returns the number of keys with queued or running tasks. Tests use it to check that
// idle keys are released.
func (q *Queue) keyCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.keys)
}
