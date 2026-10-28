# kelyk-queue-go

A keyed task queue for Go. Tasks with the same key run strictly in FIFO order and never overlap.
Tasks with different keys run in parallel. Standard library only.

## The problem

Asynchronous events often belong together. All updates of one order, for example, must be applied
in the order they were sent. Run them in parallel and two updates race: the later one may be
written first, or both read the same state and overwrite each other.

Serializing everything fixes the race but wastes throughput. A plain mutex per key helps with
ordering, but it has no queue. Waiters compete for the lock in any order, nothing bounds the total
concurrency, and nothing times out.

This library provides:

- **Order per key.** Tasks with the same key run in the order they were queued, one at a time.
- **Parallelism across keys.** Tasks with different keys run side by side.
- **A global limit.** `WithConcurrency` caps the number of tasks running at the same time.
- **Timeouts and cancellation.** A task gets a context that ends on timeout or when the caller
  gives up.
- **Backpressure.** `WithMaxPending` bounds the queue, and a full queue fails at once.
- **No leaks.** A key is removed from memory as soon as it has no queued or running tasks.

## Installation

```sh
go get github.com/yedoma-labs/kelyk-queue-go
```

Requires Go 1.23 or newer.

## Quickstart

Apply the updates of each order in order, and let different orders proceed in parallel:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/yedoma-labs/kelyk-queue-go"
)

func main() {
	ctx := context.Background()
	q := kelyk.New(
		kelyk.WithConcurrency(8),               // at most 8 tasks at a time, across all orders
		kelyk.WithTaskTimeout(5*time.Second),  // each task gets 5 seconds
	)
	defer q.Close(ctx)

	// Updates of order 42 run one after another, in the order they were queued.
	// Updates of other orders run in parallel with them.
	status, err := kelyk.Run(q, ctx, "order:42", func(ctx context.Context) (string, error) {
		if err := saveStatus(ctx, 42, "shipped"); err != nil {
			return "", err
		}
		return "shipped", nil
	})
	if err != nil {
		fmt.Println("update failed:", err)
		return
	}
	fmt.Println("order 42 is", status)
}

func saveStatus(ctx context.Context, orderID int, status string) error {
	// Write to your database here. Respect ctx so that timeouts take effect.
	return nil
}
```

`Run` is a generic function, because Go methods cannot have type parameters. For a task that
returns nothing, use `struct{}` as the type argument.

## Semantics

| Topic | Behavior |
| --- | --- |
| Order | Tasks with the same key start in queue order and never overlap. Each task starts only after the previous one for its key has returned. |
| Parallelism | Tasks with different keys run in parallel, up to `WithConcurrency` in total. Unlimited by default. |
| Task error | Returned to the caller of that task only. It does not stop later tasks of the same key. |
| Timeout | `WithTaskTimeout` sets a default for every task. The clock starts when the task starts. The task's context is cancelled, and the caller gets `ErrTimeout`. The task may still be running; its key and slot stay busy until it returns. |
| Caller cancel | A queued task is removed and never runs. A running task gets its context cancelled. The caller gets `ErrAborted`, which also matches the context error with `errors.Is`. |
| Queue full | When `WithMaxPending` tasks are waiting, `Run` returns `ErrQueueFull` at once. Default: 10000. |
| Close | `Close(ctx)` rejects new calls and queued tasks with `ErrClosed`, then waits for running tasks. If `ctx` ends first, it returns `ctx.Err()`. |
| Memory | A key is removed as soon as it has no queued or running tasks. |
| Key | Must have 1 to 256 characters. Otherwise `ErrInvalidKey`. |
| Options | `WithConcurrency` and `WithMaxPending` must be greater than 0, and `WithTaskTimeout` must not be negative. Otherwise `Run` and `Close` return `ErrInvalidOption`. |
| Monitoring | `Stats()` returns `Keys`, `Pending` and `Active`. |

Errors can be checked with `errors.Is`: `ErrInvalidKey`, `ErrInvalidOption`, `ErrQueueFull`,
`ErrTimeout`, `ErrAborted` and `ErrClosed`.

Tasks should respect their context. A task that ignores it keeps its key and its concurrency slot
until it returns, even after the caller has received `ErrTimeout` or `ErrAborted`. A panic in a
task is not recovered, so it stops the program, as with any goroutine.

## Scope: one process only

The queue lives in the memory of one process. It orders tasks inside that process only. Several
instances of your service each have their own queue, so tasks for the same key can still run in
parallel across instances. The queue does not provide a distributed lock.

If the work must be serialized across instances, use a distributed lock such as a Redis lock
(for example `SET key value NX PX ttl`, or a library such as redsync), and keep the critical
section short. The queue can still help inside each instance to bound concurrency and order work.

## Development

```sh
gofmt -l .
go vet ./...
go test -race ./...
```

## License

MIT. See [LICENSE](LICENSE).
