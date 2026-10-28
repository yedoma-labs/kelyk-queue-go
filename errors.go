package kelyk

import "errors"

var (
	// ErrInvalidKey means the key is empty or longer than 256 characters.
	ErrInvalidKey = errors.New("kelyk: invalid key")

	// ErrInvalidOption means an option passed to New has an invalid value. Run and Close return
	// it until the queue is fixed, because New cannot return an error.
	ErrInvalidOption = errors.New("kelyk: invalid option")

	// ErrQueueFull means the number of tasks waiting to start has reached WithMaxPending.
	ErrQueueFull = errors.New("kelyk: queue is full")

	// ErrTimeout means the task did not finish within WithTaskTimeout. Its context is cancelled.
	// The task may still be running when Run returns; its concurrency slot is held until it
	// returns.
	ErrTimeout = errors.New("kelyk: task timed out")

	// ErrAborted means the caller's context ended before the task finished. Run wraps the
	// context error, so errors.Is also matches context.Canceled or context.DeadlineExceeded.
	ErrAborted = errors.New("kelyk: task aborted")

	// ErrClosed means the queue was closed. It is returned by Run for new calls and for tasks
	// that had not started when Close was called.
	ErrClosed = errors.New("kelyk: queue is closed")
)
