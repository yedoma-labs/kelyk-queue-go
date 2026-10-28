package kelyk

import (
	"fmt"
	"time"
)

const defaultMaxPending = 10000

// Option configures a Queue. Options are applied by New.
type Option func(*config)

type config struct {
	concurrency int           // 0 means unlimited
	taskTimeout time.Duration // 0 means no timeout
	maxPending  int
	err         error // first invalid option, reported by Run and Close
}

func (c *config) fail(format string, args ...any) {
	if c.err == nil {
		c.err = fmt.Errorf("%w: %s", ErrInvalidOption, fmt.Sprintf(format, args...))
	}
}

// WithConcurrency limits how many tasks run at the same time across all keys. The default is
// unlimited. n must be greater than 0.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n <= 0 {
			c.fail("concurrency must be greater than 0, got %d", n)
			return
		}
		c.concurrency = n
	}
}

// WithTaskTimeout sets the default timeout for each task. The clock starts when the task starts,
// not when it is queued. The default is 0, which means no timeout. d must not be negative.
func WithTaskTimeout(d time.Duration) Option {
	return func(c *config) {
		if d < 0 {
			c.fail("task timeout must not be negative, got %s", d)
			return
		}
		c.taskTimeout = d
	}
}

// WithMaxPending limits how many tasks may wait to start across all keys. Run returns
// ErrQueueFull when the limit is reached. The default is 10000. n must be greater than 0.
func WithMaxPending(n int) Option {
	return func(c *config) {
		if n <= 0 {
			c.fail("max pending must be greater than 0, got %d", n)
			return
		}
		c.maxPending = n
	}
}
