package upkeep

import (
	"context"
	"time"
)

// SetNow replaces the clock that the back-off and Every are measured with, so that the tests do
// not have to wait for them. It must be called before the runner runs.
func (r *Runner) SetNow(now func() time.Time) {
	r.now = now
}

// Round does one round of what a running runner does, without waiting for the interval after it.
func (r *Runner) Round(ctx context.Context) error {
	return r.round(ctx, false)
}
