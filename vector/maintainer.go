package vector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
)

// defaultInterval is the time that a running maintainer waits between two rounds, unless an
// option says otherwise.
const defaultInterval = time.Second

// maxPasses is the number of passes that Maintain gives up after. A pass that does something is
// followed by another, which can only go on this long if there is a mistake in the maintainer.
const maxPasses = 10000

// runRecount is the number of partitions that a round of Run counts exactly.
const runRecount = 16

// Source is a store whose vector indexes a maintainer maintains, like an ext store or a record
// store.
type Source interface {
	// Extensions returns the extensions of the store.
	Extensions() []any
}

// Maintainer keeps the partitions of the partitioned vector indexes of some stores: it places the
// vectors that were written in the partitions of the nearest centroids, splits a partition that
// has more vectors than its size, and dissolves one that has few left. A database should have
// one running. The indexes are right without it — every search reads the unplaced vectors — but a
// search takes longer the more there are of them.
//
// It works in transactions of its own, a batch of vectors or a partition at a time, while the
// records are written. It never changes a record.
type Maintainer struct {
	db       kv.DB
	sources  []Source
	interval time.Duration
	onError  func(error)
	mu       sync.Mutex

	// What a maintainer that was spawned is stopped with, and what tells that it has stopped.
	spawned sync.Mutex
	cancel  context.CancelFunc
	stopped chan struct{}
}

// MaintainerOption is an option of a Maintainer.
type MaintainerOption func(*Maintainer)

// WithInterval sets the time that a running maintainer waits between two rounds.
func WithInterval(d time.Duration) MaintainerOption {
	return func(m *Maintainer) {
		m.interval = d
	}
}

// WithErrorHandler sets the function that is told of what a round of a running maintainer could
// not do. The maintainer goes on, and tries again in the next round.
func WithErrorHandler(f func(error)) MaintainerOption {
	return func(m *Maintainer) {
		m.onError = f
	}
}

// NewMaintainer creates a maintainer for the vector indexes of some stores.
func NewMaintainer(db kv.DB, sources []Source, opts ...MaintainerOption) *Maintainer {
	m := &Maintainer{
		db:       db,
		sources:  slices.Clone(sources),
		interval: defaultInterval,
		onError:  func(error) {},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// indexes returns the indexes with partitions that the sources have right now, which are found
// among their extensions in the same way every time.
func (m *Maintainer) indexes() []upkeeper {
	var found []upkeeper
	for _, source := range m.sources {
		if source == nil {
			continue
		}
		for _, ext := range source.Extensions() {
			if u, ok := ext.(upkept); ok && u.upkeeper().upkeepPartitioned() {
				found = append(found, u.upkeeper())
			}
		}
	}
	return found
}

// Maintain does everything that there is to do now, for every index, and returns once there is
// nothing left: the vectors are placed, and the partitions have no more vectors than their size
// and no fewer than an eighth of it. The indexes take turns being maintained, and the errors
// of the ones that could not be are joined.
func (m *Maintainer) Maintain(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, u := range m.indexes() {
		if err := m.settle(ctx, u); err != nil {
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
		}
	}
	return errors.Join(errs...)
}

// settle does passes over an index until one does nothing.
func (m *Maintainer) settle(ctx context.Context, u upkeeper) error {
	for range maxPasses {
		if err := ctx.Err(); err != nil {
			return err
		}
		did, err := u.upkeepPass(ctx, m.db, -1)
		if err != nil {
			return fmt.Errorf("failed to maintain the index %q: %w", u.upkeepName(), err)
		}
		if !did {
			return nil
		}
	}
	return fmt.Errorf("the index %q is still not settled after %d passes", u.upkeepName(), maxPasses)
}

// round does one pass over every index, with a few of the partitions counted.
func (m *Maintainer) round(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, u := range m.indexes() {
		if _, err := u.upkeepPass(ctx, m.db, runRecount); err != nil {
			errs = append(errs, fmt.Errorf("failed to maintain the index %q: %w", u.upkeepName(), err))
		}
	}
	return errors.Join(errs...)
}

// Run maintains again and again, waiting for the interval between two rounds, until the context
// is done. It returns the error of the context. A round counts a few of the partitions of an
// index, so that the counts, which only grow while records are removed, are made right in turn.
func (m *Maintainer) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		if err := m.round(ctx); err != nil && ctx.Err() == nil {
			m.onError(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Spawn runs the maintainer in the background, as Run does, until Shutdown is called or the
// context is done. A maintainer that is running already is left running, and one that was
// shut down can be spawned again.
func (m *Maintainer) Spawn(ctx context.Context) {
	m.spawned.Lock()
	defer m.spawned.Unlock()

	if m.stopped != nil {
		select {
		case <-m.stopped:
			// It has stopped by itself, with its context.
		default:
			return
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	m.cancel, m.stopped = cancel, stopped
	go func() {
		defer close(stopped)
		defer cancel()
		_ = m.Run(ctx)
	}()
}

// Shutdown stops a maintainer that was spawned, and returns once it has stopped. The maintainer
// is not cut off: the transaction that it is in is finished, and what is left to do is left for
// the next maintainer, so no partition is half changed.
//
// If the context is done before the maintainer has stopped, Shutdown returns the error of the
// context; the maintainer still stops after its transaction. Shutting down a maintainer that is
// not running does nothing.
func (m *Maintainer) Shutdown(ctx context.Context) error {
	m.spawned.Lock()
	cancel, stopped := m.cancel, m.stopped
	m.spawned.Unlock()

	if stopped == nil {
		return nil
	}
	cancel()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
