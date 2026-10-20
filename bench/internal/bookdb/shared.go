package bookdb

import (
	"context"
	"sync"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
)

// Shared is the database that the variants of a workload on one system share: the first to run
// opens it (loads and indexes the books, as phases of the run that asked for it), and the last to
// finish closes it. It saves loading the same books once for every way of asking.
type Shared[E any] struct {
	open  func(ctx context.Context, rc *harness.RunContext) (E, error)
	close func(E)

	mu   sync.Mutex
	refs int
	env  E
	have bool
	err  error
}

// NewShared returns the database of refs variants. open is called by the first one to run, with its
// run context.
func NewShared[E any](refs int, open func(ctx context.Context, rc *harness.RunContext) (E, error), closeFn func(E)) *Shared[E] {
	return &Shared[E]{refs: refs, open: open, close: closeFn}
}

// Acquire returns the database, opening it if no variant has. An error of opening is returned to
// every variant, without trying again.
func (s *Shared[E]) Acquire(ctx context.Context, rc *harness.RunContext) (E, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.have && s.err == nil {
		s.env, s.err = s.open(ctx, rc)
		s.have = s.err == nil
	}
	return s.env, s.err
}

// Release says that a variant is done, whether it used the database or not. The last one closes it.
func (s *Shared[E]) Release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs--
	if s.refs <= 0 && s.have {
		s.close(s.env)
		var zero E
		s.env, s.have = zero, false
	}
}

// Phase measures a load or a build as a phase of the run, or just runs it when the run has nowhere
// to write results to, as in the verification.
func Phase(ctx context.Context, rc *harness.RunContext, name string, fn func(ctx context.Context) (int64, error)) error {
	if rc.Sink == nil {
		_, err := fn(ctx)
		return err
	}
	return rc.Phase(ctx, harness.Case{Name: name, Concurrency: 1}, fn)
}
