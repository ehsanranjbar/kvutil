package upkeep

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
)

// maxBackoff is the greatest number of times that the wait after a failed pass is doubled, so that
// a job that keeps failing is tried again after 64 intervals at the least often.
const maxBackoff = 6

// Runner runs the jobs of the extensions of some stores, and the jobs that are added to it. A
// database should have one runner, running in the background. The stores are right without it,
// but reading them takes longer the more there is that was not done.
//
// The work goes in rounds. A round gives every job that is due a pass, which the jobs go through
// by turns, a step each, so that a busy job does not keep the others waiting. A pass goes forward
// only, so that what keeps being written at its start does not keep it from getting to its end.
type Runner struct {
	db       kv.DB
	workers  int
	interval time.Duration
	batch    int
	onError  func(error)
	now      func() time.Time

	// mu keeps the rounds from overlapping: Drain waits for the round that is running.
	mu sync.Mutex

	// state guards what the runner is given and what it knows of the jobs.
	state   sync.Mutex
	sources []Source
	added   []added
	known   map[Job]*jobState
	order   []*jobState

	// wake is what Wake tells a running runner with. It holds one, so that a wake that is made
	// while a round is running is not lost and not repeated.
	wake chan struct{}

	// What a runner that was spawned is stopped with, and what tells that it has stopped.
	spawned sync.Mutex
	cancel  context.CancelFunc
	stopped chan struct{}
}

// added is a job that was given to the runner directly.
type added struct {
	job   Job
	every time.Duration
}

// jobState is what the runner knows of a job.
type jobState struct {
	name     string
	passes   uint64
	steps    uint64
	failures int
	err      error

	// began is the time that the last pass was started at, and failedAt the time that the last
	// failed one ended at. They are what Every and the back-off are measured from.
	began    time.Time
	failedAt time.Time
}

// run is a pass that is going on.
type run struct {
	job    Job
	state  *jobState
	cursor []byte
}

// Option is an option of a Runner.
type Option func(*Runner)

// WithWorkers sets the number of jobs that are stepped at once, which is one by default. The steps
// of one job never run at once whatever the number is. A number below one is one.
func WithWorkers(n int) Option {
	return func(r *Runner) {
		r.workers = max(n, 1)
	}
}

// WithInterval sets the time that a running runner waits between two rounds, which is
// DefaultInterval by default. An interval that is not positive is the default.
func WithInterval(d time.Duration) Option {
	return func(r *Runner) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithBatchSize sets the size of a step that the jobs are told of, in the units of the job: the
// number of notes that an aggregation takes in one transaction, for one. Zero, which is the default,
// leaves it to the jobs.
func WithBatchSize(n int) Option {
	return func(r *Runner) {
		r.batch = max(n, 0)
	}
}

// WithErrorHandler sets the function that is told of what the jobs put aside without failing, and
// of the errors of the rounds of a running runner. What is wrong with a single note, say, does not
// stop a job: the note is put aside, or left for the next round, and the job goes on.
func WithErrorHandler(f func(error)) Option {
	return func(r *Runner) {
		if f != nil {
			r.onError = f
		}
	}
}

// JobOption is an option of a job that is added to a Runner with AddJob.
type JobOption func(*jobConfig)

type jobConfig struct {
	every time.Duration
}

// Every makes a job run at most once in the time given. It runs in the first round after that time
// has passed since its last pass was started, so a time that is shorter than the interval of the
// runner is the interval. It is for work that is costly, like the maintenance of an engine.
// Drain ignores it.
func Every(d time.Duration) JobOption {
	return func(c *jobConfig) {
		c.every = d
	}
}

// New creates a runner for a database.
func New(db kv.DB, opts ...Option) *Runner {
	r := &Runner{
		db:       db,
		workers:  1,
		interval: DefaultInterval,
		onError:  func(error) {},
		now:      time.Now,
		known:    map[Job]*jobState{},
		wake:     make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Add adds stores to the runner. Their jobs are found among their extensions, the values that
// implement Provider, at the start of every round, in the order of the stores and of their
// extensions, so a store can get an extension after it is added. A source that is nil is ignored.
func (r *Runner) Add(sources ...Source) *Runner {
	r.state.Lock()
	defer r.state.Unlock()

	for _, s := range sources {
		if s != nil {
			r.sources = append(r.sources, s)
		}
	}
	return r
}

// AddJob adds a job that is not of an extension, like the maintenance of an engine. The jobs that
// are added this way come after the ones that are found in the stores.
func (r *Runner) AddJob(j Job, opts ...JobOption) *Runner {
	var c jobConfig
	for _, opt := range opts {
		opt(&c)
	}

	r.state.Lock()
	defer r.state.Unlock()

	r.added = append(r.added, added{job: j, every: c.every})
	return r
}

// find returns the jobs that the runner has right now, which are found among the extensions of
// the stores in the same way every time. A job that is found more than once is only returned the
// first time.
func (r *Runner) find() []added {
	r.state.Lock()
	sources, extra := slices.Clone(r.sources), slices.Clone(r.added)
	r.state.Unlock()

	var (
		found []added
		seen  = map[Job]bool{}
	)
	keep := func(a added) {
		if a.job == nil || seen[a.job] {
			return
		}
		seen[a.job] = true
		found = append(found, a)
	}
	for _, s := range sources {
		for _, ext := range s.Extensions() {
			if p, ok := ext.(Provider); ok {
				for _, j := range p.Jobs() {
					keep(added{job: j})
				}
			}
		}
	}
	for _, a := range extra {
		keep(a)
	}
	return found
}

// due reports whether a job is to have a pass now. The state is locked.
func (r *Runner) due(s *jobState, every time.Duration, now time.Time) bool {
	// A job that fails is left alone for longer the more it fails, so that it does not fail over
	// and over at the pace of the interval.
	if s.failures > 0 && now.Sub(s.failedAt) < r.interval<<min(s.failures-1, maxBackoff) {
		return false
	}
	return every <= 0 || s.began.IsZero() || now.Sub(s.began) >= every
}

// begin finds the jobs and starts a pass for each of those that are due. All of them are, if the
// work is drained.
func (r *Runner) begin(drain bool) []*run {
	found := r.find()
	now := r.now()

	r.state.Lock()
	defer r.state.Unlock()

	var runs []*run
	for _, a := range found {
		s, ok := r.known[a.job]
		if !ok {
			s = &jobState{name: a.job.Name()}
			r.known[a.job] = s
			r.order = append(r.order, s)
		}
		if !drain && !r.due(s, a.every, now) {
			continue
		}
		s.began = now
		runs = append(runs, &run{job: a.job, state: s})
	}
	return runs
}

// Drain does everything that is outstanding now for every job, and returns once all the passes are
// over. The jobs are done whether they are due or not: it ignores Every, and the back-off of the
// jobs that failed. The jobs take turns, and the errors of the ones that failed are joined; the
// others are done all the same. If the context is done, Drain stops between two steps and returns
// its error with the others.
func (r *Runner) Drain(ctx context.Context) error {
	return r.round(ctx, true)
}

// round gives a pass to the jobs that are due, and goes through them until they are all over.
func (r *Runner) round(ctx context.Context, drain bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	runs := r.begin(drain)
	if r.workers > 1 && len(runs) > 1 {
		return r.concurrently(ctx, runs, drain)
	}
	return r.inline(ctx, runs, drain)
}

// inline goes through the passes in the calling goroutine, a step of each by turns.
func (r *Runner) inline(ctx context.Context, runs []*run, drain bool) error {
	var errs []error
	for queue := runs; len(queue) > 0; {
		x := queue[0]
		queue = queue[1:]

		if err := ctx.Err(); err != nil {
			return errors.Join(append(errs, err)...)
		}
		more, err := r.step(ctx, x, drain)
		if err != nil {
			errs = append(errs, err)
		} else if more {
			queue = append(queue, x)
		}
	}
	return errors.Join(errs...)
}

// concurrently goes through the passes with as many goroutines as the runner has workers, which
// take the passes from one queue by turns. A pass is only in the queue while no goroutine is
// stepping it, so the steps of a job never run at once.
func (r *Runner) concurrently(ctx context.Context, runs []*run, drain bool) error {
	var (
		mu     sync.Mutex
		cond   = sync.NewCond(&mu)
		queue  = slices.Clone(runs)
		active int
		cut    bool
		errs   []error
	)

	work := func() {
		mu.Lock()
		defer mu.Unlock()
		for {
			// The round is over when no pass is waiting and none can come back to the queue.
			for len(queue) == 0 && active > 0 {
				cond.Wait()
			}
			if len(queue) == 0 {
				return
			}
			x := queue[0]
			queue = queue[1:]

			if ctx.Err() != nil {
				cut, queue = true, nil
				cond.Broadcast()
				return
			}

			active++
			mu.Unlock()
			more, err := r.step(ctx, x, drain)
			mu.Lock()
			active--

			if err != nil {
				errs = append(errs, err)
			} else if more {
				queue = append(queue, x)
			}
			cond.Broadcast()
		}
	}

	var wg sync.WaitGroup
	for range min(r.workers, len(runs)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			work()
		}()
	}
	wg.Wait()

	if cut {
		errs = append(errs, ctx.Err())
	}
	return errors.Join(errs...)
}

// step does one step of a pass and reports whether the pass goes on. The error that ends a pass
// is returned with the name of the job.
func (r *Runner) step(ctx context.Context, x *run, drain bool) (more bool, err error) {
	next, err := x.job.Step(ctx, r.db, Pass{
		Cursor: x.cursor,
		Drain:  drain,
		Batch:  r.batch,
		Report: r.onError,
	})
	x.cursor = next

	r.finish(ctx, x, err)
	if err != nil {
		return false, fmt.Errorf("failed to run %s: %w", x.state.name, err)
	}
	return next != nil, nil
}

// finish counts a step, and the pass if it is over.
func (r *Runner) finish(ctx context.Context, x *run, err error) {
	now := r.now()

	r.state.Lock()
	defer r.state.Unlock()

	s := x.state
	s.steps++
	if err == nil && x.cursor != nil {
		return
	}
	s.passes++

	switch {
	case err == nil:
		s.failures, s.err = 0, nil
	case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		// A pass that was cut off because the runner is stopping did not fail, and the job is
		// not to be left alone for it.
	default:
		s.failures++
		s.err = err
		s.failedAt = now
	}
}

// Run does a round, waits for the interval or a Wake, and does another, until the context is done.
// It returns the error of the context. The errors of a round are told to the error handler,
// unless they are the result of the context being done.
//
// The interval is the time between the end of a round and the start of the next one. A round that
// takes longer than the interval does not make the next one start at once.
func (r *Runner) Run(ctx context.Context) error {
	for {
		if err := r.round(ctx, false); err != nil && ctx.Err() == nil {
			r.onError(err)
		}

		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-r.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Wake makes a running runner start its next round now, instead of when its interval is over.
// It never blocks, and wakes of a runner that is busy count as one.
func (r *Runner) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Spawn runs the runner in the background, as Run does, until Shutdown is called or the
// context is done. A runner that is running already is left running, and one that was shut
// down can be spawned again.
func (r *Runner) Spawn(ctx context.Context) {
	r.spawned.Lock()
	defer r.spawned.Unlock()

	if r.stopped != nil {
		select {
		case <-r.stopped:
			// It has stopped by itself, with its context.
		default:
			return
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	r.cancel, r.stopped = cancel, stopped
	go func() {
		defer close(stopped)
		defer cancel()
		_ = r.Run(ctx)
	}()
}

// Shutdown stops a runner that was spawned, and returns once it has stopped. The runner is
// not cut off: the steps that it is in are finished and written, and what is left to do is left
// for the next runner, so nothing is lost and no job leaves a change half written.
//
// If the context is done before the runner has stopped, Shutdown returns the error of the
// context; the runner still stops after its steps. Shutting down a runner that is not running
// does nothing.
func (r *Runner) Shutdown(ctx context.Context) error {
	r.spawned.Lock()
	cancel, stopped := r.cancel, r.stopped
	r.spawned.Unlock()

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

// Stat is what a runner knows of a job.
type Stat struct {
	// Name is the name of the job.
	Name string
	// Passes is the number of passes that are over, with an error or without.
	Passes uint64
	// Steps is the number of steps that were done.
	Steps uint64
	// Failures is the number of passes that failed one after the other, which is 0 after a pass
	// that did not.
	Failures int
	// Err is the error of the last pass that failed, or nil after a pass that did not.
	Err error
}

// Stats returns what the runner knows of the jobs that it has seen, in the order that it first
// saw them. A job is seen when a round, or a Drain, finds it.
func (r *Runner) Stats() []Stat {
	r.state.Lock()
	defer r.state.Unlock()

	stats := make([]Stat, len(r.order))
	for i, s := range r.order {
		stats[i] = Stat{Name: s.name, Passes: s.passes, Steps: s.steps, Failures: s.failures, Err: s.err}
	}
	return stats
}
