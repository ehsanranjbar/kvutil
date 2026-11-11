package upkeep_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/upkeep"
)

// stepLog records the steps that the fake jobs are asked to do, in the order that they were.
type stepLog struct {
	mu    sync.Mutex
	names []string
}

func (l *stepLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.names = append(l.names, name)
}

func (l *stepLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.names)
}

// fakeJob is a job whose passes have a number of steps. It tells what it is asked of.
type fakeJob struct {
	name string
	// length is the number of steps of a pass.
	length int
	// failAt is the step of a pass that fails, counting from 1, or 0 if none does.
	failAt int
	err    error
	// fail makes the first step of a pass fail while it is set.
	fail atomic.Bool
	// hook is called at the start of a step, with the number of the step counting from 0.
	hook func(n int)
	log  *stepLog

	mu      sync.Mutex
	cursors [][]byte
	drains  []bool
	batches []int
	steps   atomic.Int64
	// inflight is the number of steps that are running, and overlaps the number of steps that
	// started while another one of the job was running.
	inflight atomic.Int32
	overlaps atomic.Int32
}

func newFake(log *stepLog, name string, length int) *fakeJob {
	return &fakeJob{name: name, length: length, log: log, err: errors.New("the job failed")}
}

func (j *fakeJob) Name() string { return j.name }

func (j *fakeJob) Step(_ context.Context, _ kv.DB, pass upkeep.Pass) ([]byte, error) {
	if j.inflight.Add(1) > 1 {
		j.overlaps.Add(1)
	}
	defer j.inflight.Add(-1)

	n := 0
	if pass.Cursor != nil {
		n = int(pass.Cursor[0])
	}
	j.mu.Lock()
	j.cursors = append(j.cursors, pass.Cursor)
	j.drains = append(j.drains, pass.Drain)
	j.batches = append(j.batches, pass.Batch)
	j.mu.Unlock()
	if j.log != nil {
		j.log.add(j.name)
	}
	j.steps.Add(1)
	if j.hook != nil {
		j.hook(n)
	}

	n++
	if (n == 1 && j.fail.Load()) || j.failAt == n {
		return nil, j.err
	}
	if n >= j.length {
		return nil, nil
	}
	return []byte{byte(n)}, nil
}

// fakeStore is a source whose extensions are given.
type fakeStore []any

func (s fakeStore) Extensions() []any { return s }

// fakeExt is an extension with jobs.
type fakeExt []upkeep.Job

func (e fakeExt) Jobs() []upkeep.Job { return e }

// clock is a time that only moves when it is told to.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestJobsTakeTurns(t *testing.T) {
	log := &stepLog{}
	a, b, c := newFake(log, "a", 3), newFake(log, "b", 1), newFake(log, "c", 2)

	r := upkeep.New(memkv.New()).Add(fakeStore{"not a provider", fakeExt{a, b}, fakeExt{c}})
	require.NoError(t, r.Drain(context.Background()))

	// A job with a long pass does not keep the others waiting, and one that is over drops out.
	require.Equal(t, []string{"a", "b", "c", "a", "c", "a"}, log.all())
}

func TestJobsAreFoundOnceInTheOrderOfTheSources(t *testing.T) {
	log := &stepLog{}
	a, b, c, d := newFake(log, "a", 1), newFake(log, "b", 1), newFake(log, "c", 1), newFake(log, "d", 1)

	r := upkeep.New(memkv.New()).
		AddJob(d).
		Add(nil, fakeStore{fakeExt{a, b}}).
		Add(fakeStore{fakeExt{b}, fakeExt{c, a}}).
		AddJob(a).
		AddJob(d)
	require.NoError(t, r.Drain(context.Background()))

	// The jobs that are added come after the ones that are found, and none of them runs twice.
	require.Equal(t, []string{"a", "b", "c", "d"}, log.all())
}

func TestJobsAreFoundAtTheStartOfEveryRound(t *testing.T) {
	log := &stepLog{}
	a, b := newFake(log, "a", 1), newFake(log, "b", 1)

	// A store gets an extension after it is added to the runner.
	store := &growingStore{ext: []any{fakeExt{a}}}
	r := upkeep.New(memkv.New()).Add(store)
	require.NoError(t, r.Drain(context.Background()))
	store.ext = append(store.ext, fakeExt{b})
	require.NoError(t, r.Drain(context.Background()))

	require.Equal(t, []string{"a", "a", "b"}, log.all())
}

type growingStore struct{ ext []any }

func (s *growingStore) Extensions() []any { return s.ext }

func TestCursorIsPassedBack(t *testing.T) {
	a := newFake(nil, "a", 4)
	require.NoError(t, upkeep.New(memkv.New(), upkeep.WithBatchSize(25)).AddJob(a).Drain(context.Background()))

	require.Equal(t, [][]byte{nil, {1}, {2}, {3}}, a.cursors)
	require.Equal(t, []int{25, 25, 25, 25}, a.batches)

	// Every pass starts again with no cursor.
	require.NoError(t, upkeep.New(memkv.New()).AddJob(a).Drain(context.Background()))
	require.Nil(t, a.cursors[4])
	require.Equal(t, 0, a.batches[4])
}

func TestAFailureEndsOnlyThePassOfItsJob(t *testing.T) {
	log := &stepLog{}
	a, b, c := newFake(log, "a", 5), newFake(log, "b", 5), newFake(log, "c", 5)
	b.failAt = 2
	b.err = errors.New("b is broken")
	c.failAt = 3
	c.err = errors.New("c is broken")

	err := upkeep.New(memkv.New()).AddJob(a).AddJob(b).AddJob(c).Drain(context.Background())

	// The errors are joined, with the names of the jobs that failed.
	require.ErrorIs(t, err, b.err)
	require.ErrorIs(t, err, c.err)
	require.ErrorContains(t, err, "failed to run b: b is broken")
	require.ErrorContains(t, err, "failed to run c: c is broken")
	require.NotContains(t, err.Error(), "failed to run a")

	// The others went on to the end of their passes.
	require.Equal(t, []string{
		"a", "b", "c",
		"a", "b", "c",
		"a", "c",
		"a",
		"a",
	}, log.all())
}

func TestDrainSetsDrainAndRoundsDoNot(t *testing.T) {
	a := newFake(nil, "a", 2)
	r := upkeep.New(memkv.New()).AddJob(a)

	require.NoError(t, r.Round(context.Background()))
	require.Equal(t, []bool{false, false}, a.drains)

	require.NoError(t, r.Drain(context.Background()))
	require.Equal(t, []bool{false, false, true, true}, a.drains)
}

func TestRunDoesRoundsAndNotDrains(t *testing.T) {
	a := newFake(nil, "a", 1)
	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Millisecond)).AddJob(a)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	require.Eventually(t, func() bool { return a.steps.Load() >= 3 }, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	for _, drain := range a.drains {
		require.False(t, drain)
	}
}

func TestRunTellsTheErrorHandlerOfFailedRounds(t *testing.T) {
	a := newFake(nil, "a", 1)
	a.fail.Store(true)

	var mu sync.Mutex
	var told []error
	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Millisecond), upkeep.WithErrorHandler(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		told = append(told, err)
	})).AddJob(a)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(told) > 0
	}, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	mu.Lock()
	defer mu.Unlock()
	require.ErrorIs(t, told[0], a.err)
	require.ErrorContains(t, told[0], "failed to run a")
}

// reporter is a job that puts something aside in every step.
type reporter struct{ err error }

func (j reporter) Name() string { return "reporter" }

func (j reporter) Step(_ context.Context, _ kv.DB, pass upkeep.Pass) ([]byte, error) {
	pass.Report(j.err)
	return nil, nil
}

func TestReportGoesToTheErrorHandler(t *testing.T) {
	var told []error
	put := errors.New("a note was put aside")
	r := upkeep.New(memkv.New(), upkeep.WithErrorHandler(func(err error) { told = append(told, err) })).
		AddJob(reporter{err: put})
	require.NoError(t, r.Drain(context.Background()))
	require.Equal(t, []error{put}, told)

	// Without a handler, what is reported goes nowhere.
	require.NoError(t, upkeep.New(memkv.New()).AddJob(reporter{err: put}).Drain(context.Background()))
}

func TestFailingJobsAreLeftAloneForLonger(t *testing.T) {
	const interval = time.Second
	clk := newClock()
	a, b := newFake(nil, "a", 1), newFake(nil, "b", 1)
	a.fail.Store(true)

	r := upkeep.New(memkv.New(), upkeep.WithInterval(interval)).AddJob(a).AddJob(b)
	r.SetNow(clk.now)
	ctx := context.Background()
	round := func(after time.Duration) {
		clk.advance(after)
		_ = r.Round(ctx)
	}

	round(0)
	require.EqualValues(t, 1, a.steps.Load())
	require.EqualValues(t, 1, b.steps.Load())

	// After the first failure the job is left alone for an interval, and the others go on.
	round(interval - 1)
	require.EqualValues(t, 1, a.steps.Load())
	require.EqualValues(t, 2, b.steps.Load())
	round(1)
	require.EqualValues(t, 2, a.steps.Load())

	// After the second, for two.
	round(2*interval - 1)
	require.EqualValues(t, 2, a.steps.Load())
	round(1)
	require.EqualValues(t, 3, a.steps.Load())

	// After the third, for four.
	round(4*interval - 1)
	require.EqualValues(t, 3, a.steps.Load())
	round(1)
	require.EqualValues(t, 4, a.steps.Load())
	require.Equal(t, 4, r.Stats()[0].Failures)

	// A drain does not wait, and a pass that succeeds ends it.
	a.fail.Store(false)
	require.NoError(t, r.Drain(ctx))
	require.EqualValues(t, 5, a.steps.Load())
	require.Zero(t, r.Stats()[0].Failures)
	round(0)
	require.EqualValues(t, 6, a.steps.Load())
}

func TestTheBackoffHasALimit(t *testing.T) {
	const interval = time.Second
	clk := newClock()
	a := newFake(nil, "a", 1)
	a.fail.Store(true)

	r := upkeep.New(memkv.New(), upkeep.WithInterval(interval)).AddJob(a)
	r.SetNow(clk.now)
	ctx := context.Background()

	for i := 1; i <= 12; i++ {
		clk.advance(64 * interval)
		_ = r.Round(ctx)
		require.EqualValues(t, i, a.steps.Load())
	}

	// It is 64 intervals however long the job has been failing.
	clk.advance(64*interval - 1)
	_ = r.Round(ctx)
	require.EqualValues(t, 12, a.steps.Load())
	clk.advance(1)
	_ = r.Round(ctx)
	require.EqualValues(t, 13, a.steps.Load())
}

func TestEvery(t *testing.T) {
	const interval = time.Second
	clk := newClock()
	often, rarely := newFake(nil, "often", 1), newFake(nil, "rarely", 1)

	r := upkeep.New(memkv.New(), upkeep.WithInterval(interval)).
		AddJob(often).
		AddJob(rarely, upkeep.Every(10*interval))
	r.SetNow(clk.now)
	ctx := context.Background()

	require.NoError(t, r.Round(ctx))
	require.EqualValues(t, 1, rarely.steps.Load())

	clk.advance(10*interval - 1)
	require.NoError(t, r.Round(ctx))
	require.EqualValues(t, 1, rarely.steps.Load())
	require.EqualValues(t, 2, often.steps.Load())

	clk.advance(1)
	require.NoError(t, r.Round(ctx))
	require.EqualValues(t, 2, rarely.steps.Load())

	// A drain does it whenever it is asked to, and it counts as a start.
	clk.advance(interval)
	require.NoError(t, r.Drain(ctx))
	require.EqualValues(t, 3, rarely.steps.Load())
	clk.advance(10*interval - 1)
	require.NoError(t, r.Round(ctx))
	require.EqualValues(t, 3, rarely.steps.Load())
}

func TestEveryAShorterTimeThanTheIntervalIsTheInterval(t *testing.T) {
	clk := newClock()
	a := newFake(nil, "a", 1)

	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Minute)).AddJob(a, upkeep.Every(time.Second))
	r.SetNow(clk.now)
	require.NoError(t, r.Round(context.Background()))

	// The rounds of a running runner are an interval apart, so the job runs in every one.
	clk.advance(time.Minute)
	require.NoError(t, r.Round(context.Background()))
	require.EqualValues(t, 2, a.steps.Load())
}

func TestWorkersStepDifferentJobsAtOnce(t *testing.T) {
	const jobs, length = 8, 30

	// The first steps of the jobs wait for each other, which they only can if four run at once.
	var arrived atomic.Int32
	met := make(chan struct{})
	var once sync.Once

	log := &stepLog{}
	var all []*fakeJob
	r := upkeep.New(memkv.New(), upkeep.WithWorkers(4))
	for i := range jobs {
		j := newFake(log, string(rune('a'+i)), length)
		j.hook = func(n int) {
			if n == 0 {
				if arrived.Add(1) == 4 {
					once.Do(func() { close(met) })
				}
				select {
				case <-met:
				case <-time.After(5 * time.Second):
				}
			}
			time.Sleep(10 * time.Microsecond)
		}
		all = append(all, j)
		r.AddJob(j)
	}
	require.NoError(t, r.Drain(context.Background()))

	select {
	case <-met:
	default:
		t.Fatal("four steps never ran at once")
	}
	for _, j := range all {
		// A job is never in two goroutines at once, and every pass got to its end.
		require.Zero(t, j.overlaps.Load(), j.name)
		require.EqualValues(t, length, j.steps.Load(), j.name)
	}
	require.Len(t, log.all(), jobs*length)
}

func TestWorkersWithOneJobOrOne(t *testing.T) {
	for _, n := range []int{-3, 0, 1, 4} {
		a, b := newFake(nil, "a", 5), newFake(nil, "b", 3)
		require.NoError(t, upkeep.New(memkv.New(), upkeep.WithWorkers(n)).AddJob(a).AddJob(b).Drain(context.Background()))
		require.EqualValues(t, 5, a.steps.Load())
		require.EqualValues(t, 3, b.steps.Load())

		c := newFake(nil, "c", 5)
		require.NoError(t, upkeep.New(memkv.New(), upkeep.WithWorkers(n)).AddJob(c).Drain(context.Background()))
		require.EqualValues(t, 5, c.steps.Load())
	}
}

func TestWorkersJoinTheErrors(t *testing.T) {
	a, b, c := newFake(nil, "a", 5), newFake(nil, "b", 5), newFake(nil, "c", 5)
	a.failAt, b.failAt = 2, 3

	err := upkeep.New(memkv.New(), upkeep.WithWorkers(3)).AddJob(a).AddJob(b).AddJob(c).Drain(context.Background())
	require.ErrorIs(t, err, a.err)
	require.ErrorIs(t, err, b.err)
	require.EqualValues(t, 5, c.steps.Load())
	require.EqualValues(t, 2, a.steps.Load())
	require.EqualValues(t, 3, b.steps.Load())
}

func TestCancelingStopsADrainBetweenSteps(t *testing.T) {
	for _, workers := range []int{1, 4} {
		ctx, cancel := context.WithCancel(context.Background())
		a, b := newFake(nil, "a", 1000), newFake(nil, "b", 1000)
		a.hook = func(n int) {
			if n == 2 {
				cancel()
			}
		}

		r := upkeep.New(memkv.New(), upkeep.WithWorkers(workers)).AddJob(a).AddJob(b)
		err := r.Drain(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 3, a.steps.Load())
		if workers == 1 {
			// They take turns, so the other did not get further.
			require.EqualValues(t, 2, b.steps.Load())
		}

		// A pass that was cut off did not fail, so the job is not left alone for it.
		for _, s := range r.Stats() {
			require.Zero(t, s.Failures)
		}
	}
}

func TestADrainDoesNotStartWithAContextThatIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := newFake(nil, "a", 3)

	require.ErrorIs(t, upkeep.New(memkv.New()).AddJob(a).Drain(ctx), context.Canceled)
	require.Zero(t, a.steps.Load())
}

func TestStats(t *testing.T) {
	a, b := newFake(nil, "a", 3), newFake(nil, "b", 2)
	b.fail.Store(true)
	r := upkeep.New(memkv.New()).Add(fakeStore{fakeExt{a}}).AddJob(b)
	ctx := context.Background()

	require.Empty(t, r.Stats())

	require.Error(t, r.Drain(ctx))
	require.Equal(t, []upkeep.Stat{
		{Name: "a", Passes: 1, Steps: 3},
		{Name: "b", Passes: 1, Steps: 1, Failures: 1, Err: b.err},
	}, r.Stats())

	require.Error(t, r.Drain(ctx))
	stats := r.Stats()
	require.Equal(t, upkeep.Stat{Name: "a", Passes: 2, Steps: 6}, stats[0])
	require.Equal(t, upkeep.Stat{Name: "b", Passes: 2, Steps: 2, Failures: 2, Err: b.err}, stats[1])

	b.fail.Store(false)
	require.NoError(t, r.Drain(ctx))
	require.Equal(t, upkeep.Stat{Name: "b", Passes: 3, Steps: 4}, r.Stats()[1])

	// What is returned is a copy.
	stats[0].Name = "changed"
	require.Equal(t, "a", r.Stats()[0].Name)
}

// blocking is a job whose step waits to be told to finish.
type blocking struct {
	entered chan struct{}
	release chan struct{}
	done    atomic.Int32
}

func newBlocking() *blocking {
	return &blocking{entered: make(chan struct{}, 100), release: make(chan struct{})}
}

func (j *blocking) Name() string { return "blocking" }

func (j *blocking) Step(context.Context, kv.DB, upkeep.Pass) ([]byte, error) {
	j.entered <- struct{}{}
	<-j.release
	j.done.Add(1)
	return nil, nil
}

func TestSpawnTwiceIsOneRunner(t *testing.T) {
	a := newFake(nil, "a", 1)
	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Millisecond)).AddJob(a)
	ctx := context.Background()

	r.Spawn(ctx)
	r.Spawn(ctx)
	require.Eventually(t, func() bool { return a.steps.Load() > 2 }, 5*time.Second, time.Millisecond)

	// A single shutdown stops everything, which it would not if a second runner was left behind.
	require.NoError(t, r.Shutdown(ctx))
	steps := a.steps.Load()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, steps, a.steps.Load())
}

func TestShutdownWaitsForTheStepInProgress(t *testing.T) {
	j := newBlocking()
	r := upkeep.New(memkv.New()).AddJob(j)
	r.Spawn(context.Background())
	<-j.entered

	stopped := make(chan error, 1)
	go func() { stopped <- r.Shutdown(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("shut down in the middle of a step: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(j.release)
	require.NoError(t, <-stopped)
	// The step was finished, not cut off.
	require.EqualValues(t, 1, j.done.Load())
}

func TestShutdownWithAContextThatIsDone(t *testing.T) {
	j := newBlocking()
	r := upkeep.New(memkv.New()).AddJob(j)
	r.Spawn(context.Background())
	<-j.entered

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, r.Shutdown(expired), context.Canceled)

	// It still stops after its step.
	close(j.release)
	require.NoError(t, r.Shutdown(context.Background()))
	require.EqualValues(t, 1, j.done.Load())
}

func TestShutdownOfARunnerThatIsNotRunning(t *testing.T) {
	r := upkeep.New(memkv.New())
	require.NoError(t, r.Shutdown(context.Background()))

	r.Spawn(context.Background())
	require.NoError(t, r.Shutdown(context.Background()))
	require.NoError(t, r.Shutdown(context.Background()))
}

func TestSpawnAfterShutdown(t *testing.T) {
	a := newFake(nil, "a", 1)
	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Millisecond)).AddJob(a)
	ctx := context.Background()

	r.Spawn(ctx)
	require.Eventually(t, func() bool { return a.steps.Load() > 0 }, 5*time.Second, time.Millisecond)
	require.NoError(t, r.Shutdown(ctx))

	steps := a.steps.Load()
	r.Spawn(ctx)
	require.Eventually(t, func() bool { return a.steps.Load() > steps+1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, r.Shutdown(ctx))
}

func TestSpawnAfterTheContextIsDone(t *testing.T) {
	a := newFake(nil, "a", 1)
	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Millisecond)).AddJob(a)

	first, cancel := context.WithCancel(context.Background())
	r.Spawn(first)
	require.Eventually(t, func() bool { return a.steps.Load() > 0 }, 5*time.Second, time.Millisecond)
	cancel()

	// A runner that stopped with its context is spawned again.
	require.Eventually(t, func() bool {
		steps := a.steps.Load()
		time.Sleep(5 * time.Millisecond)
		return a.steps.Load() == steps
	}, 5*time.Second, time.Millisecond)
	steps := a.steps.Load()
	r.Spawn(context.Background())
	require.Eventually(t, func() bool { return a.steps.Load() > steps+1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, r.Shutdown(context.Background()))
}

func TestWakeStartsARoundBeforeTheInterval(t *testing.T) {
	a := newFake(nil, "a", 1)
	r := upkeep.New(memkv.New(), upkeep.WithInterval(time.Hour)).AddJob(a)
	ctx := context.Background()

	r.Spawn(ctx)
	require.Eventually(t, func() bool { return a.steps.Load() == 1 }, 5*time.Second, time.Millisecond)

	// Nothing happens by itself.
	time.Sleep(20 * time.Millisecond)
	require.EqualValues(t, 1, a.steps.Load())

	r.Wake()
	require.Eventually(t, func() bool { return a.steps.Load() == 2 }, 5*time.Second, time.Millisecond)

	// Waking a runner that is not running, or many times over, does not block.
	for range 10 {
		r.Wake()
	}
	require.NoError(t, r.Shutdown(ctx))
	upkeep.New(memkv.New()).Wake()
}

func TestADrainWaitsForTheRoundThatIsRunning(t *testing.T) {
	j := newBlocking()
	r := upkeep.New(memkv.New()).AddJob(j)
	r.Spawn(context.Background())
	<-j.entered

	drained := make(chan error, 1)
	go func() { drained <- r.Drain(context.Background()) }()
	select {
	case err := <-drained:
		t.Fatalf("drained while a round was running: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(j.release)
	require.NoError(t, <-drained)
	require.NoError(t, r.Shutdown(context.Background()))
	require.EqualValues(t, 2, j.done.Load())
}
