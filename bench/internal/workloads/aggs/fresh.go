package aggs

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

const (
	// pollEvery is how often a trial looks at the stored aggregates for what it wrote.
	pollEvery = 5 * time.Millisecond
	// lagTimeout is how long a trial waits for the aggregates to have what was written.
	lagTimeout = 60 * time.Second
	// burstSize is the number of events a trial of insertions writes, and the number of updates
	// and of deletes of old events a trial of changes makes.
	burstSize                = 200
	changeUpdates            = 20
	changeDeletes            = 5
	recentDays               = 60
	minTrials, maxTrials int = 4, 40
)

// freshTo is the end of the windows of the queries that look at the events that a run adds: past
// the day that they are added in.
func (s setup) freshTo(u unit) time.Time {
	t := s.spec.End.AddDate(0, 0, 2)
	if now := time.Now().UTC().AddDate(0, 0, 1); now.After(t) {
		t = now
	}
	return alignUp(u, t)
}

// focusQueries are the series of the customer that the trials write, by day, week and month, over
// the recent days and the days of the stream. They are what the trials poll.
func (s setup) focusQueries(customer string) []query {
	from := s.spec.End.AddDate(0, 0, -recentDays)
	var qs []query
	for _, u := range []unit{day, week, month} {
		qs = append(qs, query{Kind: qSeries, Unit: u, Customer: customer, From: startOf(u, from), To: s.freshTo(u)})
	}
	return qs
}

// trialCount is the number of trials of a kind: two for every interval of the background work that
// fits in the duration of the run, from 4 to 40.
func trialCount(p harness.Params, interval time.Duration) int {
	n := int(2 * p.Duration / interval)
	return min(max(n, minTrials), maxTrials)
}

// matches reports whether every query of the set reads what it should.
func (ss *session) matches(ctx context.Context, qs []query, want [][]row, done []time.Duration, t0 time.Time) error {
	for i, q := range qs {
		if done[i] > 0 {
			continue
		}
		got, err := ss.e.Read(ctx, q)
		if err != nil {
			return err
		}
		if equalRows(got, want[i]) {
			done[i] = max(time.Since(t0), 1)
		}
	}
	return nil
}

// trial writes, and then looks at the stored aggregates until they have what was written. It
// returns when the series of days had it, and when all of them did; a trial that waited for the
// timeout returns it for both, with timedOut.
func (ss *session) trial(ctx context.Context, qs []query, write func(ctx context.Context) error) (daily, all time.Duration, timedOut bool, err error) {
	if err := write(ctx); err != nil {
		return 0, 0, false, err
	}
	t0 := time.Now()
	want := make([][]row, len(qs))
	for i, q := range qs {
		if want[i], err = ss.e.Truth(ctx, q); err != nil {
			return 0, 0, false, err
		}
	}
	done := make([]time.Duration, len(qs))
	for {
		if err := ss.matches(ctx, qs, want, done, t0); err != nil {
			return 0, 0, false, err
		}
		if !slices.Contains(done, 0) {
			break
		}
		if time.Since(t0) > lagTimeout {
			return lagTimeout, lagTimeout, true, nil
		}
		select {
		case <-ctx.Done():
			return 0, 0, false, ctx.Err()
		case <-time.After(pollEvery):
		}
	}
	return done[0], slices.Max(done), false, nil
}

// quiet waits until the stored aggregates have everything that is written.
func (ss *session) quiet(ctx context.Context, qs []query) error {
	deadline := time.Now().Add(lagTimeout)
	for {
		var differs string
		for _, q := range qs {
			got, err := ss.e.Read(ctx, q)
			if err != nil {
				return err
			}
			want, err := ss.e.Truth(ctx, q)
			if err != nil {
				return err
			}
			if !equalRows(got, want) {
				differs = fmt.Sprintf("%s reads %s, the events say %s", q.label("series"), summarize(q, got), summarize(q, want))
				break
			}
		}
		if differs == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the aggregates did not catch up in %s: %s", lagTimeout, differs)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// burst writes the events with some workers at the same time, one transaction for each.
func (ss *session) burst(ctx context.Context, st *stream, customer string, count int) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	var next int
	for range ss.s.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev := &event{}
			for {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				if i >= count || ctx.Err() != nil {
					return
				}
				st.fill(ev, customer)
				if err := ss.e.Insert(ctx, ev); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return first
}

// summary is the minimum, mean, median, 95th percentile and maximum of durations in milliseconds.
func summary(ds []time.Duration) (lo, mean, p50, p95, hi float64) {
	if len(ds) == 0 {
		return
	}
	ms := make([]float64, len(ds))
	var sum float64
	for i, d := range ds {
		ms[i] = float64(d.Microseconds()) / 1000
		sum += ms[i]
	}
	sort.Float64s(ms)
	at := func(p float64) float64 {
		// nearest rank
		i := int(p*float64(len(ms))+0.999999) - 1
		return ms[min(max(i, 0), len(ms)-1)]
	}
	return ms[0], sum / float64(len(ms)), at(0.5), at(0.95), ms[len(ms)-1]
}

func joinMS(ds []time.Duration) string {
	parts := make([]string, len(ds))
	for i, d := range ds {
		parts[i] = strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64)
	}
	return strings.Join(parts, ",")
}

// runFreshness measures how stale the stored aggregates are. A trial of `lag-insert` waits until
// the stored aggregates have everything, then writes a burst of new events of one customer, and
// polls the reads of the variant (every 5 ms) until they return what the events say (the exact
// answer from the events, computed after the burst): the time from the last write returning to the
// first read that is right. A trial of `lag-change-old` does it for updates and deletes of old
// events of that customer. Each case reports the lags of the series of days and of all three series
// (days, weeks, months), and the lags of every trial. Then, with the background work stopped, bursts
// of events are left pending, and a daily series and a total are read while they are: the cost
// of an exact read that has to take the notes (kvutil) or the unmaterialized range (TimescaleDB)
// into account, against the read of what is stored.
func runFreshness(ctx context.Context, rc *harness.RunContext, variant string) error {
	ss, err := begin(ctx, rc, variant, "agg-freshness")
	if err != nil {
		return err
	}
	defer ss.close(ctx)
	e, s, n, spec := ss.e, ss.s, ss.n, ss.s.spec
	focus := hotCustomer()
	qs := s.focusQueries(focus)
	st := newStream(spec, n+1)
	trials := trialCount(rc.Params, s.interval)

	// The recent events of the customer are the ones that the trials of changes work on.
	var alive []*event
	recent := spec.End.AddDate(0, 0, -recentDays)
	for _, id := range spec.CustomersOf(0, n) {
		if ev := spec.Gen(id, 0); !ev.At.Before(recent) {
			alive = append(alive, ev)
		}
	}
	if len(alive) < changeUpdates+changeDeletes {
		return fmt.Errorf("the customer has %d events in the last %d days: the scale is too small for the trials of changes", len(alive), recentDays)
	}
	rnd := harness.NewRand(rc.Params.Seed^0xf2e5, 1)
	version := uint32(0)

	if err := ss.background(ctx, true); err != nil {
		return err
	}
	kinds := []struct {
		name  string
		write func(ctx context.Context) error
	}{
		{"insert", func(ctx context.Context) error { return ss.burst(ctx, st, focus, burstSize) }},
		{"change-old", func(ctx context.Context) error {
			for range changeUpdates {
				version++
				old := alive[rnd.IntN(len(alive))]
				if err := e.Update(ctx, old, spec.Gen(old.ID, version)); err != nil {
					return err
				}
			}
			for range changeDeletes {
				i := rnd.IntN(len(alive))
				if err := e.Delete(ctx, alive[i]); err != nil {
					return err
				}
				alive = slices.Delete(alive, i, i+1)
			}
			return nil
		}},
	}
	for _, k := range kinds {
		start := time.Now()
		var dailies, alls []time.Duration
		timeouts := 0
		for range trials {
			if err := ss.quiet(ctx, qs); err != nil {
				return fmt.Errorf("lag-%s: %w", k.name, err)
			}
			// Trials start at any point of the interval of the background work.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(rnd.Int64N(int64(s.interval)))):
			}
			daily, all, timedOut, err := ss.trial(ctx, qs, k.write)
			if err != nil {
				return fmt.Errorf("lag-%s: %w", k.name, err)
			}
			if timedOut {
				timeouts++
			}
			dailies, alls = append(dailies, daily), append(alls, all)
		}
		extra := map[string]float64{"trials": float64(trials), "timeouts": float64(timeouts)}
		for name, ds := range map[string][]time.Duration{"daily": dailies, "all": alls} {
			mn, mean, p50, p95, mx := summary(ds)
			extra["lag_"+name+"_ms_min"], extra["lag_"+name+"_ms_mean"] = mn, mean
			extra["lag_"+name+"_ms_p50"], extra["lag_"+name+"_ms_p95"], extra["lag_"+name+"_ms_max"] = p50, p95, mx
		}
		c := harness.Case{
			Name: "lag-" + k.name, Concurrency: s.workers,
			Params: s.caseParams(map[string]any{"events": n, "burst": burstSize, "customer": focus, "poll_ms": pollEvery.Milliseconds(), "trials": trials}),
			Info:   map[string]string{"lag_daily_ms": joinMS(dailies), "lag_all_ms": joinMS(alls)},
		}
		if err := rc.Report(ctx, c, start, time.Now(), metrics.Metrics{Extra: extra}); err != nil {
			return err
		}
	}

	if variant == pgComputed || variant == pgTrigger {
		return nil
	}
	return ss.pendingReads(ctx, st, focus)
}

// pendingTargets are the numbers of events that are pending when the reads are measured.
func pendingTargets(n int64) []int {
	return []int{0, int(max(n/100, 100)), int(max(n/10, 1000))}
}

// pendingReads stops the background work, leaves more and more events of one customer pending,
// and measures a daily series of the last 30 days and a total of that customer at each step.
func (ss *session) pendingReads(ctx context.Context, st *stream, focus string) error {
	e, s := ss.e, ss.s
	if err := ss.background(ctx, false); err != nil {
		return err
	}
	// A refresh that was running when the policies were stopped finishes.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.interval + 500*time.Millisecond):
	}
	if err := e.CatchUp(ctx); err != nil {
		return err
	}

	today := startOf(day, time.Now())
	series := query{Kind: qSeries, Unit: day, Customer: focus, From: today.AddDate(0, 0, -29), To: today.AddDate(0, 0, 1)}
	total := query{Kind: qTotal, Customer: focus}

	have := 0
	for _, target := range pendingTargets(ss.n) {
		var batch []*event
		for ; have < target; have++ {
			ev := &event{}
			st.fill(ev, focus)
			batch = append(batch, ev)
		}
		if err := e.InsertBatch(ctx, batch); err != nil {
			return err
		}
		if exactReads(ss.variantOf()) {
			for _, q := range []query{series, total} {
				got, err := e.Read(ctx, q)
				if err != nil {
					return err
				}
				want, err := e.Truth(ctx, q)
				if err != nil {
					return err
				}
				if !equalRows(got, want) {
					return fmt.Errorf("with %d events pending the read of %s is not the exact answer", target, q.label("pending"))
				}
			}
		}
		for _, r := range []struct {
			name string
			q    query
		}{{"daily30-pending-", series}, {"total-pending-", total}} {
			err := ss.measure(ctx, harness.Case{
				Name: r.name + strconv.Itoa(target), Concurrency: 1, Unit: "rows",
				Params: s.caseParams(map[string]any{"events": ss.n, "pending": target, "customer": focus}),
			}, func(ctx context.Context, w *harness.Worker) error {
				w.Begin()
				rows, err := e.Read(ctx, r.q)
				w.Add(len(rows))
				return err
			})
			if err != nil {
				return err
			}
		}
	}
	return ss.background(ctx, true)
}

// variantOf is the variant that the session was opened for.
func (ss *session) variantOf() string { return ss.rc.Variant }
