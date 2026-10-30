package aggs

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// oracle answers a query from the events themselves, in Go.
func oracle(events map[int64]*event, q query) []row {
	switch q.Kind {
	case qSeries:
		by := map[int64]*row{}
		for _, e := range events {
			if e.Customer != q.Customer || e.At.Before(q.From) || !e.At.Before(q.To) {
				continue
			}
			b := startOf(q.Unit, e.At)
			r, ok := by[b.Unix()]
			if !ok {
				r = &row{Bucket: b, Max: e.Amount}
				by[b.Unix()] = r
			}
			r.Sum += e.Amount
			r.Count++
			r.Max = max(r.Max, e.Amount)
		}
		var rows []row
		for _, r := range by {
			rows = append(rows, *r)
		}
		sortRows(rows)
		return rows
	case qTotal:
		var r row
		for _, e := range events {
			if e.Customer == q.Customer {
				r.Sum += e.Amount
				r.Count++
			}
		}
		return []row{r}
	}
	sums := map[string]int64{}
	for _, e := range events {
		if !e.At.Before(q.From) && e.At.Before(q.To) {
			sums[e.Customer] += e.Amount
		}
	}
	var rows []row
	for c, s := range sums {
		rows = append(rows, row{Customer: c, Sum: s})
	}
	return topRows(rows)
}

func TestBucketsAreThoseOfKvutil(t *testing.T) {
	cals := map[unit]agg.Calendar{day: agg.Daily, week: agg.Weekly, month: agg.Monthly}
	times := []time.Time{
		time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 28, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), // a Sunday
		time.Date(2026, 3, 1, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), // a Monday
		time.Date(2024, 2, 29, 5, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, tm := range times {
		for u, cal := range cals {
			if got, want := startOf(u, tm), cal.Start(tm); !got.Equal(want) {
				t.Errorf("startOf(%s, %s) = %s, kvutil says %s", u, tm, got, want)
			}
			s := startOf(u, tm)
			if got, want := nextOf(u, s), cal.Next(s); !got.Equal(want) {
				t.Errorf("nextOf(%s, %s) = %s, kvutil says %s", u, s, got, want)
			}
		}
	}
	if a := alignUp(week, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)); !a.Equal(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("a Monday midnight is not aligned: %s", a)
	}
	if a := alignUp(month, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)); !a.Equal(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("alignUp month: %s", a)
	}
}

func TestTheVerificationEndsOnAMondayThatStartsAMonth(t *testing.T) {
	if verifyEnd.Weekday() != time.Monday || verifyEnd.Day() != 1 {
		t.Fatalf("%s is a %s, the %dth", verifyEnd, verifyEnd.Weekday(), verifyEnd.Day())
	}
	s := verifySetup(testutil.Params(2000))
	ts := boundaryTimes(s.spec)
	if len(ts) < 7 {
		t.Fatalf("%d instants: %v", len(ts), ts)
	}
	var sawMonth, sawWeek, sawYear bool
	for _, tm := range ts {
		sawMonth = sawMonth || (tm.Day() == 1 && tm.Hour() == 0 && tm.Minute() == 0 && tm.Second() == 0)
		sawWeek = sawWeek || (tm.Weekday() == time.Monday && tm.Hour() == 0 && tm.Second() == 0)
		sawYear = sawYear || (tm.YearDay() == 1 && tm.Second() == 0 && tm.Minute() == 0)
		if tm.Before(s.spec.Start()) || !tm.Before(s.spec.End) {
			t.Errorf("%s is out of the span", tm)
		}
	}
	if !sawMonth || !sawWeek || !sawYear {
		t.Errorf("month %v week %v year %v: %v", sawMonth, sawWeek, sawYear, ts)
	}
	// Both sides of a boundary are there.
	seen := map[time.Time]bool{}
	for _, tm := range ts {
		seen[tm] = true
	}
	for _, tm := range ts {
		if tm.Second() == 0 && tm.Minute() == 0 && tm.Hour() == 0 && tm.After(s.spec.Start()) && !seen[tm.Add(-time.Second)] {
			t.Errorf("%s has no event a second before", tm)
		}
	}
}

func TestReadCasesDrawQueriesInTheSpan(t *testing.T) {
	p := testutil.Params(10_000)
	s := newSetup(p)
	r := harness.NewRand(1, 1)
	names := map[string]bool{}
	for _, c := range readCases() {
		names[c.name] = true
		for i := 0; i < 200; i++ {
			q := c.gen(s, r)
			if q.Kind != qTotal && (q.From.IsZero() || !q.From.Before(q.To)) {
				t.Fatalf("%s: window [%s, %s)", c.name, q.From, q.To)
			}
			if q.Kind == qSeries {
				if !startOf(q.Unit, q.From).Equal(q.From) || !alignUp(q.Unit, q.To).Equal(q.To) {
					t.Fatalf("%s: [%s, %s) is not aligned to the %s", c.name, q.From, q.To, q.Unit)
				}
				if q.From.Before(startOf(q.Unit, s.spec.Start())) || q.To.After(alignUp(q.Unit, s.spec.End)) {
					t.Fatalf("%s: [%s, %s) is outside the span", c.name, q.From, q.To)
				}
			}
			if q.Kind == qTop && !q.To.Equal(nextOf(month, q.From)) {
				t.Fatalf("top: [%s, %s) is not a month", q.From, q.To)
			}
		}
	}
	for _, want := range []string{"daily-30", "daily-365", "weekly-year", "monthly-year", "total", "top10-monthly"} {
		if !names[want] {
			t.Errorf("no case %s", want)
		}
	}
	q := readCases()[0].gen(s, r)
	if q.To.Sub(q.From) != 30*dayLen {
		t.Errorf("daily-30 is %s long", q.To.Sub(q.From))
	}
}

// load fills an env with the events 1 to n, and also returns them as the oracle has them.
func loadKV(t *testing.T, variant string, n int64) (env, setup, map[int64]*event) {
	t.Helper()
	p := testutil.Params(int(n))
	s := verifySetup(p)
	e, err := openEnv(context.Background(), testutil.Kvutil(t), variant, s, "test", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close(context.Background()) })
	if err := e.Load(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	events := map[int64]*event{}
	for id := int64(1); id <= n; id++ {
		events[id] = s.spec.Gen(id, 0)
	}
	return e, s, events
}

func TestKvutilReadsAreTheOracle(t *testing.T) {
	ctx := context.Background()
	const n = 1500
	for _, variant := range []string{kvExact, kvCollected} {
		t.Run(variant, func(t *testing.T) {
			e, s, events := loadKV(t, variant, n)
			spec := s.spec
			qs := s.verifyQueries()

			// The boundaries of the buckets, for the most popular customer.
			id := int64(n)
			for i, tm := range boundaryTimes(spec) {
				id++
				ev := spec.Gen(id, 0)
				ev.Customer, ev.At, ev.Amount = dataset.CustomerName(0), tm, int64(1000+7*i)
				if err := e.Insert(ctx, ev); err != nil {
					t.Fatal(err)
				}
				events[id] = ev
			}

			check := func(stage string, collected bool) {
				t.Helper()
				for _, q := range qs {
					got, err := e.Read(ctx, q)
					if err != nil {
						t.Fatal(err)
					}
					want := oracle(events, q)
					if collected && variant == kvCollected {
						// Before a collection the collected state has nothing.
						if equalRows(got, want) && len(want) > 0 && q.Kind != qTop {
							t.Fatalf("%s: %s: the collected read already has everything", stage, q.label("q"))
						}
						continue
					}
					if !equalRows(got, want) {
						t.Fatalf("%s: %s: %d rows, want %d\n got  %v\n want %v", stage, q.label("q"), len(got), len(want), lines(q, got), lines(q, want))
					}
					truth, err := e.Truth(ctx, q)
					if err != nil || !equalRows(truth, want) {
						t.Fatalf("%s: %s: the truth is wrong: %v", stage, q.label("q"), err)
					}
				}
			}

			check("loaded, nothing collected", true)
			if err := e.CatchUp(ctx); err != nil {
				t.Fatal(err)
			}
			check("collected", false)

			// Updates, moves and deletes of old events.
			for i := int64(1); i <= n; i++ {
				old := spec.Gen(i, 0)
				switch {
				case i%11 == 0:
					ev := spec.Gen(i, 1)
					events[i] = ev
					if err := e.Update(ctx, old, ev); err != nil {
						t.Fatal(err)
					}
				case i%13 == 0:
					ev := &dataset.Event{}
					spec.GenMoved(ev, i, 2)
					events[i] = ev
					if err := e.Update(ctx, old, ev); err != nil {
						t.Fatal(err)
					}
				case i%17 == 0:
					delete(events, i)
					if err := e.Delete(ctx, old); err != nil {
						t.Fatal(err)
					}
				}
			}
			// New events, after the span, in a batch.
			var fresh []*dataset.Event
			for k := 0; k < 30; k++ {
				id++
				ev := &dataset.Event{}
				spec.GenStream(ev, id, spec.End.Add(time.Duration(k)*97*time.Minute))
				fresh = append(fresh, ev)
				events[id] = ev
			}
			if err := e.InsertBatch(ctx, fresh); err != nil {
				t.Fatal(err)
			}
			check("changed, nothing collected", true)
			if err := e.CatchUp(ctx); err != nil {
				t.Fatal(err)
			}
			check("changed and collected", false)

			st, err := e.Storage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !st.Logical || st.Events == 0 || st.Aggs == 0 {
				t.Errorf("storage %+v", st)
			}
		})
	}
}

func TestKvutilCollectorKeepsTheAggregatesUpToDate(t *testing.T) {
	t.Setenv(envInterval, "20ms")
	ctx := context.Background()
	e, s, events := loadKV(t, kvCollected, 500)
	if err := e.CatchUp(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Background(ctx, true); err != nil {
		t.Fatal(err)
	}
	st := newStream(s.spec, 501)
	for i := 0; i < 50; i++ {
		ev := &dataset.Event{}
		st.fill(ev, hotCustomer())
		events[ev.ID] = ev
		if err := e.Insert(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	q := query{Kind: qTotal, Customer: hotCustomer()}
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := e.Read(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if equalRows(got, oracle(events, q)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the collector did not collect in 10 s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := e.Background(ctx, false); err != nil {
		t.Fatal(err)
	}
	if stats := e.Stats(); stats["collector_errors"] != 0 {
		t.Errorf("stats %v", stats)
	}
}

func TestKvutilBuildsAggregatesOverEvents(t *testing.T) {
	ctx := context.Background()
	p := testutil.Params(800)
	s := verifySetup(p)
	e, err := openEnv(ctx, testutil.Kvutil(t), kvCollector, s, "test-build", true)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(ctx)
	rb := e.(rebuilder)
	if err := rb.LoadPlain(ctx, 800); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Read(ctx, query{Kind: qTotal, Customer: hotCustomer()}); err == nil {
		t.Error("a read of aggregates that are not defined worked")
	}
	var steps []string
	err = rb.Build(ctx, func(name string, fn func(context.Context) error) error {
		steps = append(steps, name)
		return fn(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "migrate rebuild-sum rebuild-count rebuild-max"; strings.Join(steps, " ") != want {
		t.Errorf("steps %v, want %s", steps, want)
	}
	events := map[int64]*event{}
	for id := int64(1); id <= 800; id++ {
		events[id] = s.spec.Gen(id, 0)
	}
	for _, q := range s.verifyQueries() {
		got, err := e.Read(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if want := oracle(events, q); !equalRows(got, want) {
			t.Fatalf("%s: %v, want %v", q.label("q"), lines(q, got), lines(q, want))
		}
	}
}

func TestSummarize(t *testing.T) {
	q := query{Kind: qSeries}
	a := []row{{Bucket: time.Unix(0, 0).UTC(), Sum: 5, Count: 2, Max: 4}, {Bucket: time.Unix(86400, 0).UTC(), Sum: 1, Count: 1, Max: 1}}
	b := []row{a[0], {Bucket: a[1].Bucket, Sum: 1, Count: 1, Max: 2}}
	if summarize(q, a) == summarize(q, b) {
		t.Error("answers that differ in a maximum are summarized the same")
	}
	if !strings.HasPrefix(summarize(q, a), "rows=2 sum=6 count=3 digest=") {
		t.Errorf("summary %q", summarize(q, a))
	}
	if !equalRows(a, append([]row(nil), a...)) || equalRows(a, b) || equalRows(a, a[:1]) {
		t.Error("equalRows")
	}
	top := topRows([]row{{Customer: "b", Sum: 5}, {Customer: "a", Sum: 5}, {Customer: "c", Sum: 9}})
	if top[0].Customer != "c" || top[1].Customer != "a" {
		t.Errorf("top %v", top)
	}
}

func TestLagSummary(t *testing.T) {
	ds := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond, 1000 * time.Millisecond}
	lo, mean, p50, p95, hi := summary(ds)
	if lo != 10 || hi != 1000 || p50 != 30 || p95 != 1000 || mean != 220 {
		t.Errorf("%v %v %v %v %v", lo, mean, p50, p95, hi)
	}
	if joinMS(ds[:2]) != "10.0,20.0" {
		t.Errorf("joinMS %q", joinMS(ds[:2]))
	}
	if got := trialCount(harness.Params{Duration: time.Second}, time.Second); got != minTrials {
		t.Errorf("trials %d", got)
	}
	if got := trialCount(harness.Params{Duration: 10 * time.Second}, time.Second); got != 20 {
		t.Errorf("trials %d", got)
	}
	if got := trialCount(harness.Params{Duration: time.Minute}, 10*time.Millisecond); got != maxTrials {
		t.Errorf("trials %d", got)
	}
	if got := pendingTargets(10_000); fmt.Sprint(got) != "[0 100 1000]" {
		t.Errorf("pending %v", got)
	}
}

func TestVariantsPerSystem(t *testing.T) {
	kv := testutil.Kvutil(t)
	want := map[string]string{
		"agg-write": "collector", "agg-hot-group": "collector", "agg-read": "exact,collected",
		"agg-freshness": "exact,collected", "agg-rebuild": "collector",
	}
	got := map[string]string{}
	for _, w := range harness.All() {
		if w.Group() != "agg" {
			continue
		}
		var names []string
		for _, v := range w.Variants(kv) {
			names = append(names, v.Name())
		}
		got[w.Name()] = strings.Join(names, ",")
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("variants on kvutil %v, want %v", got, want)
	}
}

func runKV(t *testing.T, name string, scale int) []metrics.Result {
	t.Helper()
	t.Setenv(envInterval, "30ms")
	sel, err := harness.Select(name)
	if err != nil {
		t.Fatal(err)
	}
	return testutil.Run(t, sel[0], testutil.Kvutil(t), testutil.Params(scale))
}

func casesOf(rs []metrics.Result) map[string]metrics.Result {
	m := map[string]metrics.Result{}
	for _, r := range rs {
		m[r.Variant+"/"+r.Case+"/"+fmt.Sprint(r.Params.Concurrency)] = r
	}
	return m
}

func TestWriteRunsOnKvutil(t *testing.T) {
	rs := casesOf(runKV(t, "agg-write", 2000))
	for _, k := range []string{"collector/load/", "collector/insert/1", "collector/insert/2", "collector/update/1", "collector/update/2",
		"collector/delete/1", "collector/delete/2", "collector/insert-background/1"} {
		found := false
		for key := range rs {
			if strings.HasPrefix(key, k) {
				found = true
			}
		}
		if !found {
			t.Errorf("no result for %s", k)
		}
	}
	for key, r := range rs {
		if strings.Contains(key, "/insert/") || strings.Contains(key, "/update/") || strings.Contains(key, "/delete/") {
			if r.Metrics.Ops == 0 || r.Metrics.Errors != 0 {
				t.Errorf("%s: %d ops, %d errors %v", key, r.Metrics.Ops, r.Metrics.Errors, r.Metrics.ErrorSample)
			}
		}
	}
	if r := rs["collector/insert/1"]; r.Params.Case["span_days"] != float64(365) || r.Params.Case["interval_ms"] != float64(30) {
		t.Errorf("params %v", r.Params.Case)
	}
}

func TestHotGroupHasNoRetriesFromTheAggregates(t *testing.T) {
	rs := casesOf(runKV(t, "agg-hot-group", 2000))
	hot, ok := rs["collector/insert-hot/2"]
	if !ok {
		t.Fatalf("results %v", rs)
	}
	if hot.Metrics.Ops == 0 || hot.Metrics.Errors != 0 {
		t.Errorf("%d ops, %d errors", hot.Metrics.Ops, hot.Metrics.Errors)
	}
	// The writers touch different records and only note their changes for the aggregations, so
	// nothing conflicts.
	if hot.Metrics.Retries != 0 {
		t.Errorf("%d retries of the writers of one customer", hot.Metrics.Retries)
	}
	if _, ok := rs["collector/insert-spread/2"]; !ok {
		t.Error("no insert-spread")
	}
	if bg, ok := rs["collector/insert-hot-background/2"]; !ok || bg.Metrics.Extra["collector_errors"] != 0 {
		t.Errorf("background %+v", bg.Metrics.Extra)
	}
}

func TestReadRunsOnKvutil(t *testing.T) {
	rs := casesOf(runKV(t, "agg-read", 2000))
	for _, variant := range []string{kvExact, kvCollected} {
		for _, c := range readCases() {
			for _, conc := range []int{1, 2} {
				r, ok := rs[fmt.Sprintf("%s/%s/%d", variant, c.name, conc)]
				if !ok || r.Metrics.Ops == 0 || r.Metrics.Errors != 0 || r.Metrics.Units == 0 {
					t.Errorf("%s %s %d: %+v", variant, c.name, conc, r.Metrics)
				}
			}
		}
	}
}

func TestRebuildRunsOnKvutil(t *testing.T) {
	rs := casesOf(runKV(t, "agg-rebuild", 1500))
	for _, k := range []string{"load-events", "migrate", "rebuild-sum", "rebuild-count", "rebuild-max"} {
		r, ok := rs["collector/"+k+"/1"]
		if !ok && k == "load-events" {
			r, ok = rs["collector/"+k+"/2"]
		}
		if !ok || r.Metrics.Records != 1500 || r.Metrics.ElapsedSec <= 0 {
			t.Errorf("%s: %+v", k, r.Metrics)
		}
	}
	st, ok := rs["collector/storage/1"]
	if !ok {
		t.Fatal("no storage")
	}
	if st.Metrics.Extra["aggregate_bytes"] <= 0 || st.Metrics.Extra["events_bytes"] <= 0 || st.Params.Case["logical"] != true {
		t.Errorf("storage %v %v", st.Metrics.Extra, st.Params.Case)
	}
}

func TestFreshnessRunsOnKvutil(t *testing.T) {
	rs := casesOf(runKV(t, "agg-freshness", 3000))
	for _, variant := range []string{kvExact, kvCollected} {
		for _, kind := range []string{"lag-insert", "lag-change-old"} {
			r, ok := rs[fmt.Sprintf("%s/%s/%d", variant, kind, 2)]
			if !ok {
				t.Fatalf("%s %s: no result in %v", variant, kind, keys(rs))
			}
			x := r.Metrics.Extra
			if x["trials"] < float64(minTrials) || x["timeouts"] != 0 || x["lag_all_ms_max"] < x["lag_daily_ms_min"] {
				t.Errorf("%s %s: %v", variant, kind, x)
			}
			if variant == kvExact && x["lag_all_ms_p95"] > 500 {
				t.Errorf("an exact read lags %.0f ms", x["lag_all_ms_p95"])
			}
			if len(strings.Split(r.Metrics.Info["lag_all_ms"], ",")) != int(x["trials"]) {
				t.Errorf("lags %q for %v trials", r.Metrics.Info["lag_all_ms"], x["trials"])
			}
		}
		for _, k := range []string{"daily30-pending-0", "daily30-pending-100", "total-pending-1000"} {
			if r, ok := rs[variant+"/"+k+"/1"]; !ok || r.Metrics.Ops == 0 {
				t.Errorf("%s %s: %+v", variant, k, r.Metrics)
			}
		}
	}
}

func keys(m map[string]metrics.Result) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// The verification, on kvutil alone: its variants must agree among themselves, which is what the
// comparison with the other systems then extends.
func TestVerifyOnKvutil(t *testing.T) {
	for _, name := range []string{"agg-read", "agg-write", "agg-hot-group", "agg-freshness", "agg-rebuild"} {
		t.Run(name, func(t *testing.T) {
			sel, err := harness.Select(name)
			if err != nil {
				t.Fatal(err)
			}
			outs, diffs := testutil.Verify(t, sel[0], testutil.Kvutil(t), testutil.Params(600))
			if len(diffs) > 0 {
				t.Fatalf("differences: %v", diffs)
			}
			groups := map[string]int{}
			for _, o := range outs {
				groups[o.Group]++
				if len(o.Lines) < 4 {
					t.Errorf("%s has %d lines", o.Label(), len(o.Lines))
				}
			}
			if name == "agg-read" && (groups["caught-up"] != 2 || groups["pending"] != 1 || groups["pending-old"] != 1) {
				t.Errorf("groups %v", groups)
			}
		})
	}
}
