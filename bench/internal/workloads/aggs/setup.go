package aggs

import (
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
)

// The environment of the workloads of aggregations (besides the flags of kvbench):
//
//	KVB_AGG_END        the end of the span of the events, a date like 2026-10-21: the day after the
//	                   last day with events, which is the day the stream of new events is added in.
//	                   The default is today in UTC. run.sh sets it once for the whole run, so that
//	                   every system gets the same events even if the run crosses midnight
//	KVB_AGG_SPAN_DAYS  the days that the events are spread over (365)
//	KVB_AGG_INTERVAL   the interval of the collector of kvutil, and the schedule of the refresh
//	                   policies of TimescaleDB (1s)
const (
	envEnd      = "KVB_AGG_END"
	envSpanDays = "KVB_AGG_SPAN_DAYS"
	envInterval = "KVB_AGG_INTERVAL"
)

// setup is what the workloads of aggregations share: the events, and the interval of the
// background work.
type setup struct {
	spec dataset.EventSpec
	// interval is how often kvutil's collector runs, and how often the refresh policies of
	// TimescaleDB are scheduled.
	interval time.Duration
	// workers is the number of workers that load the events.
	workers int
}

// newSetup reads the environment and the parameters of the run.
func newSetup(p harness.Params) setup {
	end := time.Now().UTC().Truncate(24 * time.Hour)
	if v := os.Getenv(envEnd); v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			end = t
		}
	}
	span := dataset.DefaultSpanDays
	if n, err := strconv.Atoi(os.Getenv(envSpanDays)); err == nil && n > 0 {
		span = n
	}
	interval := time.Second
	if d, err := time.ParseDuration(os.Getenv(envInterval)); err == nil && d >= 10*time.Millisecond {
		interval = d
	}
	return setup{
		spec:     dataset.EventSpec{Seed: p.Seed, Events: p.Scale, End: end, SpanDays: span},
		interval: interval,
		workers:  min(max(p.Concurrency, 1), 8),
	}
}

// caseParams are the parameters of the dataset that go into the results.
func (s setup) caseParams(extra map[string]any) map[string]any {
	m := map[string]any{
		"span_days":    s.spec.SpanDays,
		"end":          s.spec.End.Format("2006-01-02"),
		"customers":    s.spec.NumCustomers(),
		"interval_ms":  s.interval.Milliseconds(),
		"customer_law": "zipf-1",
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// customer returns the name of a customer that is drawn like the customers of the events are: the
// reads go to the customers that have the events.
func (s setup) customer(r *rand.Rand) string {
	return dataset.CustomerName(s.spec.RankOfUniform(r.Float64()))
}

// window returns the spans of a unit that the whole span of the events is in.
func (s setup) window(u unit) (from, to time.Time) {
	return startOf(u, s.spec.Start()), alignUp(u, s.spec.End)
}

// readCase is a read of the workload of reads: a query that is drawn at random.
type readCase struct {
	name string
	gen  func(s setup, r *rand.Rand) query
}

const dayLen = 24 * time.Hour

// readCases are the reads of agg-read, in the order that they run.
func readCases() []readCase {
	series := func(u unit, days int) func(s setup, r *rand.Rand) query {
		return func(s setup, r *rand.Rand) query {
			from, to := s.window(u)
			if days > 0 && int(to.Sub(from)/dayLen) > days {
				span := int(to.Sub(from) / dayLen)
				from = from.Add(time.Duration(r.IntN(span-days+1)) * dayLen)
				to = from.Add(time.Duration(days) * dayLen)
			}
			return query{Kind: qSeries, Unit: u, Customer: s.customer(r), From: from, To: to}
		}
	}
	return []readCase{
		{"daily-30", series(day, 30)},
		{"daily-365", series(day, 0)},
		{"weekly-year", series(week, 0)},
		{"monthly-year", series(month, 0)},
		{"total", func(s setup, r *rand.Rand) query {
			return query{Kind: qTotal, Customer: s.customer(r)}
		}},
		{"top10-monthly", func(s setup, r *rand.Rand) query {
			from, to := s.window(month)
			months := 0
			for t := from; t.Before(to); t = nextOf(month, t) {
				months++
			}
			from = from.AddDate(0, r.IntN(months), 0)
			return query{Kind: qTop, Unit: month, From: from, To: nextOf(month, from)}
		}},
	}
}
