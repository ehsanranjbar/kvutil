package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

type fakeSystem struct{ retries atomic.Int64 }

func (*fakeSystem) Name() string                { return "fake" }
func (*fakeSystem) Version() string             { return "v0" }
func (*fakeSystem) Settings() map[string]string { return map[string]string{"a": "b"} }
func (*fakeSystem) Close() error                { return nil }
func (f *fakeSystem) Retries() int64            { return f.retries.Load() }

// newContext returns a run context that writes its results to a file, and a function that reads
// them back.
func newContext(t *testing.T, p Params) (*RunContext, func() []metrics.Result) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "r.jsonl")
	sink, err := metrics.NewSink(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	rc := &RunContext{Params: p, Sys: &fakeSystem{}, Workload: "w", Variant: "v", Sink: sink}
	return rc, func() []metrics.Result {
		sink.Close()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var rs []metrics.Result
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var r metrics.Result
			if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
				t.Fatal(err)
			}
			rs = append(rs, r)
		}
		return rs
	}
}

func TestMeasureCountsAndExcludesWarmup(t *testing.T) {
	p := Params{Scale: 100, Values: "small", Durability: Durable, Concurrency: 2, Duration: 300 * time.Millisecond, Warmup: 200 * time.Millisecond, Seed: 1}
	rc, results := newContext(t, p)

	var calls atomic.Int64
	err := rc.Measure(context.Background(), Case{Name: "sleep", Concurrency: 2, Unit: "rows"}, func(ctx context.Context, w *Worker) error {
		calls.Add(1)
		time.Sleep(time.Millisecond)
		w.Add(10)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rs := results()
	if len(rs) != 1 {
		t.Fatalf("%d results", len(rs))
	}
	m := rs[0].Metrics
	// Two workers doing a 1 ms operation: at most 500 in 250 ms.. of a 300 ms interval, and the
	// warm-up's operations are not counted.
	if m.Ops < 100 || m.Ops > 600 {
		t.Errorf("ops = %d", m.Ops)
	}
	if calls.Load() <= m.Ops {
		t.Errorf("%d operations ran, %d are measured: the warm-up is counted", calls.Load(), m.Ops)
	}
	if m.Errors != 0 || m.Latency == nil || m.Latency.Count != m.Ops {
		t.Errorf("errors = %d, latency = %+v", m.Errors, m.Latency)
	}
	if m.Latency.P50Us < 900 || m.Latency.P50Us > 5000 {
		t.Errorf("p50 = %v us for a sleep of 1 ms", m.Latency.P50Us)
	}
	if m.ElapsedSec < 0.25 || m.ElapsedSec > 0.5 {
		t.Errorf("elapsed = %v", m.ElapsedSec)
	}
	if m.Units != m.Ops*10 || m.UnitName != "rows" || m.UnitsPerSec < m.OpsPerSec*9 {
		t.Errorf("units = %d %s (%v/s) for %d ops", m.Units, m.UnitName, m.UnitsPerSec, m.Ops)
	}
	if rs[0].System != "fake" || rs[0].SystemVersion != "v0" || rs[0].Settings["a"] != "b" || rs[0].Case != "sleep" ||
		rs[0].Params.Concurrency != 2 || rs[0].Params.DurationSec != 0.3 || rs[0].Params.WarmupSec != 0.2 {
		t.Errorf("the result is %+v", rs[0])
	}
}

func TestCaseInfoIsInTheResult(t *testing.T) {
	p := Params{Scale: 100, Values: "small", Durability: Durable, Concurrency: 1, Duration: 20 * time.Millisecond, Warmup: 5 * time.Millisecond, Seed: 1}
	rc, results := newContext(t, p)

	info := map[string]string{"plan": "Seq Scan (rows=5)", "plan_nodes": "Seq Scan"}
	err := rc.Measure(context.Background(), Case{Name: "q", Info: info}, func(context.Context, *Worker) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Report(context.Background(), Case{Name: "r", Info: map[string]string{"x": "y"}}, time.Now(), time.Now(), metrics.Metrics{}); err != nil {
		t.Fatal(err)
	}
	rs := results()
	if len(rs) != 2 {
		t.Fatalf("%d results", len(rs))
	}
	if rs[0].Metrics.Info["plan"] != "Seq Scan (rows=5)" || rs[0].Metrics.Info["plan_nodes"] != "Seq Scan" {
		t.Errorf("info %v", rs[0].Metrics.Info)
	}
	if rs[1].Metrics.Info["x"] != "y" {
		t.Errorf("info %v", rs[1].Metrics.Info)
	}
	if line := metrics.Summary(&rs[0]); !strings.Contains(line, "plan=Seq Scan") {
		t.Errorf("summary %q", line)
	}
}

func TestMeasureBeginExcludesPreparation(t *testing.T) {
	p := Params{Concurrency: 1, Duration: 200 * time.Millisecond, Seed: 1}
	rc, results := newContext(t, p)
	err := rc.Measure(context.Background(), Case{Name: "begin"}, func(ctx context.Context, w *Worker) error {
		time.Sleep(5 * time.Millisecond) // preparation
		w.Begin()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lat := results()[0].Metrics.Latency; lat.P99Us > 2000 {
		t.Errorf("the preparation of 5 ms is measured: p99 = %v us", lat.P99Us)
	}
}

func TestMeasureCountsErrorsAndRetries(t *testing.T) {
	p := Params{Concurrency: 1, Duration: 200 * time.Millisecond, Seed: 1}
	rc, results := newContext(t, p)
	sys := rc.Sys.(*fakeSystem)

	var n atomic.Int64
	boom := errors.New("boom")
	err := rc.Measure(context.Background(), Case{Name: "errors"}, func(ctx context.Context, w *Worker) error {
		time.Sleep(200 * time.Microsecond)
		if n.Add(1)%4 == 0 {
			sys.retries.Add(1)
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	m := results()[0].Metrics
	if m.Errors < 10 || m.Ops < 3*10 {
		t.Errorf("ops = %d, errors = %d", m.Ops, m.Errors)
	}
	if m.Retries != m.Errors {
		t.Errorf("retries = %d, errors = %d", m.Retries, m.Errors)
	}
	if len(m.ErrorSample) == 0 || m.ErrorSample[0] != "boom" {
		t.Errorf("error sample = %v", m.ErrorSample)
	}
}

func TestMeasureFailsWhenEverythingFails(t *testing.T) {
	p := Params{Concurrency: 2, Duration: 100 * time.Millisecond, Seed: 1}
	rc, results := newContext(t, p)
	err := rc.Measure(context.Background(), Case{Name: "broken"}, func(ctx context.Context, w *Worker) error {
		time.Sleep(time.Millisecond)
		return errors.New("always")
	})
	if err == nil || !strings.Contains(err.Error(), "always") {
		t.Fatalf("err = %v", err)
	}
	if rs := results(); len(rs) != 1 || rs[0].Metrics.Errors == 0 || rs[0].Metrics.Latency != nil {
		t.Errorf("results = %+v", rs)
	}
}

func TestMeasureStopsOnErrDone(t *testing.T) {
	p := Params{Concurrency: 3, Duration: 5 * time.Second, Seed: 1}
	rc, results := newContext(t, p)
	var n atomic.Int64
	start := time.Now()
	err := rc.Measure(context.Background(), Case{Name: "pool"}, func(ctx context.Context, w *Worker) error {
		if n.Add(1) > 100 {
			return ErrDone
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("it ran %v after the pool was empty", time.Since(start))
	}
	if m := results()[0].Metrics; m.Ops != 100 || m.Errors != 0 {
		t.Errorf("ops = %d, errors = %d", m.Ops, m.Errors)
	}
}

func TestMeasureOperations(t *testing.T) {
	p := Params{Concurrency: 4, Duration: 5 * time.Second, Warmup: 5 * time.Second, Seed: 1}
	rc, results := newContext(t, p)
	var n atomic.Int64
	start := time.Now()
	err := rc.Measure(context.Background(), Case{Name: "ops", Ops: 1000, WarmupOps: 100}, func(ctx context.Context, w *Worker) error {
		n.Add(1)
		time.Sleep(50 * time.Microsecond)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("it took %v: the warm-up was by time", time.Since(start))
	}
	m := results()[0].Metrics
	if n.Load() != 1000 || m.Ops != 900 {
		t.Errorf("%d operations ran, %d are measured", n.Load(), m.Ops)
	}
	if m.OpsPerSec <= 0 || m.ElapsedSec <= 0 {
		t.Errorf("ops/s = %v over %v", m.OpsPerSec, m.ElapsedSec)
	}
}

func TestMeasureOperationsStopAtTheDuration(t *testing.T) {
	p := Params{Concurrency: 2, Duration: 200 * time.Millisecond, Seed: 1}
	rc, results := newContext(t, p)
	start := time.Now()
	err := rc.Measure(context.Background(), Case{Name: "ops", Ops: 1_000_000, WarmupOps: 10}, func(ctx context.Context, w *Worker) error {
		time.Sleep(time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("it took %v", d)
	}
	if m := results()[0].Metrics; m.Ops < 50 || m.Ops > 600 {
		t.Errorf("ops = %d", m.Ops)
	}
}

func TestWorkersAreSeededAlike(t *testing.T) {
	first := func() []uint64 {
		p := Params{Concurrency: 1, Duration: 50 * time.Millisecond, Seed: 42}
		rc, _ := newContext(t, p)
		var got []uint64
		rc.Measure(context.Background(), Case{Name: "rand", Ops: 5}, func(ctx context.Context, w *Worker) error {
			got = append(got, w.Rand.Uint64())
			return nil
		})
		return got
	}
	a, b := first(), first()
	if len(a) != 5 || len(b) != 5 {
		t.Fatalf("%d and %d numbers", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("the numbers differ: %v and %v", a, b)
		}
	}
}

func TestPhase(t *testing.T) {
	rc, results := newContext(t, Params{Concurrency: 4, Seed: 1})
	rc.SizeFn = func(context.Context) (Sizes, error) { return Sizes{Data: 1234, WAL: 56}, nil }
	err := rc.Phase(context.Background(), Case{Name: "load", Concurrency: 1, Params: map[string]any{"batch": 100}}, func(ctx context.Context) (int64, error) {
		time.Sleep(100 * time.Millisecond)
		return 5000, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	r := results()[0]
	if r.Metrics.Records != 5000 || r.Metrics.RecordsPerSec < 30000 || r.Metrics.RecordsPerSec > 50000 {
		t.Errorf("records = %d at %v/s", r.Metrics.Records, r.Metrics.RecordsPerSec)
	}
	if r.Metrics.DiskBytes != 1234 || r.Metrics.WALBytes != 56 {
		t.Errorf("sizes = %d, %d", r.Metrics.DiskBytes, r.Metrics.WALBytes)
	}
	if r.Params.Concurrency != 1 || r.Params.Case["batch"] != float64(100) || r.Params.DurationSec != 0 {
		t.Errorf("params = %+v", r.Params)
	}

	err = rc.Phase(context.Background(), Case{Name: "bad"}, func(ctx context.Context) (int64, error) { return 0, errors.New("nope") })
	if err == nil || !strings.Contains(err.Error(), "bad: nope") {
		t.Errorf("err = %v", err)
	}
}

type fakeWorkload struct{ name, group string }

func (w fakeWorkload) Name() string              { return w.name }
func (w fakeWorkload) Group() string             { return w.group }
func (w fakeWorkload) Description() string       { return "" }
func (w fakeWorkload) Variants(System) []Variant { return nil }
func (w fakeWorkload) Verify(context.Context, System, Params) ([]Output, error) {
	return nil, nil
}

func TestSelect(t *testing.T) {
	registry = nil
	Register(fakeWorkload{"a1", "ga"})
	Register(fakeWorkload{"a2", "ga"})
	Register(fakeWorkload{"b1", "gb"})

	names := func(spec string) string {
		ws, err := Select(spec)
		if err != nil {
			return "error: " + err.Error()
		}
		var ns []string
		for _, w := range ws {
			ns = append(ns, w.Name())
		}
		return strings.Join(ns, ",")
	}
	for spec, want := range map[string]string{
		"all":      "a1,a2,b1",
		"ga":       "a1,a2",
		"b1":       "b1",
		"b1, ga":   "b1,a1,a2",
		"a1,ga,a1": "a1,a2",
	} {
		if got := names(spec); got != want {
			t.Errorf("Select(%q) = %s, want %s", spec, got, want)
		}
	}
	if got := names("nothing"); !strings.HasPrefix(got, "error: no workload or group \"nothing\"") {
		t.Errorf("Select(nothing) = %s", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("a name that is registered twice does not panic")
		}
	}()
	Register(fakeWorkload{"a1", "ga"})
}

func TestCompare(t *testing.T) {
	same := []Labeled{
		{System: "x", Output: Output{Variant: "a", Lines: []string{"1", "2"}}},
		{System: "y", Output: Output{Variant: "b", Lines: []string{"1", "2"}}},
	}
	if d := Compare(same, 0); len(d) != 0 {
		t.Errorf("equal outputs differ: %v", d)
	}

	differ := []Labeled{
		{System: "x", Output: Output{Variant: "a", Lines: []string{"1", "2", "3"}}},
		{System: "y", Output: Output{Variant: "b", Lines: []string{"1", "9"}}},
	}
	d := Compare(differ, 0)
	if len(d) != 2 || !strings.Contains(d[0], "3 lines against 2") || !strings.Contains(d[1], `"2" against "9"`) {
		t.Errorf("differences = %v", d)
	}

	// Only the outputs of a group are the same.
	groups := []Labeled{
		{System: "x", Output: Output{Variant: "a", Group: "g1", Lines: []string{"1"}}},
		{System: "x", Output: Output{Variant: "b", Group: "g2", Lines: []string{"2"}}},
		{System: "y", Output: Output{Variant: "a", Group: "g1", Lines: []string{"1"}}},
		{System: "y", Output: Output{Variant: "b", Group: "g2", Lines: []string{"3"}}},
	}
	d = Compare(groups, 0)
	if len(d) != 1 || !strings.Contains(d[0], "x/b vs y/b") {
		t.Errorf("differences = %v", d)
	}

	many := []Labeled{
		{Output: Output{Lines: make([]string, 50)}},
		{Output: Output{Lines: strings.Split(strings.Repeat("x,", 49)+"x", ",")}},
	}
	if d := Compare(many, 3); len(d) != 3 {
		t.Errorf("%d differences reported, the limit is 3", len(d))
	}
}

func TestLevels(t *testing.T) {
	for in, want := range map[int]string{0: "1", 1: "1", 4: "1,4"} {
		var got []string
		for _, l := range Levels(in) {
			got = append(got, string(rune('0'+l)))
		}
		if strings.Join(got, ",") != want {
			t.Errorf("Levels(%d) = %v", in, Levels(in))
		}
	}
}
