package report

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

func load(t *testing.T, name string) *Run {
	t.Helper()
	run, err := LoadRun(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func mustContain(t *testing.T, text string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(text, p) {
			t.Errorf("the report does not have %q", p)
		}
	}
}

func TestParseSample(t *testing.T) {
	s, ok := parseSample("2026-10-20T10:00:30Z,kvbench-postgres,150.50%,1.5GiB / 4GiB,37.5%,1kB / 1kB,0B / 0B,20")
	if !ok || s.Name != "kvbench-postgres" || s.CPUCores != 1.505 || s.MemBytes != 1.5*(1<<30) {
		t.Fatalf("got %+v %v", s, ok)
	}
	s, ok = parseSample("2026-10-20T10:00:32Z,kvbench-postgres,--,--,--,--,--,--")
	if !ok || !math.IsNaN(s.CPUCores) || !math.IsNaN(s.MemBytes) {
		t.Fatalf("a sample that docker did not measure must be NaN, got %+v", s)
	}
	for _, bad := range []string{"ts,name,cpu_perc,mem_usage", "", "x,y", "notatime,n,1%,1MiB / 2MiB"} {
		if _, ok := parseSample(bad); ok {
			t.Errorf("%q is not a sample", bad)
		}
	}
	for in, want := range map[string]float64{"0B": 0, "1.93MB": 1.93e6, "2KiB": 2048, "3kB": 3000} {
		if got, ok := parseSize(in); !ok || got != want {
			t.Errorf("parseSize(%q) = %v %v", in, got, ok)
		}
	}
}

func TestLoadRun(t *testing.T) {
	mac := load(t, "mac")
	if len(mac.Results) != 19 || !mac.HasMeta || mac.Meta.KvutilCommit != "abc123" || len(mac.Stats) != 7 {
		t.Fatalf("results=%d meta=%v commit=%q stats=%d", len(mac.Results), mac.HasMeta, mac.Meta.KvutilCommit, len(mac.Stats))
	}
	if !mac.Verify.Present || !mac.Verify.Passed {
		t.Errorf("verify of mac: %+v", mac.Verify)
	}
	if v := load(t, "linux").Verify; !v.Present || v.Passed {
		t.Errorf("verify of linux must have failed: %+v", v)
	}

	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "results-durable.jsonl"), []byte("not json\n\n{\"workload\":\"w\",\"system\":\"kvutil\"}\n"), 0o644)
	run, err := LoadRun(dir)
	if err != nil || len(run.Results) != 1 || run.Skipped != 1 || run.HasMeta || run.Verify.Present {
		t.Fatalf("a directory with a bad line and no meta: %+v %v", run, err)
	}
	if _, err := LoadRun(t.TempDir()); err == nil {
		t.Error("a directory without results must be an error")
	}
}

func TestJoinStats(t *testing.T) {
	mac := load(t, "mac")
	rv := newRunView(mac, "mac")
	find := func(workload, system, cs string) *row {
		for _, x := range rv.rows {
			if x.r.Workload == workload && x.r.System == system && x.r.Case == cs && x.conc != 1 || (x.r.Workload == workload && x.r.System == system && x.r.Case == cs && cs == "load") {
				return x
			}
		}
		t.Fatalf("no %s %s %s", workload, system, cs)
		return nil
	}
	// Four samples fall in the interval of the case; the one that is "--" does not count.
	pg := find("kv-basic", "postgres", "get")
	if pg.srv.n != 3 || pg.srv.cpu != 2 || pg.srv.mem != 2*(1<<30) {
		t.Errorf("postgres get: %+v", pg.srv)
	}
	// No sample falls in the interval: not available, and not zero.
	sw := find("selectivity-sweep", "postgres", "p1-ordered")
	if !sw.srv.own || sw.srv.n != 0 || !math.IsNaN(sw.srv.cpu) || !math.IsNaN(sw.srv.mem) {
		t.Errorf("postgres sweep: %+v", sw.srv)
	}
	// kvutil is in the harness: it has no server.
	if kv := find("kv-basic", "kvutil", "get"); kv.srv.own {
		t.Errorf("kvutil has no server: %+v", kv.srv)
	}
}

func TestBuildOneRunContent(t *testing.T) {
	doc := Build([]*Run{load(t, "mac")})
	md := doc.Markdown()

	mustContain(t, md,
		"> **Warning:** Docker Desktop (macOS)",
		"| host | mac |",
		"| PostgreSQL | PostgreSQL 18.6 |",
		"| verify | passed |",
		"results have errors", "boom",
		"### kv-basic", "### selectivity-sweep", "### key-order-fetch", "### concurrency", "### storage", "### agg-freshness",
		// The ratio against the fastest, and the fastest in bold.
		"| ops/s | **1,000** | 500 |", "| vs the fastest | 1.00x | 0.50x |",
		"| records/s | **20,000** | 12,500 |",
		// A figure that was not recorded is n/a: the peak memory of the harness in the PostgreSQL case, and the samples of the server.
		"| get | 4 | postgres/bytea | 1.00 | n/a | 2.00 | 2.0 GiB | 3 |",
		"| get | 4 | kvutil/typed | 2.00 | 1.0 MiB | embedded | embedded |",
		// The special sections.
		"Plan per step", "IndexScan[a]", "Index Scan[a_idx]", "Seq Scan",
		"Lag distribution", "Cost of pending events on a read",
		"Throughput and retries against workers",
		"Bytes on disk after each step", "Write amplification", "Space before and after each step", "-33.3%",
		"Fetching K records by key",
	)

	page := doc.HTML()
	mustContain(t, page, "<!doctype html>", "<svg", "prefers-color-scheme:dark", "--c-kvutil", "--c-postgres", "<table>", "<details")
	for _, bad := range []string{"<script", "http://", "https://", "<link", "@import"} {
		if strings.Contains(strings.ReplaceAll(page, "http://www.w3.org/2000/svg", ""), bad) {
			t.Errorf("the page is not self-contained: has %q", bad)
		}
	}
	// The colour of a system is defined once.
	if n := strings.Count(page, "--c-kvutil:#"); n != 2 { // light and dark
		t.Errorf("the colour of kvutil is defined %d times", n)
	}
	if strings.Count(page, "<svg") < 10 {
		t.Errorf("only %d charts", strings.Count(page, "<svg"))
	}
}

func TestCompare(t *testing.T) {
	doc := Build([]*Run{load(t, "mac"), load(t, "linux")})
	md := doc.Markdown()
	mustContain(t, md,
		"## Comparison of runs",
		"> **Warning:** linux: kvbench verify FAILED",
		"mac: Docker Desktop (macOS)",
		// kvutil get: 2000 against 1000.
		"| get | 4 | durable | kvutil/typed | 1,000 | 2,000 | 2.00x |",
		// The load differs in no parameter but the first run has it at 20,000 and the second at 40,000.
		"| load | 1 | durable | kvutil/typed | 20,000 | 40,000 | 2.00x |",
		// The scale of the case of PostgreSQL differs.
		"Not compared: different parameters", "scale 100000 / 1000000",
		"Not compared: in one run only",
		"## Results: mac", "## Results: linux",
	)
	// The case that PostgreSQL has with another scale is not in the table.
	if strings.Contains(md, "| get | 4 | durable | postgres/bytea") {
		t.Error("a case with other parameters was compared")
	}
}

func TestWriteFiles(t *testing.T) {
	out := t.TempDir()
	if err := Generate(out, []string{"testdata/mac", "testdata/linux"}); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"report.md", "report.html", "runs.csv", "r1-mac-durable-kv-basic.csv", "r2-linux-durable-kv-basic.csv", "compare-kv-basic.csv"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Errorf("missing %s: %v", f, err)
		}
	}
	charts, _ := filepath.Glob(filepath.Join(out, "charts", "*.svg"))
	if len(charts) < 10 {
		t.Errorf("%d charts", len(charts))
	}
	svg, _ := os.ReadFile(charts[0])
	mustContain(t, string(svg), "<svg", "<style>", "--c-kvutil")

	// The CSV of a workload leaves a figure that was not recorded empty, and does not write zero.
	f, err := os.Open(filepath.Join(out, "r1-mac-durable-kv-basic.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	col := map[string]int{}
	for i, h := range recs[0] {
		col[h] = i
	}
	found := false
	for _, r := range recs[1:] {
		if r[col["system"]] == "postgres" && r[col["case"]] == "get" {
			found = true
			if r[col["harness_max_rss_bytes"]] != "" || r[col["ops_per_sec"]] != "500" || r[col["server_cpu_cores_mean"]] != "2" {
				t.Errorf("row: rss=%q ops=%q cpu=%q", r[col["harness_max_rss_bytes"]], r[col["ops_per_sec"]], r[col["server_cpu_cores_mean"]])
			}
		}
		if r[col["system"]] == "kvutil" && r[col["server_cpu_cores_mean"]] != "" {
			t.Errorf("kvutil has no server figures: %q", r[col["server_cpu_cores_mean"]])
		}
	}
	if !found {
		t.Error("no row for postgres get")
	}
}

// A workload that the report has never heard of appears in the generic tables, with its extras
// and info, and without any code for it.
func TestNewWorkloadIsGeneric(t *testing.T) {
	mk := func(system string, ops float64) *metrics.Result {
		return &metrics.Result{
			Start: time.Unix(1000, 0), End: time.Unix(1010, 0), System: system, Workload: "brand-new", Variant: "v", Case: "c1",
			Params: metrics.Params{Scale: 1000000, Values: "small", Durability: "durable", Concurrency: 2, DurationSec: 10},
			Metrics: metrics.Metrics{OpsPerSec: ops, Ops: int64(ops) * 10, ElapsedSec: 10,
				Latency: &metrics.Latency{P50Us: 10, P95Us: 20, P99Us: 30},
				Extra:   map[string]float64{"widget_bytes": 2048}, Info: map[string]string{"note": "hello"}},
		}
	}
	run := &Run{Name: "x", Results: []*metrics.Result{mk("kvutil", 400), mk("postgres", 100)}}
	md := Build([]*Run{run}).Markdown()
	mustContain(t, md, "### brand-new", "| widget_bytes | 2.0 KiB | 2.0 KiB |", "| note | hello | hello |", "| vs the fastest | 1.00x | 0.25x |")
	if strings.Contains(md, "smoke scale") {
		t.Error("a run of a million records is not a smoke run")
	}
}

func TestCharts(t *testing.T) {
	bars := BarChart("b", "Throughput", "ops/s", []Bar{
		{"kvutil/typed", "kvutil", 100, "100"}, {"postgres/bytea", "postgres", 50, "50"}, {"timescale/x", "timescale", math.NaN(), ""},
	})
	in := bars.Inline()
	mustContain(t, in, `class="c-kvutil"`, `class="c-postgres"`, "n/a", "<svg")
	if strings.Contains(in, `c-timescale"><rect`) {
		t.Error("a figure that is not available has no bar")
	}

	line := LineChart("l", []Line{
		{Label: "a", System: "kvutil", X: []float64{1, 10, 100}, Y: []float64{5, 50, 500}},
		{Label: "b", System: "kvutil", Style: 1, X: []float64{1, 10, 100}, Y: []float64{math.NaN(), 20, 40}},
		{Label: "c", System: "postgres", X: []float64{1, 10}, Y: []float64{1, 2}},
	}, LineOpts{Title: "t", LogX: true, LogY: true})
	mustContain(t, line.Inline(), "<path", "stroke-dasharray", "c-postgres")
	if strings.Contains(line.Inline(), "NaN") || strings.Contains(line.Inline(), "Inf") {
		t.Error("NaN or Inf in the SVG")
	}

	// No data at all must not break the chart.
	empty := LineChart("e", nil, LineOpts{Title: "e", LogX: true, LogY: true})
	if strings.Contains(empty.Inline(), "NaN") {
		t.Error("NaN in an empty chart")
	}
	cat := LineChart("c", []Line{{Label: "a", System: "kvutil", X: []float64{0}, Y: []float64{3}}}, LineOpts{Title: "c", XTicks: []string{"only"}})
	if strings.Contains(cat.Inline(), "NaN") {
		t.Error("NaN in a chart of one category")
	}
}

func TestFormat(t *testing.T) {
	cases := map[string]string{
		fmtNum(math.NaN()): "n/a", fmtNum(1234567): "1,234,567", fmtNum(12.345): "12.35", fmtNum(0.0123): "0.0123",
		fmtRatio(0.4237): "0.42x", fmtRatio(12.34): "12.3x", fmtRatio(math.NaN()): "n/a",
		fmtUs(12.34): "12.3 µs", fmtUs(2500): "2.50 ms", fmtUs(3e6): "3.00 s", fmtBytes(1536): "1.5 KiB", fmtBytes(math.NaN()): "n/a",
		fmtSI(1500): "1.5k", fmtSI(20e6): "20M", fmtNs(1500): "1.5 µs",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
	if got := N(math.NaN(), fmtNum).Raw(); got != "" {
		t.Errorf("a number that is not available is empty in a CSV, not %q", got)
	}
	if got := ratio(1, 0); !math.IsNaN(got) {
		t.Errorf("ratio against zero is %v", got)
	}
}

func TestParamDiff(t *testing.T) {
	a := metrics.Params{Scale: 10, Values: "small", Case: map[string]any{"rows": 100.0, "only_a": 1.0}}
	b := metrics.Params{Scale: 20, Values: "small", Case: map[string]any{"rows": 100.0}}
	d := strings.Join(paramDiff(a, b), "; ")
	mustContain(t, d, "scale 10 / 20", "case.only_a only in the first run")
	if len(paramDiff(a, a)) != 0 {
		t.Error("the same parameters must not differ")
	}
}

// A case that was run again, in the same file or in a later one, is there once: the last result.
func TestRerunReplaces(t *testing.T) {
	line := func(system, cs string, conc int, ops float64) string {
		return fmt.Sprintf(`{"workload":"w","system":%q,"variant":"v","case":%q,"params":{"scale":1000,"values":"small","durability":"durable","concurrency":%d},"metrics":{"ops_per_sec":%v,"latency":{"p99_us":1}}}`+"\n", system, cs, conc, ops)
	}
	dir := t.TempDir()
	// kvutil get at 4 workers is run twice; at 1 worker, and the other case, and the other system, once.
	text := line("kvutil", "get", 4, 100) + line("kvutil", "get", 1, 10) + line("postgres", "get", 4, 50) + line("kvutil", "get", 4, 300) + line("kvutil", "scan", 4, 7)
	if err := os.WriteFile(filepath.Join(dir, "results-durable.jsonl"), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	run, err := LoadRun(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Results) != 4 || run.Replaced != 1 {
		t.Fatalf("results=%d replaced=%d", len(run.Results), run.Replaced)
	}
	for _, r := range run.Results {
		if r.System == "kvutil" && r.Case == "get" && r.Params.Concurrency == 4 && r.Metrics.OpsPerSec != 300 {
			t.Errorf("the first run of the case was kept: %v ops/s", r.Metrics.OpsPerSec)
		}
	}
	// The order of the others is kept, and the report says that results were left out.
	if run.Results[0].Params.Concurrency != 1 || run.Results[3].Case != "scan" {
		t.Errorf("order: %s %d ... %s", run.Results[0].Case, run.Results[0].Params.Concurrency, run.Results[3].Case)
	}
	mustContain(t, Build([]*Run{run}).Markdown(), "| results | 4 (1 earlier results of cases that were run again are left out) |")
}

// summaryRun is a run whose results are made for the summary: three systems with variants, a case
// that is a time, a case that is a size, a case of one system, and the levels of workers.
func summaryRun() *Run {
	mk := func(system, variant, cs string, conc int, m metrics.Metrics) *metrics.Result {
		return &metrics.Result{
			Start: time.Unix(1000, 0), End: time.Unix(1010, 0), System: system, Workload: "relations", Variant: variant, Case: cs,
			Params:  metrics.Params{Scale: 1000000, Values: "small", Durability: "durable", Concurrency: conc, DurationSec: 10},
			Metrics: m,
		}
	}
	timed := func(ops float64) metrics.Metrics {
		return metrics.Metrics{Ops: int64(ops), OpsPerSec: ops, ElapsedSec: 10, Latency: &metrics.Latency{P99Us: 5}}
	}
	phase := func(rps float64) metrics.Metrics {
		return metrics.Metrics{Records: 1000, RecordsPerSec: rps, ElapsedSec: 1000 / rps}
	}
	took := func(sec float64) metrics.Metrics { return metrics.Metrics{ElapsedSec: sec} }
	size := func(b int64) metrics.Metrics { return metrics.Metrics{DiskBytes: b, Extra: map[string]float64{"x": 1}} }
	return &Run{Name: "s", Results: []*metrics.Result{
		// 4 workers is there: the level of 8 is not shown. The best variant of each system counts.
		mk("kvutil", "a", "read", 4, timed(1000)), mk("kvutil", "b", "read", 4, timed(3000)), mk("kvutil", "a", "read", 8, timed(9000)),
		mk("postgres", "a", "read", 4, timed(1000)), mk("postgres", "b", "read", 4, timed(1500)),
		mk("timescale", "t", "read", 4, timed(2000)),
		// No such level: the largest one, 8.
		mk("kvutil", "a", "wide", 1, timed(10)), mk("kvutil", "a", "wide", 8, timed(80)),
		mk("postgres", "a", "wide", 1, timed(40)), mk("postgres", "a", "wide", 8, timed(40)),
		// A phase has records/s; a time is inverted, so that more than 1x is kvutil faster.
		mk("kvutil", "a", "load", 1, phase(5000)), mk("postgres", "a", "load", 1, phase(2500)),
		mk("kvutil", "a", "convert", 1, took(2)), mk("postgres", "a", "convert", 1, took(8)), mk("timescale", "t", "convert", 1, took(4)),
		// Left out: a size, a case of one system, a case without kvutil, and a time that is no result.
		mk("kvutil", "a", "size", 1, size(10)), mk("postgres", "a", "size", 1, size(20)),
		mk("kvutil", "a", "only-kvutil", 4, timed(5)),
		mk("postgres", "a", "no-kvutil", 4, timed(5)), mk("timescale", "t", "no-kvutil", 4, timed(6)),
		mk("kvutil", "a", "cascade-limit", 1, took(300)), mk("postgres", "a", "cascade-limit", 1, took(100)),
	}}
}

func TestSummary(t *testing.T) {
	md := Build([]*Run{summaryRun()}).Markdown()
	i := strings.Index(md, "## Summary")
	j := strings.Index(md, "## Results")
	if i < 0 || j < i || i < strings.Index(md, "## Runs") {
		t.Fatalf("the summary is after the runs and before the results: %d %d", i, j)
	}
	sum := md[i:j]
	mustContain(t, sum,
		"### relations",
		"| case | workers | unit | kvutil | variant | PostgreSQL | variant | TimescaleDB | variant | kvutil / best other |",
		// kvutil b against the best of the others, TimescaleDB's 2,000.
		"| read | 4 | ops/s | 3,000 | b | 1,500 | b | 2,000 | t | 1.50x |",
		"| wide | 8 | ops/s | 80 | a | 40 | a |  |  | 2.00x |",
		"| load | 1 | records/s | 5,000 | a | 2,500 | a |  |  | 2.00x |",
		// Time: 4 s of TimescaleDB against 2 s of kvutil.
		"| convert | 1 | seconds | 2.00 s | a | 8.00 s | a | 4.00 s | t | 2.00x |",
	)
	for _, left := range []string{"size", "only-kvutil", "no-kvutil", "cascade-limit"} {
		if strings.Contains(sum, "| "+left+" |") {
			t.Errorf("the case %s is in the summary", left)
		}
	}
	if strings.Contains(sum, "9,000") {
		t.Error("a level other than 4 was shown for a case that has 4")
	}
	mustContain(t, Build([]*Run{summaryRun()}).HTML(), `<h2 id="summary">Summary</h2>`)

	// Several runs: a summary of each, after the runs and before their comparison.
	md = Build([]*Run{load(t, "mac"), load(t, "linux")}).Markdown()
	a, b, c := strings.Index(md, "## Summary: mac"), strings.Index(md, "## Summary: linux"), strings.Index(md, "## Comparison of runs")
	if a < 0 || b < a || c < b {
		t.Errorf("summaries: %d %d, comparison %d", a, b, c)
	}
}
