package report

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

// workloadOrder is the order of the workloads in bench/docs/workloads.md. A workload that is not here
// comes after them, in the order that it appears in the results.
var workloadOrder = []string{
	"kv-basic", "bulk-load", "record-codecs", "index-maintenance",
	"queries", "selectivity-sweep", "key-order-fetch",
	"agg-write", "agg-hot-group", "agg-read", "agg-freshness", "agg-rebuild",
	"relations", "lifecycle", "concurrency", "storage",
}

// statsLag is how long after its time stamp a sample of docker stats describes: the stamp is taken
// before the command, which takes a second to measure.
const statsLag = time.Second

// row is a result with what the report derives from it.
type row struct {
	r      *metrics.Result
	series string // system/variant
	dur    string
	conc   int
	srv    server
}

// server is what docker stats saw of the container of the database during a case. Embedded
// systems (kvutil) have none: they are in the harness.
type server struct {
	own bool
	cpu float64 // mean, in cores
	mem float64 // peak, with the page cache that the cgroup counts
	n   int     // samples
}

func (x *row) throughput() (float64, bool) {
	m := &x.r.Metrics
	switch {
	case m.Latency != nil:
		return m.OpsPerSec, true
	case m.Records > 0:
		return m.RecordsPerSec, true
	}
	return 0, false
}

func (x *row) p99() float64 {
	if l := x.r.Metrics.Latency; l != nil {
		return l.P99Us
	}
	return math.NaN()
}

// runView is a run with its rows.
type runView struct {
	run    *Run
	rows   []*row
	prefix string
}

func systemRank(system string) int {
	for i, c := range systemColors {
		if c.name == system {
			return i
		}
	}
	return len(systemColors)
}

func container(run *Run, system string) string {
	switch system {
	case "postgres":
		if run.Meta.Args.PGService == "timescaledb" {
			return "kvbench-timescaledb"
		}
		return "kvbench-postgres"
	case "timescale":
		return "kvbench-timescaledb"
	}
	return ""
}

func newRunView(run *Run, prefix string) *runView {
	rv := &runView{run: run, prefix: prefix}
	for _, r := range run.Results {
		x := &row{r: r, series: r.System + "/" + r.Variant, dur: r.Params.Durability, conc: r.Params.Concurrency}
		if r.Variant == "" {
			x.series = r.System
		}
		x.srv = joinStats(run, r)
		rv.rows = append(rv.rows, x)
	}
	return rv
}

// joinStats selects the samples of docker stats of the container of the system that fall in the
// interval of the result.
func joinStats(run *Run, r *metrics.Result) server {
	name := container(run, r.System)
	s := server{own: name != "", cpu: math.NaN(), mem: math.NaN()}
	if name == "" {
		return s
	}
	var cpu float64
	for _, smp := range run.Stats {
		at := smp.At.Add(statsLag)
		if smp.Name != name || at.Before(r.Start) || at.After(r.End) {
			continue
		}
		if !math.IsNaN(smp.CPUCores) {
			cpu += smp.CPUCores
			s.n++
		}
		if !math.IsNaN(smp.MemBytes) && (math.IsNaN(s.mem) || smp.MemBytes > s.mem) {
			s.mem = smp.MemBytes
		}
	}
	if s.n > 0 {
		s.cpu = cpu / float64(s.n)
	}
	return s
}

// Build makes the report of the runs.
func Build(runs []*Run) *Doc {
	b := &builder{used: map[string]int{}}
	for i, run := range runs {
		prefix := slug(run.Name)
		if len(runs) > 1 {
			prefix = fmt.Sprintf("r%d-%s", i+1, prefix)
		}
		b.runs = append(b.runs, newRunView(run, prefix))
	}

	doc := &Doc{Title: "kvutil benchmark report"}
	b.doc = doc
	b.header()
	for _, rv := range b.runs {
		b.summary(rv)
	}
	if len(b.runs) > 1 {
		b.compare()
	}
	for _, rv := range b.runs {
		title := "Results"
		if len(b.runs) > 1 {
			title += ": " + rv.run.Name
		}
		doc.Add(&Heading{2, title})
		b.results(rv)
	}
	return doc
}

type builder struct {
	runs []*runView
	doc  *Doc
	used map[string]int
}

// unique makes a name that no chart or CSV file has yet.
func (b *builder) unique(parts ...string) string {
	var ps []string
	for _, p := range parts {
		ps = append(ps, slug(p))
	}
	name := strings.Join(ps, "-")
	if len(name) > 90 {
		name = name[:90]
	}
	b.used[name]++
	if n := b.used[name]; n > 1 {
		return fmt.Sprintf("%s-%d", name, n)
	}
	return name
}

// --- the header: the runs, and what to be careful of

func (b *builder) header() {
	doc := b.doc
	var warns []string
	for _, rv := range b.runs {
		for _, w := range runWarnings(rv) {
			if len(b.runs) > 1 {
				w = rv.run.Name + ": " + w
			}
			warns = append(warns, w)
		}
	}
	doc.Add(&Heading{2, "Runs"})
	for _, w := range warns {
		doc.Add(&Para{w, "warn"})
	}
	if len(warns) == 0 {
		doc.Add(&Para{"No warnings: verification passed, no case had errors, and the scale is not a smoke run's.", "note"})
	}

	t := &Table{Name: "runs", Header: []string{""}}
	for _, rv := range b.runs {
		t.Header = append(t.Header, rv.run.Name)
	}
	for _, f := range runFields {
		r := []Cell{T(f.label)}
		for _, rv := range b.runs {
			r = append(r, T(f.get(rv)))
		}
		t.Rows = append(t.Rows, r)
	}
	doc.Add(t)
	doc.Add(&Para{"Harness figures (CPU, memory of the benchmark process, which holds kvutil) and server figures (docker stats of the database container) are separate columns wherever they are shown; kvutil has no server of its own. A figure shown as n/a was not recorded; it is not zero.", "note"})
}

type field struct {
	label string
	get   func(*runView) string
}

func first(rv *runView) *metrics.Result { return rv.run.Results[0] }

// versionOf finds the version string of a system in the results.
func versionOf(rv *runView, system string) string {
	for _, r := range rv.run.Results {
		if r.System == system {
			return r.SystemVersion
		}
	}
	return "-"
}

func shortPG(v string) string {
	if v == "-" {
		return v
	}
	pg, _, _ := strings.Cut(v, " on ")
	if _, ts, ok := strings.Cut(v, "timescaledb "); ok {
		pg += ", timescaledb " + strings.TrimSpace(ts)
	}
	return pg
}

func distinct(rv *runView, f func(*metrics.Result) string) string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rv.run.Results {
		if s := f(r); s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return "-"
	}
	return strings.Join(out, ", ")
}

func secs(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) + " s" }

var runFields = []field{
	{"host", func(rv *runView) string { return first(rv).Host.Label }},
	{"started", func(rv *runView) string {
		if rv.run.HasMeta && rv.run.Meta.Started != "" {
			return rv.run.Meta.Started
		}
		return first(rv).Start.UTC().Format(time.RFC3339)
	}},
	{"harness container", func(rv *runView) string {
		h := first(rv).Host
		return fmt.Sprintf("%s/%s %s, go %s", h.OS, h.Arch, h.Kernel, strings.TrimPrefix(h.GoVersion, "go"))
	}},
	{"docker", func(rv *runView) string {
		m := rv.run.Meta.Docker
		if !rv.run.HasMeta || m.Server == "" {
			return "-"
		}
		return fmt.Sprintf("%s %s, %.0f CPUs, %s", m.OS, m.Server, m.NCPU, fmtBytes(m.MemTotal))
	}},
	{"CPUs of the machine (as the harness sees them)", func(rv *runView) string { return strconv.Itoa(first(rv).Host.NumCPU) }},
	{"limits of the harness container", func(rv *runView) string {
		h := first(rv).Host
		if h.CPULimit == 0 && h.MemLimitBytes == 0 {
			return "none read"
		}
		return fmt.Sprintf("%s CPUs, %s memory", trim(h.CPULimit), fmtBytes(float64(h.MemLimitBytes)))
	}},
	{"limits of each container (run.sh)", func(rv *runView) string {
		l := rv.run.Meta.Limits
		if len(l) == 0 {
			return "-"
		}
		return fmt.Sprintf("%s CPUs, %s memory; shared_buffers %s", l["cpus"], l["mem"], l["shared_buffers"])
	}},
	{"kvutil", func(rv *runView) string {
		v := versionOf(rv, "kvutil")
		if c := rv.run.Meta.KvutilCommit; rv.run.HasMeta && c != "" && !strings.Contains(v, c) {
			v += "; commit " + rv.run.Meta.KvutilCommit
		}
		return v
	}},
	{"PostgreSQL", func(rv *runView) string { return shortPG(versionOf(rv, "postgres")) }},
	{"TimescaleDB", func(rv *runView) string { return shortPG(versionOf(rv, "timescale")) }},
	{"images", func(rv *runView) string {
		if len(rv.run.Images) == 0 {
			return "-"
		}
		return strings.Join(rv.run.Images, "; ")
	}},
	{"scale", func(rv *runView) string {
		return distinct(rv, func(r *metrics.Result) string { return strconv.Itoa(r.Params.Scale) })
	}},
	{"values", func(rv *runView) string {
		return distinct(rv, func(r *metrics.Result) string { return r.Params.Values })
	}},
	{"durability", func(rv *runView) string {
		return distinct(rv, func(r *metrics.Result) string { return r.Params.Durability })
	}},
	{"workers", func(rv *runView) string {
		return distinct(rv, func(r *metrics.Result) string { return strconv.Itoa(r.Params.Concurrency) })
	}},
	{"duration, warm-up", func(rv *runView) string {
		return distinct(rv, func(r *metrics.Result) string {
			if r.Params.DurationSec == 0 {
				return ""
			}
			return secs(r.Params.DurationSec) + ", " + secs(r.Params.WarmupSec)
		})
	}},
	{"systems", func(rv *runView) string { return distinct(rv, func(r *metrics.Result) string { return r.System }) }},
	{"workloads", func(rv *runView) string { return strings.Join(workloadsOf(rv), ", ") }},
	{"results", func(rv *runView) string {
		if n := rv.run.Replaced; n > 0 {
			return fmt.Sprintf("%d (%d earlier results of cases that were run again are left out)", len(rv.run.Results), n)
		}
		return strconv.Itoa(len(rv.run.Results))
	}},
	{"verify", func(rv *runView) string {
		switch v := rv.run.Verify; {
		case !v.Present:
			return "not run"
		case v.Passed:
			return "passed"
		}
		return "FAILED"
	}},
}

func isSmoke(r *metrics.Result) bool {
	return r.Params.Scale <= 10000 || (r.Params.DurationSec > 0 && r.Params.DurationSec < 3)
}

func runWarnings(rv *runView) []string {
	var ws []string
	run := rv.run
	if run.Verify.Present && !run.Verify.Passed {
		ws = append(ws, "kvbench verify FAILED: the systems did not return the same answers, so the numbers compare different work (see verify.txt).")
	}
	errs, ops, shown, seenErr := 0, int64(0), []string{}, map[string]bool{}
	for _, x := range rv.rows {
		if e := x.r.Metrics.Errors; e > 0 {
			errs++
			ops += e
			s := fmt.Sprintf("%s %s %s", x.r.Workload, x.series, x.r.Case)
			if len(x.r.Metrics.ErrorSample) > 0 {
				s += " (" + x.r.Metrics.ErrorSample[0] + ")"
			}
			if len(shown) < 5 && !seenErr[s] {
				seenErr[s] = true
				shown = append(shown, s)
			}
		}
	}
	if errs > 0 {
		ws = append(ws, fmt.Sprintf("%d results have errors (%d failed operations in all), for example %s. Latencies leave failed operations out.", errs, ops, strings.Join(shown, "; ")))
	}
	if strings.Contains(run.Meta.Docker.OS, "Docker Desktop") || strings.Contains(first(rv).Host.Kernel, "linuxkit") || strings.Contains(run.Meta.Docker.Kernel, "linuxkit") {
		ws = append(ws, "Docker Desktop (macOS): the containers run in a VM, and fsync reaches the disk image of the VM, not the disk of the machine. Durable-mode numbers are not comparable with those of a Linux host.")
	}
	longest := 0.0
	for _, x := range rv.rows {
		longest = math.Max(longest, x.r.Params.DurationSec)
	}
	if isSmoke(first(rv)) || (longest > 0 && longest < 3) {
		ws = append(ws, fmt.Sprintf("smoke scale (scale %d, %s per timed case): the numbers show that the suite works, not how the systems compare.", first(rv).Params.Scale, secs(longest)))
	}
	if strings.HasSuffix(run.Meta.KvutilCommit, "-dirty") {
		ws = append(ws, "kvutil was benchmarked with uncommitted changes (commit "+run.Meta.KvutilCommit+").")
	}
	if run.Skipped > 0 {
		ws = append(ws, fmt.Sprintf("%d lines of the results files could not be read and were skipped.", run.Skipped))
	}
	return ws
}

// workloadsOf lists the workloads of a run in the order of docs/workloads.md.
func workloadsOf(rv *runView) []string {
	seen := map[string]bool{}
	var names []string
	for _, x := range rv.rows {
		if !seen[x.r.Workload] {
			seen[x.r.Workload] = true
			names = append(names, x.r.Workload)
		}
	}
	rank := func(n string) int {
		for i, w := range workloadOrder {
			if w == n {
				return i
			}
		}
		return len(workloadOrder)
	}
	sort.SliceStable(names, func(i, j int) bool { return rank(names[i]) < rank(names[j]) })
	return names
}

func durabilitiesOf(rv *runView) []string {
	seen := map[string]bool{}
	var ds []string
	for _, x := range rv.rows {
		if !seen[x.dur] {
			seen[x.dur] = true
			ds = append(ds, x.dur)
		}
	}
	return ds
}

// rowsOf are the rows of a workload of a run in one durability mode.
func rowsOf(rv *runView, workload, dur string) []*row {
	var rows []*row
	for _, x := range rv.rows {
		if x.r.Workload == workload && x.dur == dur {
			rows = append(rows, x)
		}
	}
	return rows
}

// results adds the sections of the workloads of a run.
func (b *builder) results(rv *runView) {
	durs := durabilitiesOf(rv)
	for _, name := range workloadsOf(rv) {
		b.doc.Add(&Heading{3, name})
		for _, d := range durs {
			rows := rowsOf(rv, name, d)
			if len(rows) == 0 {
				continue
			}
			w := &wctx{b: b, rv: rv, dur: d, workload: name, rows: rows}
			if len(durs) > 1 {
				b.doc.Add(&Heading{4, name + ", durability " + d})
			}
			w.build()
		}
	}
}
