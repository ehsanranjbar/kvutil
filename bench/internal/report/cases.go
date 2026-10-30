package report

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// wctx is the results of one workload of one run in one durability mode.
type wctx struct {
	b        *builder
	rv       *runView
	dur      string
	workload string
	rows     []*row
	order    []string // the systems/variants, in the order of the columns
}

// caseGroup is a case with its results at each level of concurrency.
type caseGroup struct {
	name   string
	concs  []int
	byConc map[int][]*row
}

// build adds what the workload has: its special charts and tables, then the generic tables of
// all of its cases, which every workload gets.
func (w *wctx) build() {
	w.order = seriesOrder(w.rows)
	doc := w.b.doc

	if f := specials[w.workload]; f != nil {
		doc.Add(f(w)...)
	}

	cases := w.cases()
	var inner []Block
	for _, c := range cases {
		inner = append(inner, w.caseBlocks(c)...)
	}
	doc.Add(&Details{Summary: fmt.Sprintf("%s: tables and charts of the %d cases", w.workload, len(cases)), Open: specials[w.workload] == nil, Blocks: inner})
	doc.Add(&Details{Summary: "Resources of the cases (harness and server)", Blocks: []Block{w.resourcesTable()}})
	doc.Data = append(doc.Data, w.dataTable())
}

func seriesOrder(rows []*row) []string {
	seen := map[string]bool{}
	var order []string
	for _, x := range rows {
		if !seen[x.series] {
			seen[x.series] = true
			order = append(order, x.series)
		}
	}
	sys := func(s string) string { s, _, _ = strings.Cut(s, "/"); return s }
	sort.SliceStable(order, func(i, j int) bool { return systemRank(sys(order[i])) < systemRank(sys(order[j])) })
	return order
}

func (w *wctx) rank(series string) int {
	for i, s := range w.order {
		if s == series {
			return i
		}
	}
	return len(w.order)
}

func (w *wctx) cases() []*caseGroup {
	var out []*caseGroup
	byName := map[string]*caseGroup{}
	for _, x := range w.rows {
		c := byName[x.r.Case]
		if c == nil {
			c = &caseGroup{name: x.r.Case, byConc: map[int][]*row{}}
			byName[x.r.Case] = c
			out = append(out, c)
		}
		if _, ok := c.byConc[x.conc]; !ok {
			c.concs = append(c.concs, x.conc)
		}
		c.byConc[x.conc] = append(c.byConc[x.conc], x)
	}
	for _, c := range out {
		sort.Ints(c.concs)
		for _, rows := range c.byConc {
			sort.SliceStable(rows, func(i, j int) bool { return w.rank(rows[i].series) < w.rank(rows[j].series) })
		}
	}
	return out
}

// --- the table of a case: one column per system/variant

// metric is a row of the table of a case. get says whether the metric applies to the result; a
// value that applies but is not available is NaN.
type metric struct {
	label string
	get   func(*row) (float64, bool)
	fmt   func(float64) string
}

func latency(f func(l latencyView) float64) func(*row) (float64, bool) {
	return func(x *row) (float64, bool) {
		l := x.r.Metrics.Latency
		if l == nil {
			return 0, false
		}
		return f(latencyView{l.P50Us, l.P95Us, l.P99Us, l.P999Us, l.MaxUs}), true
	}
}

type latencyView struct{ p50, p95, p99, p999, max float64 }

var caseMetrics = []metric{
	{"ops/s", func(x *row) (float64, bool) {
		return x.r.Metrics.OpsPerSec, x.r.Metrics.Latency != nil
	}, fmtNum},
	{"records/s", func(x *row) (float64, bool) {
		return x.r.Metrics.RecordsPerSec, x.r.Metrics.Latency == nil && x.r.Metrics.Records > 0
	}, fmtNum},
	{"units/s", func(x *row) (float64, bool) {
		return x.r.Metrics.UnitsPerSec, x.r.Metrics.UnitsPerSec > 0
	}, fmtNum},
	{"p50", latency(func(l latencyView) float64 { return l.p50 }), fmtUs},
	{"p95", latency(func(l latencyView) float64 { return l.p95 }), fmtUs},
	{"p99", latency(func(l latencyView) float64 { return l.p99 }), fmtUs},
	{"p99.9", latency(func(l latencyView) float64 { return l.p999 }), fmtUs},
	{"max", latency(func(l latencyView) float64 { return l.max }), fmtUs},
	{"errors", func(x *row) (float64, bool) {
		return float64(x.r.Metrics.Errors), x.r.Metrics.Latency != nil || x.r.Metrics.Errors > 0
	}, fmtNum},
	{"retries", func(x *row) (float64, bool) {
		return float64(x.r.Metrics.Retries), x.r.Metrics.Latency != nil || x.r.Metrics.Retries > 0
	}, fmtNum},
	{"records", func(x *row) (float64, bool) { return float64(x.r.Metrics.Records), x.r.Metrics.Records > 0 }, fmtNum},
	{"seconds", func(x *row) (float64, bool) {
		m := x.r.Metrics
		return m.ElapsedSec, m.Latency == nil && m.ElapsedSec > 0
	}, fmtSec},
	{"size on disk", func(x *row) (float64, bool) { return float64(x.r.Metrics.DiskBytes), x.r.Metrics.DiskBytes > 0 }, fmtBytes},
	{"WAL", func(x *row) (float64, bool) { return float64(x.r.Metrics.WALBytes), x.r.Metrics.WALBytes > 0 }, fmtBytes},
}

// caseTable is the table of a case at a level of concurrency. Rows for the extras and the info
// of the results come from the results, so that a new workload needs nothing here.
func caseTable(c *caseGroup, conc int, rows []*row) *Table {
	t := &Table{Header: []string{"workers: " + strconv.Itoa(conc)}}
	for _, x := range rows {
		t.Header = append(t.Header, x.series)
	}
	t.Caption = caseCaption(c, rows)

	add := func(label string, get func(*row) (float64, bool), f func(float64) string) (vals []float64, any bool) {
		cells := []Cell{T(label)}
		for _, x := range rows {
			v, ok := get(x)
			if !ok {
				cells = append(cells, T(""))
				vals = append(vals, math.NaN())
				continue
			}
			any = true
			cells = append(cells, N(v, f))
			vals = append(vals, v)
		}
		if any {
			t.Rows = append(t.Rows, cells)
		}
		return vals, any
	}

	for _, m := range caseMetrics {
		label := m.label
		if label == "units/s" {
			for _, x := range rows {
				if u := x.r.Metrics.UnitName; u != "" {
					label = u + "/s"
					break
				}
			}
		}
		vals, ok := add(label, m.get, m.fmt)
		if !ok {
			continue
		}
		if m.label == "ops/s" || m.label == "records/s" {
			t.ratioRow(vals)
		}
	}

	for _, k := range extraKeys(rows) {
		k := k
		add(k, func(x *row) (float64, bool) { v, ok := x.r.Metrics.Extra[k]; return v, ok }, formatFor(k))
	}
	for _, k := range infoKeys(rows) {
		cells := []Cell{T(k)}
		for _, x := range rows {
			cells = append(cells, T(x.r.Metrics.Info[k]))
		}
		t.Rows = append(t.Rows, cells)
	}
	return t
}

// ratioRow adds the row of the ratios against the fastest, and makes the fastest bold.
func (t *Table) ratioRow(vals []float64) {
	best := math.NaN()
	for _, v := range vals {
		if !math.IsNaN(v) && (math.IsNaN(best) || v > best) {
			best = v
		}
	}
	if math.IsNaN(best) || best <= 0 || countOK(vals) < 2 {
		return
	}
	cells := []Cell{T("vs the fastest")}
	for i, v := range vals {
		if math.IsNaN(v) {
			cells = append(cells, T(""))
			continue
		}
		cells = append(cells, N(v/best, fmtRatio))
		if v == best {
			last := t.Rows[len(t.Rows)-1]
			last[i+1] = last[i+1].B()
		}
	}
	t.Rows = append(t.Rows, cells)
}

func countOK(vs []float64) int {
	n := 0
	for _, v := range vs {
		if !math.IsNaN(v) {
			n++
		}
	}
	return n
}

func caseCaption(c *caseGroup, rows []*row) string {
	if len(rows) == 0 {
		return ""
	}
	return strings.Trim(paramText(rows[0].r.Params.Case), " ()")
}

func paramText(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, k+"="+str(m[k]))
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

// str shows a value of a JSON file without an exponent.
func str(v any) string {
	if f, ok := v.(float64); ok {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

func extraKeys(rows []*row) []string {
	seen := map[string]bool{}
	var ks []string
	for _, x := range rows {
		for k := range x.r.Metrics.Extra {
			if !seen[k] {
				seen[k] = true
				ks = append(ks, k)
			}
		}
	}
	sort.Strings(ks)
	return ks
}

// infoKeys are the keys of the text of the results that fit a cell: the full plans are in the CSV.
func infoKeys(rows []*row) []string {
	seen := map[string]bool{}
	var ks []string
	for _, x := range rows {
		for k, v := range x.r.Metrics.Info {
			if !seen[k] && len(v) <= 300 && !strings.Contains(v, "\n") {
				seen[k] = true
				ks = append(ks, k)
			}
		}
	}
	sort.Slice(ks, func(i, j int) bool {
		if (ks[i] == "plan_nodes") != (ks[j] == "plan_nodes") {
			return ks[i] == "plan_nodes"
		}
		return ks[i] < ks[j]
	})
	return ks
}

// caseBlocks are the tables of a case, one for each level of concurrency, and its bar charts.
func (w *wctx) caseBlocks(c *caseGroup) []Block {
	out := []Block{&Heading{5, c.name}}
	for _, conc := range c.concs {
		out = append(out, caseTable(c, conc, c.byConc[conc]))
	}
	if ch := w.caseCharts(c); ch != nil {
		out = append(out, ch)
	}
	return out
}

func (w *wctx) caseCharts(c *caseGroup) *Charts {
	timed, phase := false, false
	for _, x := range w.rows {
		if x.r.Case != c.name {
			continue
		}
		timed = timed || x.r.Metrics.Latency != nil
		phase = phase || (x.r.Metrics.Latency == nil && x.r.Metrics.Records > 0)
	}
	type spec struct {
		title, unit string
		get         func(*row) float64
		fmt         func(float64) string
	}
	var specs []spec
	switch {
	case timed:
		specs = []spec{
			{"Throughput", "ops/s, higher is better", func(x *row) float64 { return x.r.Metrics.OpsPerSec }, fmtNum},
			{"p99 latency", "lower is better", func(x *row) float64 { return x.p99() }, fmtUs},
		}
	case phase:
		specs = []spec{
			{"Throughput", "records/s, higher is better", func(x *row) float64 { return x.r.Metrics.RecordsPerSec }, fmtNum},
			{"Time", "lower is better", func(x *row) float64 { return x.r.Metrics.ElapsedSec }, fmtSec},
		}
	default:
		return nil
	}
	out := &Charts{}
	for _, s := range specs {
		var bars []Bar
		for _, conc := range c.concs {
			for _, x := range c.byConc[conc] {
				label := x.series
				if len(c.concs) > 1 {
					label = fmt.Sprintf("c=%d %s", conc, x.series)
				}
				v := s.get(x)
				bars = append(bars, Bar{Label: label, System: x.r.System, Value: v, Text: s.fmt(v)})
			}
		}
		name := w.b.unique(w.rv.prefix, w.dur, w.workload, c.name, s.title)
		out.Items = append(out.Items, BarChart(name, c.name+": "+s.title, s.unit, bars))
	}
	return out
}

// --- resources

func (w *wctx) resourcesTable() *Table {
	t := &Table{Header: []string{"case", "workers", "system/variant", "harness CPU (cores)", "harness peak RSS", "server CPU, mean (cores)", "server memory, peak", "server samples"}}
	for _, c := range w.cases() {
		for _, conc := range c.concs {
			for _, x := range c.byConc[conc] {
				t.Rows = append(t.Rows, row3(c.name, conc, x))
			}
		}
	}
	t.Caption = "Harness: the benchmark process (kvutil runs in it; for PostgreSQL it is the client), from getrusage. Server: docker stats of the database container, the samples that fall in the case; its memory counts the page cache. kvutil has no server of its own."
	return t
}

func row3(name string, conc int, x *row) []Cell {
	cpu, rss := harnessFigures(x)
	if !x.srv.own {
		return cellRow(name, conc, x.series, N(cpu, fmtCores), N(rss, fmtBytes), "embedded", "embedded", "")
	}
	return cellRow(name, conc, x.series, N(cpu, fmtCores), N(rss, fmtBytes), N(x.srv.cpu, fmtCores), N(x.srv.mem, fmtBytes), x.srv.n)
}

// harnessFigures are the CPU and the peak memory of the harness, NaN where they were not recorded.
func harnessFigures(x *row) (cpu, rss float64) {
	r := x.r.Metrics.Resources
	cpu, rss = math.NaN(), math.NaN()
	if r.WallSec > 0 {
		cpu = r.CPUCores
	}
	if r.MaxRSSBytes > 0 {
		rss = float64(r.MaxRSSBytes)
	}
	return cpu, rss
}

// --- the data of a workload, one line for each result

var dataCols = []struct {
	name string
	get  func(*row) float64
}{
	{"ops", func(x *row) float64 { return opt(x.r.Metrics.Ops > 0, float64(x.r.Metrics.Ops)) }},
	{"ops_per_sec", func(x *row) float64 { return opt(x.r.Metrics.Latency != nil, x.r.Metrics.OpsPerSec) }},
	{"units_per_sec", func(x *row) float64 { return opt(x.r.Metrics.UnitsPerSec > 0, x.r.Metrics.UnitsPerSec) }},
	{"errors", func(x *row) float64 { return float64(x.r.Metrics.Errors) }},
	{"retries", func(x *row) float64 { return float64(x.r.Metrics.Retries) }},
	{"elapsed_sec", func(x *row) float64 { return x.r.Metrics.ElapsedSec }},
	{"records", func(x *row) float64 { return opt(x.r.Metrics.Records > 0, float64(x.r.Metrics.Records)) }},
	{"records_per_sec", func(x *row) float64 { return opt(x.r.Metrics.Records > 0, x.r.Metrics.RecordsPerSec) }},
	{"p50_us", func(x *row) float64 { return latOf(x, func(l latencyView) float64 { return l.p50 }) }},
	{"p95_us", func(x *row) float64 { return latOf(x, func(l latencyView) float64 { return l.p95 }) }},
	{"p99_us", func(x *row) float64 { return latOf(x, func(l latencyView) float64 { return l.p99 }) }},
	{"p999_us", func(x *row) float64 { return latOf(x, func(l latencyView) float64 { return l.p999 }) }},
	{"max_us", func(x *row) float64 { return latOf(x, func(l latencyView) float64 { return l.max }) }},
	{"disk_bytes", func(x *row) float64 { return opt(x.r.Metrics.DiskBytes > 0, float64(x.r.Metrics.DiskBytes)) }},
	{"wal_bytes", func(x *row) float64 { return opt(x.r.Metrics.WALBytes > 0, float64(x.r.Metrics.WALBytes)) }},
	{"harness_cpu_cores", func(x *row) float64 { c, _ := harnessFigures(x); return c }},
	{"harness_max_rss_bytes", func(x *row) float64 { _, r := harnessFigures(x); return r }},
	{"harness_cgroup_cpu_cores", func(x *row) float64 {
		return opt(x.r.Metrics.Resources.CgroupCPUCores > 0, x.r.Metrics.Resources.CgroupCPUCores)
	}},
	{"harness_cgroup_mem_peak_bytes", func(x *row) float64 {
		return opt(x.r.Metrics.Resources.CgroupMemPeakBytes > 0, float64(x.r.Metrics.Resources.CgroupMemPeakBytes))
	}},
	{"server_cpu_cores_mean", func(x *row) float64 { return x.srv.cpu }},
	{"server_mem_peak_bytes", func(x *row) float64 { return x.srv.mem }},
	{"server_samples", func(x *row) float64 { return opt(x.srv.own, float64(x.srv.n)) }},
}

// opt is v where it applies and a figure that is not available where it does not.
func opt(applies bool, v float64) float64 {
	if applies {
		return v
	}
	return math.NaN()
}

func latOf(x *row, f func(latencyView) float64) float64 {
	l := x.r.Metrics.Latency
	if l == nil {
		return math.NaN()
	}
	return f(latencyView{l.P50Us, l.P95Us, l.P99Us, l.P999Us, l.MaxUs})
}

// dataTable is every result of the workload as a line of a CSV file, with the parameters of the
// case, the extras and the info of the results as columns.
func (w *wctx) dataTable() *Table {
	t := &Table{Name: w.b.unique(w.rv.prefix, w.dur, w.workload),
		Header: []string{"durability", "case", "workers", "system", "variant", "start", "end", "scale", "values", "seed", "duration_sec", "warmup_sec"}}
	for _, c := range dataCols {
		t.Header = append(t.Header, c.name)
	}

	params, extras, infos := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, x := range w.rows {
		for k := range x.r.Params.Case {
			params[k] = true
		}
		for k := range x.r.Metrics.Extra {
			extras[k] = true
		}
		for k := range x.r.Metrics.Info {
			infos[k] = true
		}
	}
	pk, ek, ik := sortedKeys(params), sortedKeys(extras), sortedKeys(infos)
	for _, k := range pk {
		t.Header = append(t.Header, "param."+k)
	}
	for _, k := range ek {
		t.Header = append(t.Header, "extra."+k)
	}
	for _, k := range ik {
		t.Header = append(t.Header, "info."+k)
	}

	for _, x := range w.rows {
		r := x.r
		cells := cellRow(x.dur, r.Case, x.conc, r.System, r.Variant, r.Start.Format("2006-01-02T15:04:05.000Z07:00"), r.End.Format("2006-01-02T15:04:05.000Z07:00"),
			r.Params.Scale, r.Params.Values, r.Params.Seed, N(r.Params.DurationSec, fmtNum), N(r.Params.WarmupSec, fmtNum))
		for _, c := range dataCols {
			cells = append(cells, N(c.get(x), fmtNum))
		}
		for _, k := range pk {
			if v, ok := r.Params.Case[k]; ok {
				cells = append(cells, T(str(v)))
			} else {
				cells = append(cells, T(""))
			}
		}
		for _, k := range ek {
			if v, ok := r.Metrics.Extra[k]; ok {
				cells = append(cells, N(v, fmtNum))
			} else {
				cells = append(cells, N(math.NaN(), fmtNum))
			}
		}
		for _, k := range ik {
			cells = append(cells, T(r.Metrics.Info[k]))
		}
		t.Rows = append(t.Rows, cells)
	}
	return t
}

func sortedKeys(m map[string]bool) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
