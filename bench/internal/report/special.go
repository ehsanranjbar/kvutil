package report

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// specials are the workloads that have charts of their own beside the generic tables. Everything
// else in the report is generic.
var specials = map[string]func(*wctx) []Block{
	"selectivity-sweep": sweepBlocks,
	"key-order-fetch":   keyFetchBlocks,
	"agg-freshness":     func(w *wctx) []Block { return append(freshnessBlocks(w), pendingBlocks(w)...) },
	"agg-read":          pendingBlocks,
	"concurrency":       concurrencyBlocks,
	"storage":           storageBlocks,
}

// pnum and pstr read a parameter of the case of a result.
func (x *row) pnum(key string) (float64, bool) {
	switch v := x.r.Params.Case[key].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	}
	return 0, false
}

func (x *row) pstr(key string) string {
	if v, ok := x.r.Params.Case[key]; ok {
		return str(v)
	}
	return ""
}

func (x *row) extra(key string) float64 {
	if v, ok := x.r.Metrics.Extra[key]; ok {
		return v
	}
	return math.NaN()
}

// buildLines makes the lines of the rows, one for each label, with their points sorted by x.
// Rows whose x is not known are left out. The lines of a system differ by style.
func buildLines(rows []*row, label func(*row) string, x func(*row) (float64, bool), y func(*row) float64) []Line {
	byLabel := map[string]*Line{}
	styles := map[string]int{}
	var lines []*Line
	for _, r := range rows {
		xv, ok := x(r)
		if !ok {
			continue
		}
		l := byLabel[label(r)]
		if l == nil {
			l = &Line{Label: label(r), System: r.r.System, Style: styles[r.r.System]}
			styles[r.r.System]++
			byLabel[l.Label] = l
			lines = append(lines, l)
		}
		l.X = append(l.X, xv)
		l.Y = append(l.Y, y(r))
	}
	out := make([]Line, 0, len(lines))
	for _, l := range lines {
		idx := make([]int, len(l.X))
		for i := range idx {
			idx[i] = i
		}
		sort.SliceStable(idx, func(a, b int) bool { return l.X[idx[a]] < l.X[idx[b]] })
		sorted := Line{Label: l.Label, System: l.System, Style: l.Style}
		for _, i := range idx {
			sorted.X, sorted.Y = append(sorted.X, l.X[i]), append(sorted.Y, l.Y[i])
		}
		out = append(out, sorted)
	}
	return out
}

// categorical turns the x values of lines into positions, and names the positions.
func categorical(lines []Line, name func(float64) string) ([]Line, []string) {
	seen := map[float64]bool{}
	var vals []float64
	for _, l := range lines {
		for _, x := range l.X {
			if !seen[x] {
				seen[x] = true
				vals = append(vals, x)
			}
		}
	}
	sort.Float64s(vals)
	pos := map[float64]float64{}
	ticks := make([]string, len(vals))
	for i, v := range vals {
		pos[v] = float64(i)
		ticks[i] = name(v)
	}
	out := make([]Line, len(lines))
	for i, l := range lines {
		out[i] = l
		out[i].X = make([]float64, len(l.X))
		for j, x := range l.X {
			out[i].X[j] = pos[x]
		}
	}
	return out, ticks
}

func (w *wctx) chartName(parts ...string) string {
	return w.b.unique(append([]string{w.rv.prefix, w.dur, w.workload}, parts...)...)
}

func workersText(n int) string {
	if n == 1 {
		return "1 worker"
	}
	return fmt.Sprintf("%d workers", n)
}

func ops(x *row) float64 { return x.r.Metrics.OpsPerSec }

// --- selectivity-sweep

func sweepBlocks(w *wctx) []Block {
	type key struct {
		order string
		conc  int
	}
	groups := map[key][]*row{}
	var keys []key
	maxConc := 0
	for _, x := range w.rows {
		if _, ok := x.pnum("selectivity_pct"); !ok || x.r.Metrics.Latency == nil {
			continue
		}
		k := key{x.pstr("order"), x.conc}
		if groups[k] == nil {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], x)
		maxConc = max(maxConc, x.conc)
	}
	if len(keys) == 0 {
		return nil
	}
	selX := func(x *row) (float64, bool) { return x.pnum("selectivity_pct") }
	label := func(x *row) string { return x.series }

	out := []Block{&Heading{4, "Throughput against selectivity"}}
	for _, k := range keys {
		title := fmt.Sprintf("order %s, %s", k.order, workersText(k.conc))
		rows := groups[k]
		ch := &Charts{}
		ch.Items = append(ch.Items, LineChart(w.chartName("sel-ops", k.order, strconv.Itoa(k.conc)),
			buildLines(rows, label, selX, ops),
			LineOpts{Title: title + ": queries/s", XLabel: "selectivity (% of the table)", YLabel: "queries/s", LogX: true, LogY: true}))
		ch.Items = append(ch.Items, LineChart(w.chartName("sel-units", k.order, strconv.Itoa(k.conc)),
			buildLines(rows, label, selX, func(x *row) float64 { return opt(x.r.Metrics.UnitsPerSec > 0, x.r.Metrics.UnitsPerSec) }),
			LineOpts{Title: title + ": rows/s", XLabel: "selectivity (% of the table)", YLabel: "rows/s", LogX: true, LogY: true}))
		out = append(out, ch)
	}

	// The plan of each system at each step, at the highest level of concurrency.
	t := &Table{Name: w.chartName("plans"), Header: []string{"order", "selectivity %", "target rows"}, Caption: "The plan that each system chose at each step (info.plan_nodes), " + workersText(maxConc)}
	t.Header = append(t.Header, w.order...)
	type step struct {
		order string
		sel   float64
	}
	cells := map[step]map[string]string{}
	var steps []step
	target := map[step]string{}
	for _, x := range w.rows {
		sel, ok := x.pnum("selectivity_pct")
		if !ok || x.conc != maxConc {
			continue
		}
		s := step{x.pstr("order"), sel}
		if cells[s] == nil {
			cells[s] = map[string]string{}
			steps = append(steps, s)
		}
		cells[s][x.series] = x.r.Metrics.Info["plan_nodes"]
		target[s] = x.pstr("target_rows")
	}
	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].order != steps[j].order {
			return steps[i].order < steps[j].order
		}
		return steps[i].sel < steps[j].sel
	})
	for _, s := range steps {
		r := cellRow(s.order, N(s.sel, fmtNum), target[s])
		for _, ser := range w.order {
			r = append(r, T(cells[s][ser]))
		}
		t.Rows = append(t.Rows, r)
	}
	return append(out, &Heading{4, "Plan per step"}, t)
}

// --- key-order-fetch

func keyFetchBlocks(w *wctx) []Block {
	byCache := map[string][]*row{}
	var caches []string
	for _, x := range w.rows {
		if _, ok := x.pnum("k"); !ok {
			continue
		}
		c := x.pstr("cache")
		if byCache[c] == nil {
			caches = append(caches, c)
		}
		byCache[c] = append(byCache[c], x)
	}
	if len(caches) == 0 {
		return nil
	}
	out := []Block{&Heading{4, "Fetching K records by key: ns per record"}}
	ch := &Charts{}
	for _, c := range caches {
		lines := buildLines(byCache[c],
			func(x *row) string { return x.series + " " + x.pstr("way") },
			func(x *row) (float64, bool) { return x.pnum("k") },
			func(x *row) float64 { return x.extra("ns_per_record_median") })
		ch.Items = append(ch.Items, LineChart(w.chartName("ns-per-record", c), lines,
			LineOpts{Title: "Median ns per record, " + c + " cache", XLabel: "K, records fetched", YLabel: "ns per record", LogX: true, YFmt: fmtNs}))
	}
	return append(out, ch)
}

// --- agg-freshness

func freshnessBlocks(w *wctx) []Block {
	t := &Table{Name: w.chartName("lag"), Caption: "Lag from the write of an event until a read shows it: of the daily aggregate and of the totals. Trials that timed out are counted apart."}
	t.Header = []string{"case", "system/variant", "trials", "timeouts"}
	stats := []string{"min", "mean", "p50", "p95", "max"}
	for _, kind := range []string{"daily", "all"} {
		for _, s := range stats {
			t.Header = append(t.Header, kind+" "+s)
		}
	}
	for _, x := range w.rows {
		if _, ok := x.r.Metrics.Extra["lag_daily_ms_mean"]; !ok {
			continue
		}
		r := cellRow(x.r.Case, x.series, N(x.extra("trials"), fmtNum), N(x.extra("timeouts"), fmtNum))
		for _, kind := range []string{"daily", "all"} {
			for _, s := range stats {
				r = append(r, N(x.extra("lag_"+kind+"_ms_"+s), fmtMs))
			}
		}
		t.Rows = append(t.Rows, r)
	}
	if len(t.Rows) == 0 {
		return nil
	}
	return []Block{&Heading{4, "Lag distribution"}, t}
}

// --- agg-read: what pending events cost a read

func pendingBlocks(w *wctx) []Block {
	type key struct {
		kind string
		conc int
	}
	groups := map[key][]*row{}
	var keys []key
	for _, x := range w.rows {
		kind, _, ok := strings.Cut(x.r.Case, "-pending-")
		if _, has := x.pnum("pending"); !ok || !has || x.r.Metrics.Latency == nil {
			continue
		}
		k := key{kind, x.conc}
		if groups[k] == nil {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], x)
	}
	if len(keys) == 0 {
		return nil
	}
	out := []Block{&Heading{4, "Cost of pending events on a read"}}
	for _, k := range keys {
		rows := groups[k]
		pending := func(x *row) (float64, bool) { return x.pnum("pending") }
		label := func(x *row) string { return x.series }
		ch := &Charts{}
		for _, m := range []struct {
			name, title string
			y           func(*row) float64
			f           func(float64) string
		}{
			{"p50", "median latency of a read", func(x *row) float64 { return latOf(x, func(l latencyView) float64 { return l.p50 }) }, fmtUs},
			{"ops", "reads per second", ops, fmtSI},
		} {
			lines, ticks := categorical(buildLines(rows, label, pending, m.y), fmtSI)
			ch.Items = append(ch.Items, LineChart(w.chartName("pending", k.kind, strconv.Itoa(k.conc), m.name), lines,
				LineOpts{Title: k.kind + ": " + m.title, XLabel: "events pending", XTicks: ticks, YFmt: m.f}))
		}
		out = append(out, ch)
	}
	return out
}

// --- concurrency

func concurrencyBlocks(w *wctx) []Block {
	out := []Block{&Heading{4, "Throughput and retries against workers"}}
	for _, c := range w.cases() {
		if len(c.concs) < 2 {
			continue
		}
		var rows []*row
		retries := false
		for _, conc := range c.concs {
			for _, x := range c.byConc[conc] {
				if x.r.Metrics.Latency != nil {
					rows = append(rows, x)
					retries = retries || x.r.Metrics.Retries > 0
				}
			}
		}
		if len(rows) == 0 {
			continue
		}
		workers := func(x *row) (float64, bool) { return float64(x.conc), true }
		label := func(x *row) string { return x.series }
		ch := &Charts{}
		lines, ticks := categorical(buildLines(rows, label, workers, ops), fmtSI)
		ch.Items = append(ch.Items, LineChart(w.chartName("workers", c.name, "ops"), lines,
			LineOpts{Title: c.name + ": operations per second", XLabel: "workers", XTicks: ticks}))
		if retries {
			lines, ticks := categorical(buildLines(rows, label, workers, func(x *row) float64 { return float64(x.r.Metrics.Retries) }), fmtSI)
			ch.Items = append(ch.Items, LineChart(w.chartName("workers", c.name, "retries"), lines,
				LineOpts{Title: c.name + ": retries of conflicting transactions", XLabel: "workers", XTicks: ticks}))
		}
		out = append(out, ch)
	}
	if len(out) == 1 {
		return nil
	}
	return out
}

// --- storage

// sizePairs are the extras that hold a size before and after something.
var sizePairs = [][2]string{
	{"disk_bytes_before", "disk_bytes_after"},
	{"disk_bytes_at_the_end_of_the_writes", "disk_bytes_after_expiry"},
}

func storageBlocks(w *wctx) []Block {
	var out []Block

	// Bytes after each step, a line for each system/variant.
	families := map[string][]*row{}
	var famOrder []string
	for _, x := range w.rows {
		if x.r.Metrics.DiskBytes <= 0 || strings.HasSuffix(x.r.Case, "-io") {
			continue
		}
		f, _, _ := strings.Cut(x.r.Variant, "-")
		if families[f] == nil {
			famOrder = append(famOrder, f)
		}
		families[f] = append(families[f], x)
	}
	if len(famOrder) > 0 {
		out = append(out, &Heading{4, "Bytes on disk after each step"})
	}
	for _, f := range famOrder {
		rows := families[f]
		var steps []string
		pos := map[string]float64{}
		for _, x := range rows {
			if _, ok := pos[x.r.Case]; !ok {
				pos[x.r.Case] = float64(len(steps))
				steps = append(steps, x.r.Case)
			}
		}
		lines := buildLines(rows, func(x *row) string { return x.series },
			func(x *row) (float64, bool) { return pos[x.r.Case], true },
			func(x *row) float64 { return float64(x.r.Metrics.DiskBytes) })
		out = append(out, &Charts{Items: []*Chart{LineChart(w.chartName("bytes", f), lines,
			LineOpts{Title: f + ": bytes on disk after each step", XTicks: steps, YFmt: fmtBytes, YBytes: true})}})

		t := &Table{Name: w.chartName("bytes", f, "table"), Caption: f + ": size on disk after each step (the WAL of PostgreSQL apart)"}
		t.Header = []string{"step"}
		for _, s := range w.order {
			if hasSeries(rows, s) {
				t.Header = append(t.Header, s+" disk")
				if hasWAL(rows, s) {
					t.Header = append(t.Header, s+" WAL")
				}
			}
		}
		for _, step := range steps {
			r := cellRow(step)
			for _, s := range w.order {
				if !hasSeries(rows, s) {
					continue
				}
				var found *row
				for _, x := range rows {
					if x.series == s && x.r.Case == step {
						found = x
					}
				}
				wal := hasWAL(rows, s)
				if found == nil {
					r = append(r, T(""))
					if wal {
						r = append(r, T(""))
					}
					continue
				}
				r = append(r, N(float64(found.r.Metrics.DiskBytes), fmtBytes))
				if wal {
					r = append(r, N(opt(found.r.Metrics.WALBytes > 0, float64(found.r.Metrics.WALBytes)), fmtBytes))
				}
			}
			t.Rows = append(t.Rows, r)
		}
		out = append(out, t)
	}

	// Write amplification.
	wa := &Table{Name: w.chartName("write-amplification"), Caption: "Bytes the container wrote to its disk for the bytes the user wrote (write_amp), where the harness could read them",
		Header: []string{"case", "workers", "system/variant", "user bytes", "container bytes written", "write amplification"}}
	for _, x := range w.rows {
		if _, ok := x.r.Metrics.Extra["write_amp"]; !ok {
			continue
		}
		wa.Rows = append(wa.Rows, cellRow(x.r.Case, x.conc, x.series, N(x.extra("user_bytes"), fmtBytes), N(x.extra("container_write_bytes"), fmtBytes), N(x.extra("write_amp"), fmtRatio)))
	}
	if len(wa.Rows) > 0 {
		out = append(out, &Heading{4, "Write amplification"}, wa)
	}

	// Steps that give space back, before and after.
	rc := &Table{Name: w.chartName("size-change"), Caption: "Size on disk before and after a step. The steps that reclaim space (VACUUM, Flatten, value log GC, expiry) are among them.",
		Header: []string{"case", "system/variant", "before", "after", "change", "time"}}
	for _, x := range w.rows {
		for _, p := range sizePairs {
			before, after := x.extra(p[0]), x.extra(p[1])
			if _, ok := x.r.Metrics.Extra[p[0]]; !ok {
				continue
			}
			rc.Rows = append(rc.Rows, cellRow(x.r.Case, x.series, N(before, fmtBytes), N(after, fmtBytes),
				N((ratio(after, before)-1)*100, fmtPct), N(opt(x.r.Metrics.ElapsedSec > 0, x.r.Metrics.ElapsedSec), fmtSec)))
		}
	}
	if len(rc.Rows) > 0 {
		out = append(out, &Heading{4, "Space before and after each step"}, rc)
	}
	return out
}

// hasWAL is true for the systems that keep a write-ahead log apart from their data.
func hasWAL(rows []*row, series string) bool {
	for _, x := range rows {
		if x.series == series && x.r.Metrics.WALBytes > 0 {
			return true
		}
	}
	return false
}

func hasSeries(rows []*row, series string) bool {
	for _, x := range rows {
		if x.series == series {
			return true
		}
	}
	return false
}
