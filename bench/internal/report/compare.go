package report

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

// compKey identifies the same measurement in different runs.
type compKey struct {
	workload, system, variant, cs, dur string
	conc                               int
}

// flatParams lists the parameters that a measurement was made with, as text.
func flatParams(p metrics.Params) map[string]string {
	m := map[string]string{
		"scale":        strconv.Itoa(p.Scale),
		"values":       p.Values,
		"durability":   p.Durability,
		"workers":      strconv.Itoa(p.Concurrency),
		"duration_sec": strconv.FormatFloat(p.DurationSec, 'f', -1, 64),
		"warmup_sec":   strconv.FormatFloat(p.WarmupSec, 'f', -1, 64),
		"seed":         strconv.FormatUint(p.Seed, 10),
	}
	for k, v := range p.Case {
		m["case."+k] = str(v)
	}
	return m
}

// paramDiff lists how the parameters of a differ from those of b; none means that they match.
func paramDiff(a, b metrics.Params) []string {
	fa, fb := flatParams(a), flatParams(b)
	keys := map[string]bool{}
	for k := range fa {
		keys[k] = true
	}
	for k := range fb {
		keys[k] = true
	}
	var diffs []string
	for _, k := range sortedKeys(keys) {
		va, oka := fa[k]
		vb, okb := fb[k]
		switch {
		case oka && okb && va != vb:
			diffs = append(diffs, fmt.Sprintf("%s %s / %s", k, va, vb))
		case !oka:
			diffs = append(diffs, fmt.Sprintf("%s only in the other run", k))
		case !okb:
			diffs = append(diffs, fmt.Sprintf("%s only in the first run", k))
		}
	}
	return diffs
}

func throughputOf(x *row) float64 {
	if v, ok := x.throughput(); ok {
		return v
	}
	return math.NaN()
}

// compare adds the section that puts the same case of different runs side by side. The first run
// is the base of the ratios. Only cases whose parameters match are compared; the others are listed.
func (b *builder) compare() {
	doc := b.doc
	doc.Add(&Heading{2, "Comparison of runs"})
	base := b.runs[0]
	doc.Add(&Para{fmt.Sprintf("The ratios are against the first run (%s): above 1x the run's figure is the larger one, which is better for throughput and worse for latency. A case is compared only when the scale, value size, durability, workers, durations, seed and the parameters of the case are the same in both runs; the others are listed after the tables. Server resources differ by machine and are not compared here.", base.run.Name), "note"})

	idx := make([]map[compKey]*row, len(b.runs))
	var order []compKey
	for i, rv := range b.runs {
		idx[i] = map[compKey]*row{}
		for _, x := range rv.rows {
			k := compKey{x.r.Workload, x.r.System, x.r.Variant, x.r.Case, x.dur, x.conc}
			if _, ok := idx[i][k]; ok {
				continue
			}
			idx[i][k] = x
			if i == 0 {
				order = append(order, k)
			}
		}
	}

	// The workloads of all the runs, in the order of docs/workloads.md.
	merged := &runView{}
	for _, rv := range b.runs {
		merged.rows = append(merged.rows, rv.rows...)
	}

	type missKey struct {
		workload string
		run      int
		diff     string
	}
	mism := map[missKey]int{}
	var missOrder []missKey
	onlyBase := map[[2]any]int{}  // workload, run
	onlyOther := map[[2]any]int{} // workload, run
	compared := 0

	for _, wl := range workloadsOf(merged) {
		t := &Table{Name: b.unique("compare", wl)}
		t.Header = []string{"case", "workers", "durability", "system/variant"}
		for _, rv := range b.runs {
			t.Header = append(t.Header, "throughput: "+rv.run.Name)
		}
		for _, rv := range b.runs[1:] {
			t.Header = append(t.Header, "throughput ratio: "+rv.run.Name)
		}
		for _, rv := range b.runs {
			t.Header = append(t.Header, "p99: "+rv.run.Name)
		}
		for _, rv := range b.runs[1:] {
			t.Header = append(t.Header, "p99 ratio: "+rv.run.Name)
		}
		for _, k := range order {
			if k.workload != wl {
				continue
			}
			bx := idx[0][k]
			series := k.system + "/" + k.variant
			matched := make([]*row, len(b.runs))
			matched[0] = bx
			for j := 1; j < len(b.runs); j++ {
				ox := idx[j][k]
				if ox == nil {
					onlyBase[[2]any{wl, j}]++
					continue
				}
				if d := paramDiff(bx.r.Params, ox.r.Params); len(d) > 0 {
					mk := missKey{wl, j, strings.Join(d, "; ")}
					if mism[mk] == 0 {
						missOrder = append(missOrder, mk)
					}
					mism[mk]++
					continue
				}
				matched[j] = ox
			}
			n := 0
			for _, m := range matched[1:] {
				if m != nil {
					n++
				}
			}
			if n == 0 {
				continue
			}
			compared++
			r := cellRow(k.cs, k.conc, k.dur, series)
			for _, m := range matched {
				r = append(r, matchedCell(m, throughputOf, fmtNum))
			}
			for _, m := range matched[1:] {
				r = append(r, ratioCell(m, bx, throughputOf))
			}
			for _, m := range matched {
				r = append(r, matchedCell(m, (*row).p99, fmtUs))
			}
			for _, m := range matched[1:] {
				r = append(r, ratioCell(m, bx, (*row).p99))
			}
			t.Rows = append(t.Rows, r)
		}
		if len(t.Rows) > 0 {
			doc.Add(&Heading{3, "Compared: " + wl}, t)
		}
	}
	if compared == 0 {
		doc.Add(&Para{"No case has the same parameters in the first run and in another, so nothing could be compared.", "warn"})
	}

	// What could not be compared.
	for j := 1; j < len(b.runs); j++ {
		for k, x := range idx[j] {
			if idx[0][k] == nil && x != nil {
				onlyOther[[2]any{k.workload, j}]++
			}
		}
	}
	if len(missOrder) > 0 {
		t := &Table{Name: b.unique("compare", "not-compared"), Header: []string{"workload", "run", "parameters that differ (first run / this run)", "cases"},
			Caption: "Cases that are in both runs but were not compared, because their parameters differ"}
		for _, mk := range missOrder {
			t.Rows = append(t.Rows, cellRow(mk.workload, b.runs[mk.run].run.Name, mk.diff, mism[mk]))
		}
		doc.Add(&Heading{3, "Not compared: different parameters"}, t)
	}
	if len(onlyBase) > 0 || len(onlyOther) > 0 {
		t := &Table{Name: b.unique("compare", "one-run-only"), Header: []string{"workload", "run", "cases only in the first run", "cases only in this run"},
			Caption: "Cases that only one of the runs has (another set of workloads, systems or levels of concurrency)"}
		seen := map[[2]any]bool{}
		var keys [][2]any
		for _, m := range []map[[2]any]int{onlyBase, onlyOther} {
			for k := range m {
				if !seen[k] {
					seen[k] = true
					keys = append(keys, k)
				}
			}
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i][0].(string) != keys[j][0].(string) {
				return keys[i][0].(string) < keys[j][0].(string)
			}
			return keys[i][1].(int) < keys[j][1].(int)
		})
		for _, k := range keys {
			t.Rows = append(t.Rows, cellRow(k[0], b.runs[k[1].(int)].run.Name, onlyBase[k], onlyOther[k]))
		}
		doc.Add(&Heading{3, "Not compared: in one run only"}, t)
	}
}

// matchedCell is the figure of a run, or a dash if the run does not have the case with the same
// parameters.
func matchedCell(x *row, f func(*row) float64, format func(float64) string) Cell {
	if x == nil {
		return T("-")
	}
	return N(f(x), format)
}

func ratioCell(x, base *row, f func(*row) float64) Cell {
	if x == nil {
		return T("-")
	}
	return N(ratio(f(x), f(base)), fmtRatio)
}
