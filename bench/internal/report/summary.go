package report

import (
	"fmt"
	"math"
	"strings"
)

// summaryWorkers is the level of concurrency that the summary shows. A case that has no such level
// shows the largest one it has.
const summaryWorkers = 4

// summaryMetrics are the metrics of caseMetrics that the summary compares, in the order of
// preference: the rate of the operations of a timed case, the rate of a phase, and the time of a
// phase that has no records. A case that none of them applies to (the size of the data) has no
// summary. Of the first two higher is better, of the last lower.
var summaryMetrics = []string{"ops/s", "records/s", "seconds"}

// notSummarised are the cases whose time is no result: a fixed interval, or a ladder of steps
// that runs until one fails.
var notSummarised = map[[2]string]bool{
	{"concurrency", "reader"}:      true,
	{"relations", "cascade-limit"}: true,
}

// systemNames are the names that the summary gives the systems.
var systemNames = map[string]string{"kvutil": "kvutil", "postgres": "PostgreSQL", "timescale": "TimescaleDB"}

func systemName(system string) string {
	if n, ok := systemNames[system]; ok {
		return n
	}
	return system
}

// best is the value of a system in a case, and the variant that made it.
type best struct {
	val     float64
	variant string
}

// summary adds the section that shows, for each workload and each case that kvutil and another
// system both have, the best figure of each system and how kvutil compares with the best of the
// others. It is a short way into the tables of the workloads, which hold everything.
func (b *builder) summary(rv *runView) {
	title := "Summary"
	if len(b.runs) > 1 {
		title += ": " + rv.run.Name
	}
	b.doc.Add(&Heading{2, title})
	b.doc.Add(&Para{fmt.Sprintf("One line for each case that kvutil and another system both ran, at %d workers (the largest number of workers the case has, if it has no such level). Each system shows its best variant, with the variant beside the figure. The ratio is kvutil against the best of the others, and is above 1x where kvutil is faster: for a rate it is kvutil's over theirs, for a time theirs over kvutil's. Cases that are a size, not a rate or a time, are left out; the tables of the workloads below have everything.", summaryWorkers), "note"})

	durs := durabilitiesOf(rv)
	shown := 0
	for _, name := range workloadsOf(rv) {
		for _, d := range durs {
			rows := rowsOf(rv, name, d)
			if len(rows) == 0 {
				continue
			}
			w := &wctx{b: b, rv: rv, dur: d, workload: name, rows: rows, order: seriesOrder(rows)}
			t := w.summaryTable()
			if t == nil {
				continue
			}
			shown++
			if len(durs) > 1 {
				b.doc.Add(&Heading{3, name + ", durability " + d})
			} else {
				b.doc.Add(&Heading{3, name})
			}
			b.doc.Add(t)
		}
	}
	if shown == 0 {
		b.doc.Add(&Para{"No case was run by kvutil and another system.", "note"})
	}
}

// summaryTable is the table of the workload, nil if no case has anything to compare.
func (w *wctx) summaryTable() *Table {
	var others []string // the systems besides kvutil that the workload has, in the order of the columns
	seen := map[string]bool{}
	for _, s := range w.order {
		sys := systemOf(s)
		if sys != "kvutil" && !seen[sys] {
			seen[sys] = true
			others = append(others, sys)
		}
	}
	t := &Table{Header: []string{"case", "workers", "unit", "kvutil", "variant"}}
	for _, sys := range others {
		t.Header = append(t.Header, systemName(sys), "variant")
	}
	t.Header = append(t.Header, "kvutil / best other")

	for _, c := range w.cases() {
		if notSummarised[[2]string{w.workload, c.name}] {
			continue
		}
		conc := c.concs[len(c.concs)-1]
		if _, ok := c.byConc[summaryWorkers]; ok {
			conc = summaryWorkers
		}
		rows := c.byConc[conc]
		m, unit := summaryMetric(rows)
		if m == nil {
			continue
		}

		bests := map[string]best{}
		for _, x := range rows {
			v, ok := m.get(x)
			if !ok || math.IsNaN(v) || v <= 0 {
				continue
			}
			cur, have := bests[x.r.System]
			if !have || (unit != "seconds" && v > cur.val) || (unit == "seconds" && v < cur.val) {
				bests[x.r.System] = best{v, x.r.Variant}
			}
		}
		kv, ok := bests["kvutil"]
		if !ok || len(bests) < 2 {
			continue
		}
		bestOther := math.NaN()
		for sys, v := range bests {
			if sys == "kvutil" {
				continue
			}
			if math.IsNaN(bestOther) || (unit != "seconds" && v.val > bestOther) || (unit == "seconds" && v.val < bestOther) {
				bestOther = v.val
			}
		}
		rel := ratio(kv.val, bestOther)
		if unit == "seconds" {
			rel = ratio(bestOther, kv.val)
		}

		cells := cellRow(c.name, conc, unit, N(kv.val, m.fmt), kv.variant)
		for _, sys := range others {
			if o, ok := bests[sys]; ok {
				cells = append(cells, N(o.val, m.fmt), T(o.variant))
			} else {
				cells = append(cells, T(""), T(""))
			}
		}
		cells = append(cells, N(rel, fmtRatio))
		t.Rows = append(t.Rows, cells)
	}
	if len(t.Rows) == 0 {
		return nil
	}
	return t
}

// summaryMetric is the first of summaryMetrics that applies to a result of the rows, and its name.
func summaryMetric(rows []*row) (*metric, string) {
	for _, name := range summaryMetrics {
		for i := range caseMetrics {
			m := &caseMetrics[i]
			if m.label != name {
				continue
			}
			for _, x := range rows {
				if _, ok := m.get(x); ok {
					return m, name
				}
			}
		}
	}
	return nil, ""
}

// systemOf is the system of a series, which is system/variant.
func systemOf(series string) string {
	sys, _, _ := strings.Cut(series, "/")
	return sys
}
