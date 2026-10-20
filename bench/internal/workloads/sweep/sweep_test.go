package sweep

import (
	"sort"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

func TestSteps(t *testing.T) {
	// A million rows has all the steps, from 10 rows to half the table.
	st := steps(1_000_000)
	if len(st) != len(percents) {
		t.Fatalf("%d steps", len(st))
	}
	if st[0].rows != 10 || st[len(st)-1].rows != 500_000 {
		t.Errorf("rows %d to %d", st[0].rows, st[len(st)-1].rows)
	}
	for i := 1; i < len(st); i++ {
		if st[i].rows <= st[i-1].rows {
			t.Errorf("step %d has %d rows after %d", i, st[i].rows, st[i-1].rows)
		}
	}
	if st[0].name() != "0.001pct" || st[2].name() != "0.1pct" || st[len(st)-1].name() != "50pct" {
		t.Errorf("names %s %s %s", st[0].name(), st[2].name(), st[len(st)-1].name())
	}

	// A small table has fewer, and none under 10 rows.
	small := steps(10_000)
	if len(small) >= len(percents) || small[0].rows != 10 {
		t.Errorf("steps of 10,000: %+v", small)
	}
	for _, s := range small {
		if s.rows < 10 || s.rows > 5000 {
			t.Errorf("a step of %d rows in a table of 10,000", s.rows)
		}
	}
	if len(steps(100)) == 0 {
		t.Error("no steps for 100 rows")
	}
}

func TestRangesCoverTheRowsOfTheStep(t *testing.T) {
	spec := dataset.Spec{Seed: 1, Scale: 20_000, Values: dataset.Small}
	st := steps(20_000)
	var scores []int64
	for id := int64(1); id <= 20_000; id++ {
		scores = append(scores, spec.GenBook(id).Score)
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i] < scores[j] })
	for idx, s := range st {
		pool := ranges(1, idx, s)
		if len(pool) != poolSize {
			t.Fatalf("%d ranges", len(pool))
		}
		total := 0
		for _, r := range pool {
			if r[0] < 0 || r[1] >= dataset.ScoreRange || r[1]-r[0]+1 != s.width {
				t.Fatalf("range %v for width %d", r, s.width)
			}
			lo := sort.Search(len(scores), func(i int) bool { return scores[i] >= r[0] })
			hi := sort.Search(len(scores), func(i int) bool { return scores[i] > r[1] })
			total += hi - lo
		}
		avg := float64(total) / float64(len(pool))
		if avg < float64(s.rows)*0.8 || avg > float64(s.rows)*1.2 {
			t.Errorf("step %s: ranges find %.1f rows on average, want about %d", s.name(), avg, s.rows)
		}
	}
}

// The three variants of kvutil give the same books for every range, in the order of the key when
// they are asked for it, and they are the books whose scores are in the range.
func TestKvutilVariantsAgree(t *testing.T) {
	p := testutil.Params(2000)
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), p)
	if len(outs) != 3 {
		t.Fatalf("%d outputs", len(outs))
	}
	if len(diffs) > 0 {
		t.Fatalf("the variants differ:\n%s", strings.Join(diffs, "\n"))
	}
	for _, line := range outs[0].Lines {
		if !strings.Contains(line, " rows digest=") || strings.Contains(line, ": 0 rows") {
			t.Errorf("a range finds nothing: %s", line)
		}
	}
	if want := len(steps(2000)) * len(orders) * verifyRuns; len(outs[0].Lines) != want {
		t.Errorf("%d lines, want %d", len(outs[0].Lines), want)
	}
}

func TestRunEmitsEveryStepWithItsPlan(t *testing.T) {
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), testutil.Params(1000))
	plan := map[string]string{}
	count := map[string]int{}
	for _, r := range rs {
		if r.Case == "load" || r.Case == "build" {
			continue
		}
		m := r.Metrics
		if m.Ops == 0 || m.Errors != 0 || m.Units == 0 {
			t.Errorf("%s/%s: ops=%d errors=%d units=%d %v", r.Variant, r.Case, m.Ops, m.Errors, m.Units, m.ErrorSample)
		}
		count[r.Variant]++
		plan[r.Variant+"/"+r.Case] = m.Info["plan_nodes"]
		if r.Params.Case["target_rows"] == nil || r.Params.Case["order"] == nil {
			t.Errorf("%s/%s: params %v", r.Variant, r.Case, r.Params.Case)
		}
	}
	want := len(steps(1000)) * len(orders) * 2
	for _, v := range []string{"planner", "index", "scan"} {
		if count[v] != want {
			t.Errorf("%s: %d results, want %d", v, count[v], want)
		}
	}
	first := steps(1000)[0].name()
	if got := plan["index/"+first+"-unordered"]; got != "IndexLookup[by_score]" {
		t.Errorf("index, unordered: plan %q", got)
	}
	if got := plan["scan/"+first+"-by-pk"]; !strings.Contains(got, "Sort") || !strings.Contains(got, "StoreScan") {
		t.Errorf("scan, by pk: plan %q", got)
	}
}

func TestStepNamesAreShort(t *testing.T) {
	st := steps(300_000)
	if got := st[0].name(); got != "0.00333pct" {
		t.Errorf("name %q", got)
	}
	seen := map[string]bool{}
	for _, n := range []int64{300, 3000, 20_000, 300_000, 1_000_000, 123_456_789} {
		clear(seen)
		for _, s := range steps(n) {
			if len(s.name()) > 14 || seen[s.name()] {
				t.Errorf("n=%d: step name %q", n, s.name())
			}
			seen[s.name()] = true
		}
	}
}
