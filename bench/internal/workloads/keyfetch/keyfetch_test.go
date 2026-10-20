package keyfetch

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

func TestCountsAndPicks(t *testing.T) {
	if got := counts(1_000_000); !reflect.DeepEqual(got, []int64{10, 100, 1000, 10_000, 100_000, 500_000}) {
		t.Errorf("counts of a million: %v", got)
	}
	if got := counts(10_000); !reflect.DeepEqual(got, []int64{10, 100, 1000, 5000}) {
		t.Errorf("counts of 10,000: %v", got)
	}
	if got := counts(100); !reflect.DeepEqual(got, []int64{10, 50}) {
		t.Errorf("counts of 100: %v", got)
	}
	if !scans(10_000, 1_000_000) || scans(1000, 1_000_000) {
		t.Error("scans")
	}

	for _, k := range []int64{1, 10, 400, 900} {
		ids := pick(1, 1000, k, 0)
		if int64(len(ids)) != k {
			t.Fatalf("%d ids for %d", len(ids), k)
		}
		seen := map[int64]bool{}
		for _, id := range ids {
			if id < 0 || id >= 1000 || seen[id] {
				t.Fatalf("k=%d: bad or repeated id %d", k, id)
			}
			seen[id] = true
		}
		if !reflect.DeepEqual(ids, pick(1, 1000, k, 0)) {
			t.Errorf("k=%d: the same trial picks other ids", k)
		}
		if k >= 10 && reflect.DeepEqual(ids, pick(1, 1000, k, 1)) {
			t.Errorf("k=%d: two trials pick the same ids", k)
		}
	}
}

// All the ways read the same values of the same keys, also when the database was opened again.
func TestWaysReadTheSame(t *testing.T) {
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(800))
	if len(outs) != 2*len(ways) {
		t.Fatalf("%d outputs", len(outs))
	}
	if len(diffs) > 0 {
		t.Fatalf("the ways differ:\n%s", strings.Join(diffs, "\n"))
	}
	for _, o := range outs {
		if len(o.Lines) == 0 || !strings.Contains(o.Lines[0], "10 values") {
			t.Errorf("%s: %v", o.Variant, o.Lines)
		}
	}
	// The sizes of the values differ.
	if outs[0].Lines[0] == outs[len(ways)].Lines[0] {
		t.Error("small and large read the same bytes")
	}
}

func TestRunReportsNanosecondsPerRecord(t *testing.T) {
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), testutil.Params(800))
	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.Variant+"/"+r.Case] = true
		e := r.Metrics.Extra
		if e["ns_per_record_median"] <= 0 || e["trials"] < 3 || e["ns_per_record_min"] > e["ns_per_record_max"] {
			t.Errorf("%s/%s: %v", r.Variant, r.Case, e)
		}
	}
	for _, v := range []string{"small", "large"} {
		for _, c := range []string{"random-get/k10/warm", "sorted-get/k100/cold", "seek/k400/warm", "scan/k400/cold", "scan/k100/warm"} {
			if !seen[v+"/"+c] {
				t.Errorf("no result for %s/%s", v, c)
			}
		}
	}
}
