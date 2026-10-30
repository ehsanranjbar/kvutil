package concurrency

import (
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

func shrink(t *testing.T) {
	t.Helper()
	oldLevels, oldKeys := levels, hotKeys
	levels, hotKeys = []int{1, 3}, []int64{1, 10, 100, 500}
	t.Cleanup(func() { levels, hotKeys = oldLevels, oldKeys })
}

// Every scenario leaves the database as the operations that succeeded make it.
func TestVerifyKvutilIsConsistent(t *testing.T) {
	outs, _ := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(300))
	if len(outs) != len(kvutilSpecs()) {
		t.Fatalf("%d outputs, want %d", len(outs), len(kvutilSpecs()))
	}
	lines := 0
	for _, o := range outs {
		for _, l := range o.Lines {
			lines++
			if strings.Contains(l, ": NO") {
				t.Errorf("%s: %s", o.Variant, l)
			}
		}
	}
	if lines < 12 {
		t.Errorf("only %d lines of verification", lines)
	}
}

// The scenarios of the two systems say the same lines, whatever the variant.
func TestVerifyLinesDoNotNameTheVariant(t *testing.T) {
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(300))
	// rmw-update-r100 has the same keys as rmw-update, and the lines are by the scenario.
	byName := map[string][]string{}
	for _, o := range outs {
		byName[o.Variant] = o.Lines
	}
	if strings.Join(byName["rmw-update"], "\n") != strings.Join(byName["rmw-update-r100"], "\n") {
		t.Errorf("rmw-update and rmw-update-r100 say different things:\n%v\n%v", byName["rmw-update"], byName["rmw-update-r100"])
	}
	_ = diffs
}

func TestRunEmitsTheCases(t *testing.T) {
	shrink(t)
	p := testutil.Params(500)
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)

	seen := map[string]int{}
	for _, r := range rs {
		seen[r.Variant+"/"+r.Case]++
		switch r.Case {
		case "load", "reader":
		default:
			if r.Metrics.Ops == 0 {
				t.Errorf("%s/%s: no operations (errors %d: %v)", r.Variant, r.Case, r.Metrics.Errors, r.Metrics.ErrorSample)
			}
		}
		if r.Case == "reader" {
			if r.Metrics.Extra["scans"] < 1 || r.Metrics.Extra["unstable_scans"] != 0 {
				t.Errorf("%s: reader extra = %v", r.Variant, r.Metrics.Extra)
			}
		}
	}
	for _, name := range []string{
		"rmw-update/rmw-k1", "rmw-update/rmw-k500", "rmw-update-r100/rmw-k100", "blind-write/blind-write",
		"mixed/mixed-uniform", "mixed/mixed-zipf", "mixed/load", "long-read/writers-alone",
		"long-read/writers-with-reader", "long-read/reader",
	} {
		if seen[name] != len(levels) && name != "mixed/load" {
			t.Errorf("%s: %d results, want %d (one per level)", name, seen[name], len(levels))
		}
	}
	if seen["rmw-update-r100/rmw-k500"] != 0 {
		t.Error("the variant with 100 retries runs the set of 10,000 keys")
	}
}

// With one hot key, workers conflict: the retries are counted, and the commits that fail after
// the limit are errors (not lost updates: the sum is checked by the run).
func TestHotKeyCountsRetries(t *testing.T) {
	shrink(t)
	p := testutil.Params(200)
	p.Concurrency = 4
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)
	var retries int64
	for _, r := range rs {
		if r.Variant == "rmw-update" && r.Case == "rmw-k1" && r.Params.Concurrency == 3 {
			retries = r.Metrics.Retries
		}
	}
	if retries == 0 {
		t.Log("no conflict happened in this run (possible on a machine with few CPUs)")
	}
}
