package kvbasic

import (
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// The variants of kvutil hold the same bytes, so they answer the same; PostgreSQL is compared
// with them by the command verify, which needs it.
func TestKvutilVariantsAgree(t *testing.T) {
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(300))
	if len(outs) != 3 {
		t.Fatalf("%d outputs", len(outs))
	}
	if len(diffs) > 0 {
		t.Fatalf("the variants differ:\n%s", strings.Join(diffs, "\n"))
	}
	lines := outs[0].Lines
	if len(lines) < 600 {
		t.Fatalf("only %d lines", len(lines))
	}
	// The script deleted and changed records, and added some.
	found := map[string]bool{}
	for _, l := range lines {
		found[l] = true
	}
	for _, want := range []string{"get 0: true", "get 0: false", "get 300: true", "get 320: false", "scan 50 rows from 0"} {
		if !found[want] {
			t.Errorf("%q is not in the output", want)
		}
	}
}

func TestRunEmitsEveryCase(t *testing.T) {
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), testutil.Params(300))

	cases := map[string]int{}
	for _, r := range rs {
		cases[r.Variant+"/"+r.Case]++
		m := r.Metrics
		if r.Case == "load" {
			if m.Records != 300 || m.DiskBytes == 0 {
				t.Errorf("%s/%s: %+v", r.Variant, r.Case, m)
			}
			continue
		}
		if m.Ops == 0 || m.Errors != 0 || m.Latency == nil || m.Latency.P50Us <= 0 {
			t.Errorf("%s/%s c=%d: ops=%d errors=%d %v", r.Variant, r.Case, r.Params.Concurrency, m.Ops, m.Errors, m.ErrorSample)
		}
		if strings.HasPrefix(r.Case, "scan") && (m.UnitName != "rows" || m.Units == 0) {
			t.Errorf("%s/%s: no rows counted", r.Variant, r.Case)
		}
	}
	for _, variant := range []string{"typed", "raw", "typed-bytes"} {
		for _, c := range []string{"load", "get", "scan-short", "scan-long", "set-update", "delete", "set-insert"} {
			want := 2
			if c == "load" {
				want = 1
			}
			if got := cases[variant+"/"+c]; got != want {
				t.Errorf("%s/%s: %d results, want %d", variant, c, got, want)
			}
		}
	}
}
