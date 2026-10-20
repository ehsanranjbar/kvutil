package bulkload

import (
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// Every strategy of kvutil leaves the same records; Verify fails if one does not.
func TestKvutilStrategiesLoadTheSame(t *testing.T) {
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(500))
	if len(outs) != 1 || len(diffs) > 0 {
		t.Fatalf("outputs = %v, differences = %v", outs, diffs)
	}
	if !strings.HasPrefix(outs[0].Lines[0], "500 records, digest ") {
		t.Errorf("line = %q", outs[0].Lines[0])
	}
}

func TestRunEmitsEveryStrategy(t *testing.T) {
	p := testutil.Params(6000)
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)

	got := map[string]int64{}
	for _, r := range rs {
		got[r.Case] += r.Metrics.Records
		if r.Metrics.RecordsPerSec <= 0 || r.Metrics.DiskBytes <= 0 || r.Params.Case["workers"] == nil {
			t.Errorf("%s: %+v", r.Case, r.Metrics)
		}
	}
	// Each strategy runs at one worker and at two; the loads of single records are capped.
	want := map[string]int64{
		"writebatch": 12000, "txn-b1000": 12000, "txn-b100": 12000, "txn-b1": 10000,
	}
	for c, n := range want {
		if got[c] != n {
			t.Errorf("%s loaded %d records, want %d", c, got[c], n)
		}
	}
	for _, r := range rs {
		if r.Case == "txn-b1" && r.Params.Case["capped"] != true {
			t.Errorf("txn-b1 of 6000 records is not marked as capped")
		}
	}
}

func TestPlaceholders(t *testing.T) {
	if got, want := placeholders(2, 3), "($1, $2, $3), ($4, $5, $6)"; got != want {
		t.Errorf("placeholders = %q, want %q", got, want)
	}
}
