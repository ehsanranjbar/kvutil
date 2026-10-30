package lifecycle

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// The answers of the new indexes and of the records after the value change are what the dataset
// says: the test computes them from the records.
func TestKvutilAnswersLikeTheRecords(t *testing.T) {
	p := testutil.Params(800)
	spec := p.Dataset()
	outs, _ := testutil.Verify(t, workload{}, testutil.Kvutil(t), p)
	if len(outs) != len(variants) {
		t.Fatalf("%d outputs, want %d", len(outs), len(variants))
	}

	records := make([]*dataset.Order, 0, p.Scale)
	for id := int64(1); id <= int64(p.Scale); id++ {
		records = append(records, spec.Gen(id, 0))
	}
	idsWhere := func(f func(o *dataset.Order) bool) string {
		var ids []int64
		for _, o := range records {
			if f(o) {
				ids = append(ids, o.ID)
			}
		}
		return idLine("", ids)[2:]
	}

	byName := map[string][]string{}
	for _, o := range outs {
		byName[o.Variant] = o.Lines
	}
	has := func(variant, label string, f func(o *dataset.Order) bool) {
		t.Helper()
		want := label + ": " + idsWhere(f)
		if !slices.Contains(byName[variant], want) {
			t.Errorf("%s: no line %q in\n%s", variant, want, strings.Join(byName[variant], "\n"))
		}
	}

	has("index-single", "new index customer=cust-000007", func(o *dataset.Order) bool { return o.Customer == "cust-000007" })
	has("index-single", "rebuilt index customer=cust-000007", func(o *dataset.Order) bool { return o.Customer == "cust-000007" })
	has("index-unique", "new index code="+spec.Gen(5, 0).Code, func(o *dataset.Order) bool { return o.ID == 5 })
	has("index-unique", "new index code="+spec.Gen(5, 1).Code, func(o *dataset.Order) bool { return false })
	has("index-multi", "new index tags=tag-07", func(o *dataset.Order) bool { return slices.Contains(o.Tags, "tag-07") })
	has("index-composite", fmt.Sprintf("new index customer=cust-000001 and at in [%d, %d]", base, base+365*day), func(o *dataset.Order) bool {
		return o.Customer == "cust-000001" && o.At.Unix() >= base && o.At.Unix() <= base+365*day
	})
	for k := int32(0); k < 10; k++ {
		for _, v := range []string{"migrate", "migrate-indexed"} {
			has(v, fmt.Sprintf("priority %d", k), func(o *dataset.Order) bool { return priorityOf(o.Total) == k })
		}
	}
	has("migrate-indexed", "after the change, index customer=cust-000007", func(o *dataset.Order) bool { return o.Customer == "cust-000007" })
	has("migrate-indexed", "after the change, index status=late", func(o *dataset.Order) bool { return o.Status == "late" })
	has("open", "after reopening, index total in [20000, 23000]", func(o *dataset.Order) bool { return o.Total >= 20000 && o.Total <= 23000 })
	if !slices.Contains(byName["migrate"], "priorities cover the records: true") {
		t.Error("the priorities do not cover the records")
	}
	if !slices.Contains(byName["migrate"], "record 1 is there after the field was added: true") {
		t.Error("record 1 is gone after the field was added")
	}
}

func TestRunEmitsTheCases(t *testing.T) {
	p := testutil.Params(500)
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)
	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.Variant+"/"+r.Case] = true
		if strings.HasSuffix(r.Case, "-new") || strings.HasSuffix(r.Case, "-loaded") {
			if r.Metrics.Extra["ready_ms_median"] <= 0 || r.Metrics.Extra["trials"] == 0 {
				t.Errorf("%s: extra %v", r.Case, r.Metrics.Extra)
			}
		}
		if r.Case == "convert" && (r.Metrics.Records != 500 || r.Metrics.RecordsPerSec <= 0) {
			t.Errorf("%s/convert: %+v", r.Variant, r.Metrics)
		}
	}
	for _, name := range []string{
		"index-single/add-index", "index-single/drop-index", "index-multi/add-index", "index-unique/drop-index",
		"index-composite/add-index", "migrate/convert", "migrate/add-field", "migrate-indexed/build-indexes",
		"migrate-indexed/convert", "open/open-new", "open/open-loaded", "open/restart-loaded",
	} {
		if !seen[name] {
			t.Errorf("no result for %s", name)
		}
	}
	if seen["index-single/add-index-concurrently"] {
		t.Error("kvutil has no concurrent build")
	}
}

func TestIndexStatements(t *testing.T) {
	for _, kind := range []string{"customer", "status", "total", "code", "composite", "tags"} {
		for _, conc := range []bool{false, true} {
			stmt := indexDDL("s.orders", kind, conc)
			if strings.Contains(stmt, "%!") || strings.Contains(stmt, "CONCURRENTLY") != conc || !strings.Contains(stmt, "ix_"+kind) {
				t.Errorf("%s (concurrently %t): %s", kind, conc, stmt)
			}
		}
	}
	if !strings.Contains(indexDDL("t", "code", false), "UNIQUE") || !strings.Contains(indexDDL("t", "tags", false), "gin") {
		t.Error("the unique and the multi-valued index are not what they should be")
	}
}
