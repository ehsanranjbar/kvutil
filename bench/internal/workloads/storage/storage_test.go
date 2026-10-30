package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

func TestVerifyKvutilCountsRecords(t *testing.T) {
	old := expiryTTL
	expiryTTL = time.Second
	t.Cleanup(func() { expiryTTL = old })

	p := testutil.Params(400)
	outs, _ := testutil.Verify(t, workload{}, testutil.Kvutil(t), p)
	byName := map[string][]string{}
	for _, o := range outs {
		byName[o.Variant] = o.Lines
	}
	want := map[string][]string{
		"footprint": {
			"after the load: 400 records", "after the rewrite 1: 400 records", "after the rewrite 2: 400 records",
			"after deleting half: 200 records", "after reclaiming: 200 records",
		},
		"expiry": {
			"just written: 200 records that do not expire, 200 that do",
			"after the ttl: 200 records that do not expire, 0 that do",
		},
	}
	for variant, lines := range want {
		if strings.Join(byName[variant], "\n") != strings.Join(lines, "\n") {
			t.Errorf("%s:\n%s\nwant\n%s", variant, strings.Join(byName[variant], "\n"), strings.Join(lines, "\n"))
		}
	}
}

func TestRunEmitsTheCases(t *testing.T) {
	old := expiryTTL
	expiryTTL = time.Second
	t.Cleanup(func() { expiryTTL = old })

	p := testutil.Params(600)
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)
	seen := map[string]map[string]float64{}
	for _, r := range rs {
		seen[r.Variant+"/"+r.Case] = r.Metrics.Extra
		if r.Metrics.Errors != 0 {
			t.Errorf("%s/%s: %d errors: %v", r.Variant, r.Case, r.Metrics.Errors, r.Metrics.ErrorSample)
		}
		if r.Case == "load" && r.Metrics.Extra["disk_bytes_after"] <= 0 {
			t.Errorf("%s/load: no size on disk: %v", r.Variant, r.Metrics.Extra)
		}
		// Without Docker the bytes written to the disk are not known, and the result says so.
		if r.Case == "load" && r.Metrics.Info["io_source"] == "" {
			t.Errorf("%s/load: no io_source", r.Variant)
		}
		if r.Metrics.Info["io_source"] == "unavailable" {
			if _, ok := r.Metrics.Extra["container_write_bytes"]; ok {
				t.Errorf("%s/%s has bytes written from a meter that is unavailable", r.Variant, r.Case)
			}
		}
	}
	for _, name := range []string{
		"footprint/load", "footprint/add-index-customer", "footprint/add-index-tags", "footprint/footprint",
		"footprint/overwrite-1", "footprint/overwrite-3", "footprint/delete-half", "footprint/value-log-gc",
		"footprint/flatten", "footprint-vlog/flatten", "expiry/insert-plain", "expiry/insert-ttl",
		"expiry/insert-ttl-io", "expiry/after-expiry", "expiry/flatten",
	} {
		if _, ok := seen[name]; !ok {
			t.Errorf("no result for %s", name)
		}
	}
	fp := seen["footprint/footprint"]
	for _, k := range []string{"table_bytes", "total_bytes", "bytes_per_record", "index_customer_bytes", "index_tags_logical_bytes"} {
		if fp[k] <= 0 {
			t.Errorf("footprint %s = %v in %v", k, fp[k], fp)
		}
	}
	if seen["footprint/overwrite-1"]["user_bytes"] <= 0 {
		t.Errorf("overwrite-1: %v", seen["footprint/overwrite-1"])
	}
}

func TestIndexStatements(t *testing.T) {
	for _, k := range indexKinds {
		if s := indexDDL("s.orders", k); !strings.Contains(s, "ix_"+k) || strings.Contains(s, "%!") {
			t.Errorf("%s: %s", k, s)
		}
	}
	if !strings.Contains(indexDDL("t", "code"), "UNIQUE") {
		t.Error("the index on the code is not unique")
	}
}
