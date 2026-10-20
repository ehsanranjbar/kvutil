package indexmaint

import (
	"strconv"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// The indexes of kvutil are kept up to date through the changes of the script: every
// configuration answers its probes with the records that satisfy them, which the test computes
// from the same changes on the dataset.
func TestKvutilIndexesAnswerLikeTheRecords(t *testing.T) {
	p := testutil.Params(600)
	spec := p.Dataset()
	outs, _ := testutil.Verify(t, workload{}, testutil.Kvutil(t), p)
	if len(outs) != len(configs) {
		t.Fatalf("%d outputs, want %d", len(outs), len(configs))
	}

	// The records after the changes of the script.
	n := int64(p.Scale)
	var records []*dataset.Order
	for id := int64(1); id <= n+25; id++ {
		switch {
		case id <= n && id%7 == 0:
		case id <= n && id%5 == 1:
			records = append(records, spec.Gen(id, 3))
		default:
			records = append(records, spec.Gen(id, 0))
		}
	}

	matches := map[string]func(o *dataset.Order, pr probe) bool{
		"customer": func(o *dataset.Order, pr probe) bool { return o.Customer == pr.S },
		"status":   func(o *dataset.Order, pr probe) bool { return o.Status == pr.S },
		"total":    func(o *dataset.Order, pr probe) bool { return o.Total >= pr.Lo && o.Total <= pr.Hi },
		"created":  func(o *dataset.Order, pr probe) bool { return o.At.Unix() >= pr.Lo && o.At.Unix() <= pr.Hi },
		"name":     func(o *dataset.Order, pr probe) bool { return o.Name == pr.S },
		"code":     func(o *dataset.Order, pr probe) bool { return o.Code == pr.S },
		"composite": func(o *dataset.Order, pr probe) bool {
			return o.Customer == pr.S && o.At.Unix() >= pr.Lo && o.At.Unix() <= pr.Hi
		},
		"lower":       func(o *dataset.Order, pr probe) bool { return strings.ToLower(o.Name) == pr.S },
		"conditional": func(o *dataset.Order, pr probe) bool { return o.Customer == pr.S && o.Total >= dataset.BigTotal },
		"all":         func(o *dataset.Order, pr probe) bool { return true },
		"tag": func(o *dataset.Order, pr probe) bool {
			for _, tag := range o.Tags {
				if tag == pr.S {
					return true
				}
			}
			return false
		},
	}

	nonEmpty := 0
	for _, out := range outs {
		ps := probes(out.Variant, spec)
		if len(ps) != len(out.Lines) {
			t.Fatalf("%s: %d probes, %d lines", out.Variant, len(ps), len(out.Lines))
		}
		for i, pr := range ps {
			var want []string
			for _, o := range records {
				if matches[pr.Kind](o, pr) {
					want = append(want, itoa(o.ID))
				}
			}
			line := pr.Label() + ": " + itoa(int64(len(want))) + " ids " + strings.Join(want, ",")
			if out.Lines[i] != line {
				t.Errorf("%s: %q\n  want %q", out.Variant, out.Lines[i], line)
			}
			if len(want) > 0 {
				nonEmpty++
			}
		}
	}
	if nonEmpty < 30 {
		t.Errorf("only %d probes find records", nonEmpty)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestRunEmitsEveryConfiguration(t *testing.T) {
	p := testutil.Params(400)
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)

	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.Variant+"/"+r.Case] = true
		switch r.Case {
		case "load":
		case "build":
			if r.Metrics.Records != 400 || r.Metrics.RecordsPerSec <= 0 {
				t.Errorf("%s build: %+v", r.Variant, r.Metrics)
			}
		default:
			if r.Metrics.Ops == 0 || r.Metrics.Errors != 0 {
				t.Errorf("%s/%s: ops=%d errors=%d %v", r.Variant, r.Case, r.Metrics.Ops, r.Metrics.Errors, r.Metrics.ErrorSample)
			}
		}
	}
	for _, c := range configs {
		for _, k := range []string{"load", "update", "delete", "insert"} {
			if !seen[c+"/"+k] {
				t.Errorf("no result for %s/%s", c, k)
			}
		}
		if c != "none" && !seen[c+"/build"] {
			t.Errorf("no build of %s", c)
		}
	}
	if seen["none/build"] {
		t.Error("the configuration without indexes builds one")
	}
}
