package codecs

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

func TestBinaryRoundTrip(t *testing.T) {
	for _, values := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		spec := dataset.Spec{Seed: 5, Scale: 500, Values: values}
		for id := int64(1); id <= 300; id++ {
			in := (*OrderBin)(spec.Gen(id, uint32(id%3)))
			bz, err := in.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			var out OrderBin
			if err := out.UnmarshalBinary(bz); err != nil {
				t.Fatalf("id %d: %v", id, err)
			}
			out.ID = in.ID
			if !reflect.DeepEqual(&out, in) {
				t.Fatalf("id %d does not round trip:\n%+v\n%+v", id, in, &out)
			}
			// A record that is cut short is an error, not a record.
			if len(bz) > 3 {
				var cut OrderBin
				if err := cut.UnmarshalBinary(bz[:len(bz)-2]); err == nil {
					t.Fatalf("id %d: a truncated record decodes", id)
				}
			}
		}
	}
}

func TestBinaryIsSmallerThanMsgpack(t *testing.T) {
	spec := dataset.Spec{Seed: 5, Scale: 500, Values: dataset.Small}
	o := spec.Gen(7, 0)
	bin, _ := (*OrderBin)(o).MarshalBinary()
	if len(bin) >= 150 {
		t.Errorf("a binary record takes %d bytes", len(bin))
	}
}

func TestKvutilCodecsReturnTheSameRecords(t *testing.T) {
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(300))
	if len(outs) != 3 {
		t.Fatalf("%d outputs", len(outs))
	}
	if len(diffs) > 0 {
		t.Fatalf("the codecs differ:\n%s", strings.Join(diffs, "\n"))
	}
	if n := len(outs[0].Lines); n != 312 {
		t.Errorf("%d lines, want one for each of 312 ids", n)
	}
	if !strings.Contains(outs[0].Lines[311], "missing") || strings.Contains(outs[0].Lines[309], "missing") {
		t.Errorf("the last lines are %q", outs[0].Lines[308:])
	}
}

func TestRunEmitsEveryCase(t *testing.T) {
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), testutil.Params(300))
	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.Variant+"/"+r.Case] = true
		switch r.Case {
		case "size":
			if b := r.Metrics.Extra["avg_record_bytes"]; b < 50 || b > 400 {
				t.Errorf("%s: %v bytes per record", r.Variant, b)
			}
		case "load":
		default:
			if r.Metrics.Ops == 0 || r.Metrics.Errors != 0 {
				t.Errorf("%s/%s: ops=%d errors=%d %v", r.Variant, r.Case, r.Metrics.Ops, r.Metrics.Errors, r.Metrics.ErrorSample)
			}
		}
	}
	for _, v := range []string{"msgpack", "json", "binary"} {
		for _, c := range []string{"load", "size", "get", "set-update", "set-insert"} {
			if !seen[v+"/"+c] {
				t.Errorf("no result for %s/%s", v, c)
			}
		}
	}
}
