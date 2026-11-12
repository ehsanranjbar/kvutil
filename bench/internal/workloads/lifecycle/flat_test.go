package lifecycle

import (
	"reflect"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat/flattest"
)

func strp(s string) *string { return &s }

func versions(size dataset.ValueSize) ([]orderV2, []orderV3Flat) {
	spec := dataset.Spec{Seed: 5, Scale: 1000, Values: size}
	var v2 []orderV2
	var v3 []orderV3Flat
	for id := int64(1); id <= 20; id++ {
		v2 = append(v2, *orderToV2(spec.Gen(id, 0)))
		o := orderToV2(spec.Gen(id, 0))
		v3 = append(v3, orderV3Flat{Code: o.Code, Customer: o.Customer, Name: o.Name, Status: o.Status,
			Total: o.Total, Qty: o.Qty, At: o.At, Note: o.Note, Tags: o.Tags, Payload: o.Payload,
			Priority: o.Priority, Extra: "extra"})
	}
	edge := orderV2{Note: strp(""), Tags: []string{"", "a"}, Priority: -1, At: time.Unix(0, 0).UTC()}
	v2 = append(v2, orderV2{At: time.Unix(0, 0).UTC()}, edge)
	v3 = append(v3, orderV3Flat{At: time.Unix(0, 0).UTC()},
		orderV3Flat{Note: strp(""), Tags: []string{"", "a"}, Priority: -1, Extra: "x", At: time.Unix(0, 0).UTC()})
	for i := range v2 {
		v2[i].ID = 0
	}
	for i := range v3 {
		v3[i].ID = 0
	}
	return v2, v3
}

func TestFlatCodecs(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		v2, v3 := versions(size)
		t.Run("v2", func(t *testing.T) { flattest.Check(t, orderV2Codec(), v2) })
		t.Run("v3", func(t *testing.T) { flattest.Check(t, orderV3Codec(), v3) })
	}
}

// The versions are compatible as the workload relies on: a record of version 1 (an order) is read
// by the codec of version 2 or 3 without the priority and the extra, a record written as version
// 2 is read by version 3 with an empty extra, and one written as version 3 is read by version 2,
// which does not know the extra. The change that needs no migration needs none for the bytes.
func TestFlatVersionsReadEachOther(t *testing.T) {
	spec := dataset.Spec{Seed: 5, Scale: 1000, Values: dataset.Small}
	for id := int64(1); id <= 20; id++ {
		o := spec.Gen(id, 0)
		o.ID = 0
		v2 := *orderToV2(o)
		v2.ID = 0

		// The version 1 record is a prefix of the fields of the later ones.
		bz := flat.EncodeOrder(o)
		got2, err := orderV2Codec().Decode(bz)
		if err != nil {
			t.Fatal(err)
		}
		want := v2
		want.Priority = 0
		if !reflect.DeepEqual(got2, want) {
			t.Fatalf("version 1 read as 2: %+v, want %+v", got2, want)
		}

		bz, err = orderV2Codec().Encode(v2)
		if err != nil {
			t.Fatal(err)
		}
		got3, err := orderV3Codec().Decode(bz)
		if err != nil {
			t.Fatal(err)
		}
		want3 := orderV3Flat{Code: v2.Code, Customer: v2.Customer, Name: v2.Name, Status: v2.Status, Total: v2.Total,
			Qty: v2.Qty, At: v2.At, Note: v2.Note, Tags: v2.Tags, Payload: v2.Payload, Priority: v2.Priority}
		if !reflect.DeepEqual(got3, want3) {
			t.Fatalf("version 2 read as 3: %+v, want %+v", got3, want3)
		}
		// And in part: a field that was not written is zero.
		dec, ok := orderV3Table.PartialDecode([]string{"Priority", "Extra"})
		if !ok {
			t.Fatal("no partial decoding")
		}
		var part orderV3Flat
		if err := dec(bz, &part); err != nil || part.Priority != v2.Priority || part.Extra != "" {
			t.Fatalf("partial of version 2 read as 3: %+v (%v)", part, err)
		}

		w3 := want3
		w3.Extra = "more"
		bz, err = orderV3Codec().Encode(w3)
		if err != nil {
			t.Fatal(err)
		}
		back2, err := orderV2Codec().Decode(bz)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(back2, v2) {
			t.Fatalf("version 3 read as 2: %+v, want %+v", back2, v2)
		}
	}
}
