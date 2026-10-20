package dataset

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestGenIsDeterministic(t *testing.T) {
	spec := Spec{Seed: 7, Scale: 1000, Values: Large}
	for _, id := range []int64{0, 1, 2, 999, 123456} {
		a, b := spec.Gen(id, 0), spec.Gen(id, 0)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("id %d: two generations differ:\n%+v\n%+v", id, a, b)
		}
		if Canon(a) != Canon(b) {
			t.Fatalf("id %d: the canonical forms differ", id)
		}
	}

	// GenInto over a record that was used for another gives the same record.
	o := spec.Gen(5, 3)
	spec.GenInto(o, 77, 0)
	if want := spec.Gen(77, 0); !reflect.DeepEqual(o, want) {
		t.Fatalf("GenInto over a used record differs:\n%+v\n%+v", o, want)
	}
}

func TestGenDependsOnSeedIdAndVersion(t *testing.T) {
	spec := Spec{Seed: 1, Scale: 1000, Values: Small}
	base := Canon(spec.Gen(10, 0))
	for name, o := range map[string]*Order{
		"seed":    Spec{Seed: 2, Scale: 1000, Values: Small}.Gen(10, 0),
		"id":      spec.Gen(11, 0),
		"version": spec.Gen(10, 1),
	} {
		if Canon(o) == base {
			t.Errorf("a different %s gives the same record", name)
		}
	}
}

func TestCodesAreUnique(t *testing.T) {
	spec := Spec{Seed: 1, Scale: 1000, Values: Small}
	seen := map[string]bool{}
	for id := int64(0); id < 2000; id++ {
		for version := uint32(0); version < 3; version++ {
			code := spec.Gen(id, version).Code
			if seen[code] {
				t.Fatalf("the code %s is generated twice", code)
			}
			seen[code] = true
		}
	}
}

func TestShapeOfRecords(t *testing.T) {
	spec := Spec{Seed: 3, Scale: 1000, Values: Small}
	var nilNotes, tagged int
	customers := map[string]bool{}
	for id := int64(1); id <= 2000; id++ {
		o := spec.Gen(id, 0)
		if o.ID != id {
			t.Fatalf("id %d: the record has the id %d", id, o.ID)
		}
		if o.Note == nil {
			nilNotes++
		}
		if len(o.Tags) > 0 {
			tagged++
		}
		seen := map[string]bool{}
		for _, tag := range o.Tags {
			if seen[tag] {
				t.Fatalf("id %d: the tag %s is twice", id, tag)
			}
			seen[tag] = true
		}
		if o.Total < 0 || o.Total >= 100000 || o.Qty < 1 || o.Qty > 20 {
			t.Fatalf("id %d: out of range: %+v", id, o)
		}
		if o.At.Location().String() != "UTC" || o.At.Nanosecond() != 0 {
			t.Fatalf("id %d: the time is %v", id, o.At)
		}
		customers[o.Customer] = true
	}
	if nilNotes < 800 || nilNotes > 1200 {
		t.Errorf("%d of 2000 records have no note, about half were expected", nilNotes)
	}
	if tagged < 1400 {
		t.Errorf("%d of 2000 records have tags", tagged)
	}
	if len(customers) != spec.Customers() {
		t.Errorf("%d customers appear, %d were expected", len(customers), spec.Customers())
	}
}

func TestSizes(t *testing.T) {
	encoded := func(v ValueSize) int {
		total := 0
		spec := Spec{Seed: 1, Scale: 1000, Values: v}
		for id := int64(1); id <= 200; id++ {
			bz, err := msgpack.Marshal(spec.Gen(id, 0))
			if err != nil {
				t.Fatal(err)
			}
			total += len(bz)
		}
		return total / 200
	}
	if n := encoded(Small); n < 120 || n > 320 {
		t.Errorf("a small record takes %d bytes, about 200 were expected", n)
	}
	if n := encoded(Large); n < 4096 || n > 4500 {
		t.Errorf("a large record takes %d bytes, about 4 KB were expected", n)
	}
}

func TestPayloadIsIncompressible(t *testing.T) {
	o := Spec{Seed: 1, Scale: 10, Values: Large}.Gen(1, 0)
	var hist [256]int
	for _, b := range o.Payload {
		hist[b]++
	}
	for b, n := range hist {
		if n == 0 || n > 60 {
			t.Fatalf("the byte %d appears %d times in 4096 random bytes", b, n)
		}
	}
}

func TestCanonIgnoresEmptyTags(t *testing.T) {
	a := &Order{ID: 1, Tags: nil}
	b := &Order{ID: 1, Tags: []string{}}
	if Canon(a) != Canon(b) {
		t.Error("no tags and an empty list of tags are not the same")
	}
}
