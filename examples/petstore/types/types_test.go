package types

import (
	"reflect"
	"slices"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
)

func TestPetRecordBinary(t *testing.T) {
	// The id is the key of the record, so it is not encoded.
	in := PetRecord{Id: 5, CategoryId: 7, Name: "rex", Status: Status_SOLD, CreatedAt: 1700000000}
	c := Codec[PetRecord]()
	bz, err := c.Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Decode(bz)
	if err != nil {
		t.Fatal(err)
	}
	want := in
	want.Id = 0
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// The partial decoding fills the fields that are asked for, and may fill more.
	decode, ok := codec.PartialOf(c, []string{"Name", "Id"})
	if !ok {
		t.Fatal("the codec has no partial decoding")
	}
	var part PetRecord
	if err := decode(bz, &part); err != nil {
		t.Fatal(err)
	}
	if part != (PetRecord{Name: "rex"}) {
		t.Fatalf("got %+v", part)
	}

	// A field that is not known is decoded with the others.
	decode, _ = codec.PartialOf(c, []string{"Name", "Other"})
	part = PetRecord{}
	if err := decode(bz, &part); err != nil {
		t.Fatal(err)
	}
	if part != want {
		t.Fatalf("got %+v, want %+v", part, want)
	}

	// Data that is not a table is an error, and not a panic.
	for _, bad := range [][]byte{nil, {1}, slices.Repeat([]byte{0xff}, 40)} {
		if _, err := c.Decode(bad); err == nil {
			t.Errorf("%v: got no error", bad)
		}
	}
	if c.Name() != "flatbuffers" {
		t.Fatalf("got the name %q", c.Name())
	}
}

func TestCategoryBinary(t *testing.T) {
	c := Codec[Category]()
	bz, err := c.Encode(Category{Id: 3, Name: "dogs"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Decode(bz)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Category{Name: "dogs"}) {
		t.Fatalf("got %+v", got)
	}
	decode, ok := codec.PartialOf(c, []string{"Name"})
	if !ok {
		t.Fatal("the codec has no partial decoding")
	}
	var part Category
	if err := decode(bz, &part); err != nil || part.Name != "dogs" {
		t.Fatalf("got %+v, %v", part, err)
	}
}
