// Package flattest is the checks that every codec of bench/internal/flat has to pass, for the tests
// of the package and of the workloads that define a codec of their own.
package flattest

import (
	"bytes"
	"math/rand/v2"
	"reflect"
	"sync"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
)

// Check runs the checks of a codec of V on the records. They cover a round trip, the partial
// decoding of each field alone and of all of them, a name that is not a field, bytes that are
// truncated or corrupt, the result not referring to the input, and concurrent use. The records are
// expected to exercise the codec: empty and non-empty lists, absent and empty optional strings.
//
// A field named ID is not in the value: the decode leaves it zero.
func Check[V any](t *testing.T, c codec.ValueCodec[V], srcs []V) {
	t.Helper()
	if c.Name() != "flatbuffers" {
		t.Errorf("the name is %q, want flatbuffers", c.Name())
	}
	typ := reflect.TypeFor[V]()
	if typ.Kind() != reflect.Struct {
		t.Fatalf("%v is not a struct", typ)
	}
	var names []string
	for i := range typ.NumField() {
		if f := typ.Field(i); f.IsExported() {
			names = append(names, f.Name)
		}
	}

	for n, src := range srcs {
		src = withoutID(src)
		bz, err := c.Encode(src)
		if err != nil {
			t.Fatalf("record %d: encode: %v", n, err)
		}
		again, _ := c.Encode(src)
		if !bytes.Equal(bz, again) {
			t.Fatalf("record %d: two encodings of the same record differ", n)
		}
		full, err := c.Decode(bz)
		if err != nil {
			t.Fatalf("record %d: decode: %v", n, err)
		}
		if !reflect.DeepEqual(src, full) {
			t.Fatalf("record %d: round trip\n got %+v\nwant %+v", n, full, src)
		}

		// A record that has an id is encoded without it.
		if withID, ok := setID(src); ok {
			bz2, err := c.Encode(withID)
			if err != nil || !bytes.Equal(bz, bz2) {
				t.Fatalf("record %d: the id changes the encoding (%v)", n, err)
			}
		}

		// Each field alone: it equals the one of the full decode and the others are zero.
		for _, name := range names {
			dec, ok := codec.PartialOf(c, []string{name})
			if !ok {
				t.Fatalf("record %d: no partial decoding of %s", n, name)
			}
			var got V
			if err := dec(bz, &got); err != nil {
				t.Fatalf("record %d: partial %s: %v", n, name, err)
			}
			gv, fv := reflect.ValueOf(got), reflect.ValueOf(full)
			for _, other := range names {
				g, f := gv.FieldByName(other), fv.FieldByName(other)
				if other == name {
					if !reflect.DeepEqual(g.Interface(), f.Interface()) {
						t.Fatalf("record %d: partial %s: got %v, want %v", n, name, g, f)
					}
				} else if !g.IsZero() {
					t.Fatalf("record %d: %s was decoded when %s was asked for", n, other, name)
				}
			}
		}

		// All the fields at once, a path into a field, and a repeated one.
		dec, ok := codec.PartialOf(c, append(append([]string{}, names...), names[0]+".x", names[0]))
		if !ok {
			t.Fatalf("record %d: no partial decoding of all the fields", n)
		}
		var got V
		if err := dec(bz, &got); err != nil || !reflect.DeepEqual(got, full) {
			t.Fatalf("record %d: partial of all the fields: %v\n got %+v\nwant %+v", n, err, got, full)
		}

		// No field: a decoder that does nothing. A name that is not a field: none.
		if dec, ok := codec.PartialOf(c, nil); !ok {
			t.Fatalf("no decoder for no fields")
		} else {
			var got V
			if err := dec(bz, &got); err != nil || !reflect.ValueOf(got).IsZero() {
				t.Fatalf("a decoder of no fields decoded %+v (%v)", got, err)
			}
		}
		for _, bad := range [][]string{{"Bogus"}, {names[0], "Bogus"}, {"Bogus", names[0]}, {"id"}} {
			if _, ok := codec.PartialOf(c, bad); ok {
				t.Fatalf("a decoder of %v is ok", bad)
			}
		}

		// The result does not refer to the input.
		dec, _ = codec.PartialOf(c, names)
		var partial V
		if err := dec(bz, &partial); err != nil {
			t.Fatal(err)
		}
		whole, err := c.Decode(bz)
		if err != nil {
			t.Fatal(err)
		}
		clear(bz)
		if !reflect.DeepEqual(partial, full) || !reflect.DeepEqual(whole, full) {
			t.Fatalf("record %d: the result refers to the input, which was overwritten", n)
		}
	}
	if len(srcs) == 0 {
		return
	}

	checkCorrupt(t, c, srcs, names)
	checkConcurrent(t, c, srcs, names)
}

// withoutID returns a copy of the record with a zero ID, if it has one.
func withoutID[V any](v V) V {
	rv := reflect.ValueOf(&v).Elem()
	if f := rv.FieldByName("ID"); f.IsValid() && f.CanSet() {
		f.SetZero()
	}
	return v
}

// setID returns a copy of the record with an ID of 77, if it has one that is an integer.
func setID[V any](v V) (V, bool) {
	rv := reflect.ValueOf(&v).Elem()
	if f := rv.FieldByName("ID"); f.IsValid() && f.CanSet() && f.CanInt() {
		f.SetInt(77)
		return v, true
	}
	return v, false
}

// checkCorrupt: bytes that are cut short or damaged are an error or a value, and never a panic.
func checkCorrupt[V any](t *testing.T, c codec.ValueCodec[V], srcs []V, names []string) {
	t.Helper()
	dec, _ := codec.PartialOf(c, names)
	rng := rand.New(rand.NewPCG(1, 2))
	for n, src := range srcs {
		bz, err := c.Encode(src)
		if err != nil {
			t.Fatal(err)
		}

		// Nothing, and less than the offset of the root, is an error.
		for _, cut := range []int{0, 1, 3} {
			if _, err := c.Decode(bz[:cut]); err == nil {
				t.Errorf("record %d: %d bytes decoded", n, cut)
			}
			var v V
			if err := dec(bz[:cut], &v); err == nil {
				t.Errorf("record %d: %d bytes decoded partially", n, cut)
			}
		}
		// The rest of the cuts may fail or not, but they return.
		for cut := 4; cut < len(bz); cut += 1 + len(bz)/40 {
			_, _ = c.Decode(bz[:cut])
			var v V
			_ = dec(bz[:cut], &v)
		}
		// A damaged buffer must not panic or allocate what it claims.
		for range 200 {
			damaged := bytes.Clone(bz)
			for range 1 + rng.IntN(4) {
				damaged[rng.IntN(len(damaged))] = byte(rng.IntN(256))
			}
			_, _ = c.Decode(damaged)
			var v V
			_ = dec(damaged, &v)
		}
	}
}

// checkConcurrent: the encode, the decode and the partial decode are safe for concurrent use.
func checkConcurrent[V any](t *testing.T, c codec.ValueCodec[V], srcs []V, names []string) {
	t.Helper()
	dec, _ := codec.PartialOf(c, names)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for round := range 100 {
				src := withoutID(srcs[(g+round)%len(srcs)])
				bz, err := c.Encode(src)
				if err != nil {
					t.Errorf("encode: %v", err)
					return
				}
				got, err := c.Decode(bz)
				if err != nil || !reflect.DeepEqual(got, src) {
					t.Errorf("concurrent round trip: %v\n got %+v\nwant %+v", err, got, src)
					return
				}
				var part V
				if err := dec(bz, &part); err != nil || !reflect.DeepEqual(part, src) {
					t.Errorf("concurrent partial decode: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
}
