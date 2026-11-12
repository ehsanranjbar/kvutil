package flat_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat"
	"github.com/ehsanranjbar/kvutil/bench/internal/flat/flattest"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/store/record"
)

func strp(s string) *string { return &s }

// orders returns generated orders (the generator's times, at whole seconds in UTC), and then the
// ones that it does not make: empty strings, a note that is present and empty, negatives, and
// many tags.
func orders(size dataset.ValueSize, n int) []dataset.Order {
	spec := dataset.Spec{Seed: 7, Scale: 10_000, Values: size}
	var os []dataset.Order
	for id := 1; id <= n; id++ {
		os = append(os, *spec.Gen(int64(id), 0))
	}
	return append(os,
		dataset.Order{At: time.Unix(0, 0).UTC()},
		dataset.Order{Note: strp(""), Tags: []string{""}, At: time.Unix(1704067200, 0).UTC()},
		dataset.Order{Code: "c", Total: -5, Qty: -1, Note: strp("n"),
			Tags:    []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			Payload: []byte{0, 1, 2}, At: time.Unix(-1, 0).UTC()},
		dataset.Order{At: time.Unix(0, 123456789).UTC()}, // below the second
	)
}

func books(size dataset.ValueSize, n int) []dataset.Book {
	spec := dataset.Spec{Seed: 7, Scale: 10_000, Values: size}
	var bs []dataset.Book
	for id := 1; id <= n; id++ {
		bs = append(bs, *spec.GenBook(int64(id)))
	}
	return append(bs,
		dataset.Book{},
		dataset.Book{Subtitle: strp(""), InPrint: true, Year: -1, Tags: []string{""}},
		dataset.Book{Subtitle: strp("s"), Payload: []byte{9}, Score: 1 << 40},
	)
}

func docs(n int) []dataset.Doc {
	v := dataset.NewVectors(7, n)
	var ds []dataset.Doc
	for id := 0; id < n; id++ {
		ds = append(ds, *v.Gen(int64(id)))
	}
	return append(ds, dataset.Doc{}, dataset.Doc{Lang: "en", Year: 2000, Embedding: []float32{1.5}})
}

func events(n int) []dataset.Event {
	spec := dataset.EventSpec{Seed: 7, Events: 5000, End: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), SpanDays: 365}
	var es []dataset.Event
	for id := 1; id <= n; id++ {
		es = append(es, *spec.Gen(int64(id), 0))
	}
	// (The zero time.Time is not in the range of Unix nanoseconds: the generators never make it.)
	return append(es, dataset.Event{At: time.Unix(0, 0).UTC()}, dataset.Event{Customer: "c", Amount: -1, Kind: "k", At: time.Unix(1, 2).UTC()})
}

func TestOrder(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		flattest.Check(t, flat.Order(), orders(size, 20))
	}
}

func TestBook(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		flattest.Check(t, flat.Book(), books(size, 20))
	}
}

func TestDoc(t *testing.T) {
	flattest.Check(t, flat.Doc(), docs(20))
}

func TestEvent(t *testing.T) {
	flattest.Check(t, flat.Event(), events(20))
}

// The nil of a *string is an absent field, and an empty string is a present one.
func TestNilAndEmptyNote(t *testing.T) {
	c := flat.Order()
	for _, note := range []*string{nil, strp(""), strp("x")} {
		bz, err := c.Encode(dataset.Order{Note: note})
		if err != nil {
			t.Fatal(err)
		}
		got, err := c.Decode(bz)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Note, note) {
			t.Errorf("note %v came back as %v", note, got.Note)
		}
		dec, _ := codec.PartialOf(c, []string{"Note"})
		var part dataset.Order
		if err := dec(bz, &part); err != nil || !reflect.DeepEqual(part.Note, note) {
			t.Errorf("note %v came back from a partial decode as %v (%v)", note, part.Note, err)
		}
	}
}

// The partial decoding asked for the single field the benchmarks and the queries use is there.
func TestPartialOf(t *testing.T) {
	if _, ok := codec.PartialOf(flat.Order(), []string{"Total"}); !ok {
		t.Error("no partial decoding of Order.Total")
	}
	// The id is not in the value: a store sets it, so asking for it is fine and does nothing.
	dec, ok := codec.PartialOf(flat.Order(), []string{"ID"})
	if !ok {
		t.Fatal("no partial decoding of Order.ID")
	}
	bz := flat.EncodeOrder(&dataset.Order{ID: 5, Code: "x"})
	var o dataset.Order
	if err := dec(bz, &o); err != nil || !reflect.ValueOf(o).IsZero() {
		t.Errorf("ID decoded %+v (%v)", o, err)
	}
	if _, ok := codec.PartialOf(flat.Order(), []string{"Nope"}); ok {
		t.Error("a field that the record has not is ok")
	}
}

func TestCorruptIsErrCorrupt(t *testing.T) {
	bz := flat.EncodeOrder(&dataset.Order{Code: "x", Tags: []string{"a"}})
	if _, err := flat.Order().Decode(bz[:6]); !errors.Is(err, flat.ErrCorrupt) {
		t.Errorf("a truncated record gives %v", err)
	}
	if _, err := flat.DecodeOrder(nil, 1); !errors.Is(err, flat.ErrCorrupt) {
		t.Errorf("no bytes give %v", err)
	}
}

// The raw helpers stand in for a record: the id goes to the decode, not the bytes.
func TestEncodeDecodeOrder(t *testing.T) {
	for _, o := range orders(dataset.Small, 5) {
		o.ID = 9
		bz := flat.EncodeOrder(&o)
		got, err := flat.DecodeOrder(bz, 9)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*got, o) {
			t.Errorf("got %+v, want %+v", *got, o)
		}
		other, err := flat.DecodeOrder(bz, 10)
		if err != nil || other.ID != 10 {
			t.Errorf("the id of the decode is %v (%v)", other, err)
		}
	}
}

// Encoding does not allocate a builder: the allocations of a small order are the copy of the
// bytes and the offsets of a vector that does not fit the stack.
func TestEncodeAllocations(t *testing.T) {
	o := orders(dataset.Small, 1)[0]
	n := testing.AllocsPerRun(100, func() { _ = flat.EncodeOrder(&o) })
	if n > 2 {
		t.Errorf("an encode makes %v allocations", n)
	}
}

// A store of records with the codec: the id comes from the key, in a Get, an iteration and a
// stored record that is decoded in part.
func TestRecordStore(t *testing.T) {
	sys := testutil.Kvutil(t)
	db, err := sys.Open("flat")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Destroy()

	st := record.NewWithCodecs[int64, dataset.Order, *dataset.Order]("orders", nil, flat.Order())
	src := orders(dataset.Small, 30)
	for i := range src {
		src[i].ID = int64(i + 1)
	}
	err = db.Update(func(tx kv.Tx) error {
		in := st.Instantiate(tx)
		for i := range src {
			if err := in.Set(&src[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx kv.Tx) error {
		in := st.Instantiate(tx)
		for i := range src {
			got, err := in.Get(src[i].ID)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(*got, src[i]) {
				t.Errorf("get %d: %+v, want %+v", src[i].ID, *got, src[i])
			}
		}

		it, err := in.All()
		if err != nil {
			return err
		}
		defer it.Close()
		n := 0
		for ; it.Valid(); it.Next() {
			v, err := it.Value()
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(*v, src[n]) {
				t.Errorf("iterated %d: %+v, want %+v", n, *v, src[n])
			}
			n++
		}
		if n != len(src) {
			t.Errorf("%d records iterated, want %d", n, len(src))
		}

		stored, err := in.GetStored(src[3].ID)
		if err != nil {
			return err
		}
		dec, ok := codec.PartialOf(flat.Order(), []string{"ID", "Total"})
		if !ok {
			t.Fatal("no partial decoding")
		}
		var into dataset.Order
		part, err := stored.Partial(dec, &into)
		if err != nil {
			return err
		}
		want := dataset.Order{ID: src[3].ID, Total: src[3].Total}
		if !reflect.DeepEqual(*part, want) {
			t.Errorf("partial %+v, want %+v", *part, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
