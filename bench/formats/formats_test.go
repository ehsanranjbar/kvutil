package formats

import (
	"reflect"
	"sync"
	"testing"
	"time"

	capnp "capnproto.org/go/capnp/v3"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/bench/formats/capgen"
	"github.com/ehsanranjbar/kvutil/bench/formats/fbgen"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/codec"
)

const (
	testSeed  = 7
	testScale = 10_000
)

func strp(s string) *string { return &s }

// testOrders returns n generated orders, and then the ones that the generator does not make:
// empty strings, a note that is present and empty, zeros and negatives.
func testOrders(size dataset.ValueSize, n int) []*dataset.Order {
	spec := dataset.Spec{Seed: testSeed, Scale: testScale, Values: size}
	var os []*dataset.Order
	for id := 1; id <= n; id++ {
		os = append(os, spec.Gen(int64(id), 0))
	}
	return append(os,
		&dataset.Order{At: time.Unix(0, 0).UTC()},
		&dataset.Order{Note: strp(""), Tags: []string{""}, At: time.Unix(1704067200, 0).UTC()},
		&dataset.Order{Code: "c", Total: -5, Qty: -1, Note: strp("n"), Tags: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			Payload: []byte{0, 1, 2}, At: time.Unix(-1, 0).UTC()},
	)
}

func testBooks(size dataset.ValueSize, n int) []*dataset.Book {
	spec := dataset.Spec{Seed: testSeed, Scale: testScale, Values: size}
	var bs []*dataset.Book
	for id := 1; id <= n; id++ {
		bs = append(bs, spec.GenBook(int64(id)))
	}
	return append(bs,
		&dataset.Book{},
		&dataset.Book{Subtitle: strp(""), InPrint: true, Year: -1, Tags: []string{""}},
		&dataset.Book{Subtitle: strp("s"), Payload: []byte{9}, Score: 1 << 40},
	)
}

func testDocs(n int) []*dataset.Doc {
	v := dataset.NewVectors(testSeed, n)
	var ds []*dataset.Doc
	for id := 0; id < n; id++ {
		ds = append(ds, v.Gen(int64(id)))
	}
	return append(ds, &dataset.Doc{}, &dataset.Doc{Lang: "en", Year: 2000, Embedding: []float32{1.5}})
}

// The ids are not in the values.
func order(o *dataset.Order) dataset.Order { c := *o; c.ID = 0; return c }
func book(b *dataset.Book) dataset.Book    { c := *b; c.ID = 0; return c }
func doc(d *dataset.Doc) dataset.Doc       { c := *d; c.ID = 0; return c }

// utc normalises a time that a format decodes in another location.
func utcOrder(o dataset.Order, err error) (dataset.Order, error) { o.At = o.At.UTC(); return o, err }

func TestRoundTripOrder(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		for _, src := range testOrders(size, 50) {
			want := order(src)

			bz, err := codec.Formatted[dataset.Order](codec.Msgpack).Encode(want)
			require.NoError(t, err)
			got, err := codec.Formatted[dataset.Order](codec.Msgpack).Decode(bz)
			got, err = utcOrder(got, err)
			require.NoError(t, err)
			require.Equal(t, want, got, "msgpack")

			bin := OrderBin(want)
			bz, err = bin.MarshalBinary()
			require.NoError(t, err)
			var gotBin OrderBin
			require.NoError(t, gotBin.UnmarshalBinary(bz))
			require.Equal(t, want, dataset.Order(gotBin), "binary")

			bz = EncodeOrderFB(flatbuffers.NewBuilder(0), src)
			got, err = DecodeOrderFB(bz)
			require.NoError(t, err)
			require.Equal(t, want, got, "flatbuffers")

			for _, w := range []*CapnpWriter{{}, {Reuse: true}} {
				bz, err = w.EncodeOrder(src)
				require.NoError(t, err)
				msg, err := capnp.Unmarshal(bz)
				require.NoError(t, err)
				got, err = DecodeOrderCapnp(msg)
				require.NoError(t, err)
				require.Equal(t, want, got, "capnp")
			}

			bz, err = (&CapnpWriter{Packed: true}).EncodeOrder(src)
			require.NoError(t, err)
			msg, err := capnp.UnmarshalPacked(bz)
			require.NoError(t, err)
			got, err = DecodeOrderCapnp(msg)
			require.NoError(t, err)
			require.Equal(t, want, got, "capnp packed")
		}
	}
}

func TestRoundTripBook(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		for _, src := range testBooks(size, 50) {
			want := book(src)

			bz, err := codec.Formatted[dataset.Book](codec.Msgpack).Encode(want)
			require.NoError(t, err)
			got, err := codec.Formatted[dataset.Book](codec.Msgpack).Decode(bz)
			require.NoError(t, err)
			require.Equal(t, want, got, "msgpack")

			bz = EncodeBookFB(flatbuffers.NewBuilder(0), src)
			got, err = DecodeBookFB(bz)
			require.NoError(t, err)
			require.Equal(t, want, got, "flatbuffers")

			for _, w := range []*CapnpWriter{{}, {Reuse: true}} {
				bz, err = w.EncodeBook(src)
				require.NoError(t, err)
				msg, err := capnp.Unmarshal(bz)
				require.NoError(t, err)
				got, err = DecodeBookCapnp(msg)
				require.NoError(t, err)
				require.Equal(t, want, got, "capnp")
			}

			bz, err = (&CapnpWriter{Packed: true}).EncodeBook(src)
			require.NoError(t, err)
			msg, err := capnp.UnmarshalPacked(bz)
			require.NoError(t, err)
			got, err = DecodeBookCapnp(msg)
			require.NoError(t, err)
			require.Equal(t, want, got, "capnp packed")
		}
	}
}

func TestRoundTripDoc(t *testing.T) {
	for _, src := range testDocs(50) {
		want := doc(src)

		bz, err := codec.Formatted[dataset.Doc](codec.Msgpack).Encode(want)
		require.NoError(t, err)
		got, err := codec.Formatted[dataset.Doc](codec.Msgpack).Decode(bz)
		require.NoError(t, err)
		require.Equal(t, want, got, "msgpack")

		bz = EncodeDocFB(flatbuffers.NewBuilder(0), src)
		got, err = DecodeDocFB(bz)
		require.NoError(t, err)
		require.Equal(t, want, got, "flatbuffers")

		for _, w := range []*CapnpWriter{{}, {Reuse: true}} {
			bz, err = w.EncodeDoc(src)
			require.NoError(t, err)
			msg, err := capnp.Unmarshal(bz)
			require.NoError(t, err)
			got, err = DecodeDocCapnp(msg)
			require.NoError(t, err)
			require.Equal(t, want, got, "capnp")
		}

		bz, err = (&CapnpWriter{Packed: true}).EncodeDoc(src)
		require.NoError(t, err)
		msg, err := capnp.UnmarshalPacked(bz)
		require.NoError(t, err)
		got, err = DecodeDocCapnp(msg)
		require.NoError(t, err)
		require.Equal(t, want, got, "capnp packed")
	}
}

// The decoders copy: the result stays the same when the input is overwritten.
func TestDecodeCopiesOutOfTheInput(t *testing.T) {
	src := testOrders(dataset.Large, 1)[0]
	src.Note = strp("a note")
	want := order(src)

	bz := EncodeOrderFB(flatbuffers.NewBuilder(0), src)
	got, err := DecodeOrderFB(bz)
	require.NoError(t, err)
	clear(bz)
	require.Equal(t, want, got, "flatbuffers")

	bz, err = (&CapnpWriter{}).EncodeOrder(src)
	require.NoError(t, err)
	msg, err := capnp.Unmarshal(bz)
	require.NoError(t, err)
	got, err = DecodeOrderCapnp(msg)
	require.NoError(t, err)
	clear(bz)
	require.Equal(t, want, got, "capnp")

	d := testDocs(1)[0]
	wantDoc := doc(d)
	bz = EncodeDocFB(flatbuffers.NewBuilder(0), d)
	gotDoc, err := DecodeDocFB(bz)
	require.NoError(t, err)
	clear(bz)
	require.Equal(t, wantDoc, gotDoc, "flatbuffers doc")
}

// The native accessors read the same values as the source, without building the record.
func TestNativeReads(t *testing.T) {
	var rd CapnpReader
	for _, src := range testOrders(dataset.Small, 20) {
		bz := EncodeOrderFB(flatbuffers.NewBuilder(0), src)
		o := fbgen.GetRootAsOrder(bz, 0)
		require.Equal(t, src.Total, o.Total())
		require.Equal(t, src.Customer, string(o.Customer()))

		bz, err := (&CapnpWriter{}).EncodeOrder(src)
		require.NoError(t, err)
		for _, open := range []func([]byte) (*capnp.Message, error){capnp.Unmarshal, rd.Open} {
			msg, err := open(bz)
			require.NoError(t, err)
			c, err := capgen.ReadRootOrder(msg)
			require.NoError(t, err)
			require.Equal(t, src.Total, c.Total())
			customer, err := c.Customer()
			require.NoError(t, err)
			require.Equal(t, src.Customer, customer)
		}
	}
	for _, d := range testDocs(5) {
		bz, err := (&CapnpWriter{}).EncodeDoc(d)
		require.NoError(t, err)
		msg, err := rd.Open(bz)
		require.NoError(t, err)
		c, err := capgen.ReadRootDoc(msg)
		require.NoError(t, err)
		l, err := c.Embedding()
		require.NoError(t, err)
		require.Equal(t, len(d.Embedding), l.Len())
		for i, f := range d.Embedding {
			require.Equal(t, f, l.At(i))
		}
	}
}

// The reader reuses one message for all of the encodings, and an encoding that is not valid is an
// error and not a panic.
func TestCapnpReaderReuse(t *testing.T) {
	var rd CapnpReader
	os := testOrders(dataset.Small, 5)
	var bzs [][]byte
	for _, o := range os {
		bz, err := (&CapnpWriter{}).EncodeOrder(o)
		require.NoError(t, err)
		bzs = append(bzs, bz)
	}
	for round := 0; round < 2; round++ {
		for i, bz := range bzs {
			msg, err := rd.Open(bz)
			require.NoError(t, err)
			got, err := DecodeOrderCapnp(msg)
			require.NoError(t, err)
			require.Equal(t, order(os[i]), got)
		}
	}
	_, err := rd.Open(nil)
	require.Error(t, err)
}

func TestCorruptFlatBuffer(t *testing.T) {
	bz := EncodeOrderFB(flatbuffers.NewBuilder(0), testOrders(dataset.Small, 1)[0])
	_, err := DecodeOrderFB(bz[:6])
	require.Error(t, err)
}

// ---------------------------------------------------------------------------------------------
// The record types of the codec.Binary codecs, and their partial decoding.

// recordPaths are the names of the fields of a record that are in its encoding (all but the ID).
func recordPaths[V any]() []string {
	t := reflect.TypeFor[V]()
	var ps []string
	for i := range t.NumField() {
		if n := t.Field(i).Name; n != "ID" {
			ps = append(ps, n)
		}
	}
	return ps
}

func convertAll[V, S any](srcs []S, conv func(S) V) []V {
	out := make([]V, len(srcs))
	for i, s := range srcs {
		out[i] = conv(s)
	}
	return out
}

// testBinaryRecord checks a record type through codec.Binary: the round trip, and the partial
// decoding of each field alone (the field equals the one of the full decode and the others are
// zero), of all the fields at once, of a path that is not known (everything is decoded), and that
// the result does not refer to the input.
func testBinaryRecord[V any](t *testing.T, c codec.ValueCodec[V], srcs []V) {
	paths := recordPaths[V]()
	for _, src := range srcs {
		bz, err := c.Encode(src)
		require.NoError(t, err)
		full, err := c.Decode(bz)
		require.NoError(t, err)
		require.Equal(t, src, full, "round trip")

		for _, path := range paths {
			dec, ok := codec.PartialOf(c, []string{path})
			require.True(t, ok, path)
			var got V
			require.NoError(t, dec(bz, &got), path)
			gv, fv := reflect.ValueOf(&got).Elem(), reflect.ValueOf(&full).Elem()
			for i := range gv.NumField() {
				name := gv.Type().Field(i).Name
				if name == path {
					require.Equal(t, fv.Field(i).Interface(), gv.Field(i).Interface(), path)
				} else {
					require.True(t, gv.Field(i).IsZero(), "%s decoded when only %s is asked for", name, path)
				}
			}
		}

		dec, ok := codec.PartialOf(c, paths)
		require.True(t, ok)
		var got V
		require.NoError(t, dec(bz, &got))
		require.Equal(t, full, got, "all the paths")

		dec, ok = codec.PartialOf(c, []string{"Bogus"})
		require.True(t, ok)
		got = *new(V)
		require.NoError(t, dec(bz, &got))
		require.Equal(t, full, got, "an unknown path decodes everything")

		dec, ok = codec.PartialOf(c, []string{paths[len(paths)-1], "Bogus"})
		require.True(t, ok)
		got = *new(V)
		require.NoError(t, dec(bz, &got))
		require.Equal(t, full, got, "an unknown path after a known one")

		// The result does not refer to the input.
		// (full was decoded before, and a decode copies.)
		dec, _ = codec.PartialOf(c, paths)
		got = *new(V)
		require.NoError(t, dec(bz, &got))
		clear(bz)
		require.Equal(t, full, got, "the input is overwritten")
	}
}

func TestPartialOrder(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		srcs := convertAll(testOrders(size, 20), func(o *dataset.Order) dataset.Order { return order(o) })
		testBinaryRecord(t, codec.Binary[OrderBin](), convertAll(srcs, func(o dataset.Order) OrderBin { return OrderBin(o) }))
		testBinaryRecord(t, codec.Binary[OrderFB](), convertAll(srcs, func(o dataset.Order) OrderFB { return OrderFB(o) }))
		testBinaryRecord(t, codec.Binary[OrderCapnp](), convertAll(srcs, func(o dataset.Order) OrderCapnp { return OrderCapnp(o) }))
		testBinaryRecord(t, codec.Binary[OrderCapnpReuse](), convertAll(srcs, func(o dataset.Order) OrderCapnpReuse { return OrderCapnpReuse(o) }))
	}
}

func TestPartialBook(t *testing.T) {
	for _, size := range []dataset.ValueSize{dataset.Small, dataset.Large} {
		srcs := convertAll(testBooks(size, 20), func(b *dataset.Book) dataset.Book { return book(b) })
		testBinaryRecord(t, codec.Binary[BookFB](), convertAll(srcs, func(b dataset.Book) BookFB { return BookFB(b) }))
		testBinaryRecord(t, codec.Binary[BookCapnp](), convertAll(srcs, func(b dataset.Book) BookCapnp { return BookCapnp(b) }))
		testBinaryRecord(t, codec.Binary[BookCapnpReuse](), convertAll(srcs, func(b dataset.Book) BookCapnpReuse { return BookCapnpReuse(b) }))
	}
}

func TestPartialDoc(t *testing.T) {
	srcs := convertAll(testDocs(20), func(d *dataset.Doc) dataset.Doc { return doc(d) })
	testBinaryRecord(t, codec.Binary[DocFB](), convertAll(srcs, func(d dataset.Doc) DocFB { return DocFB(d) }))
	testBinaryRecord(t, codec.Binary[DocCapnp](), convertAll(srcs, func(d dataset.Doc) DocCapnp { return DocCapnp(d) }))
	testBinaryRecord(t, codec.Binary[DocCapnpReuse](), convertAll(srcs, func(d dataset.Doc) DocCapnpReuse { return DocCapnpReuse(d) }))
}

// A truncated or corrupt encoding is an error and not a panic, in a partial decode too.
func TestPartialCorrupt(t *testing.T) {
	src := OrderFB(order(testOrders(dataset.Small, 1)[0]))
	cf := codec.Binary[OrderFB]()
	bz, err := cf.Encode(src)
	require.NoError(t, err)
	decF, _ := codec.PartialOf(cf, []string{"Total"})
	var gotF OrderFB
	require.Error(t, decF(bz[:6], &gotF))

	cc := codec.Binary[OrderCapnp]()
	bz, err = cc.Encode(OrderCapnp(src))
	require.NoError(t, err)
	dec, _ := codec.PartialOf(cc, []string{"Total"})
	var gotC OrderCapnp
	require.Error(t, dec(bz[:5], &gotC))

	cb := codec.Binary[OrderBin]()
	bz, err = cb.Encode(OrderBin(src))
	require.NoError(t, err)
	dec2, _ := codec.PartialOf(cb, []string{"Payload"})
	var gotB OrderBin
	require.Error(t, dec2(bz[:len(bz)-1], &gotB))
}

// The pooled readers of the Reuse types are safe for concurrent use, and one decode does not
// disturb another.
func TestPartialReuseConcurrent(t *testing.T) {
	c := codec.Binary[OrderCapnpReuse]()
	srcs := convertAll(testOrders(dataset.Small, 16), func(o *dataset.Order) OrderCapnpReuse { return OrderCapnpReuse(order(o)) })
	var bzs [][]byte
	for _, s := range srcs {
		bz, err := c.Encode(s)
		require.NoError(t, err)
		bzs = append(bzs, bz)
	}
	dec, ok := codec.PartialOf(c, []string{"Customer", "Total"})
	require.True(t, ok)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for round := range 200 {
				i := round % len(bzs)
				var got OrderCapnpReuse
				if err := dec(bzs[i], &got); err != nil || got.Total != srcs[i].Total || got.Customer != srcs[i].Customer {
					t.Errorf("record %d: %v %v", i, err, got)
					return
				}
				full, err := c.Decode(bzs[i])
				if err != nil || !reflect.DeepEqual(full, srcs[i]) {
					t.Errorf("record %d: full decode differs: %v", i, err)
					return
				}
			}
		})
	}
	wg.Wait()
}
