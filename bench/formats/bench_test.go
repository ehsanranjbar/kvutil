package formats

// Benchmarks of the formats that a stored record of kvutil can be in: msgpack (the default of
// kvutil, codec.Formatted[T](codec.Msgpack)), the binary codec written by hand (Order only,
// codec.Binary[OrderBin]), FlatBuffers and Cap'n Proto.
//
// Run them with
//
//	go test -run '^$' -bench . -benchmem -count 10 -timeout 60m | tee out.txt
//	benchstat -col /format -row /type,/op,/how out.txt
//
// The names are BenchmarkFormats/type=T/op=O/how=H/format=F, so that benchstat can pivot on any of
// them. The types are order-small, order-large (the order with a payload of 4096 bytes), book and
// doc (an embedding of 384 floats). The ops are
//
//	encode                      the record to bytes (bytes/rec is the average size)
//	decode-full                 the bytes to the record
//	read-scalar                 Order.Total, Book.Year
//	read-string                 Order.Customer, Book.Title
//	read-scalar-near-embedding  Doc.Year, a field that lies next to the embedding (doc only; it is
//	                            the only scalar of a doc, so there is no read-scalar of it)
//	read-embedding              the sum of the 384 floats of Doc.Embedding
//
// and how is "full" for what builds the record of Go (every encode, and a read that decodes the
// record and then reads the field), and "native" for what reads the bytes with the accessors of
// the format and builds nothing. Msgpack and binary have only "full" and "partial".
//
// "partial" is kvutil's partial decoding, which is what a query does per record that it rejects
// on a field: the decoder is prepared once, codec.PartialOf(c, []string{"Total"}), and then each
// record is a zeroed scratch value, dec(bz, &v) and the read of the field. The codecs are the
// ones that a store is given: codec.Formatted[T](codec.Msgpack) (a narrower struct that skips the
// other fields of the map), codec.Binary[OrderBin] (walks and skips the fields before the one it
// wants), and codec.Binary[OrderFB], codec.Binary[OrderCapnp] and codec.Binary[OrderCapnpReuse]
// (partial.go: the accessor of the format straight from the bytes, and a copy). capnp does
// capnp.Unmarshal per record, as a store has to; capnp-reuse gets its message from a sync.Pool.
// A setup fails the benchmark if PartialOf has no decoder, or if a decoded field differs from the
// source, so that a row cannot silently be a full decode.
//
// The full rows of flatbuffers and capnp call the decoders of convert.go. They are not repeated
// through codec.Binary: its Decode is those decoders (capnp.Unmarshal for capnp) and one copy of
// the struct, which is the same work.
//
// The rules that keep it fair:
//
//  1. Every format encodes the very same records: N=256 of the generators of bench/internal/dataset
//     (the same seed and ids), and the loop of a benchmark cycles through them.
//  2. A builder or an arena is reused where that is the idiom of the format, and then the name of
//     the row says so: flatbuffers-reuse (one Builder, Reset between records, and a copy of the
//     result because the caller must own it) against flatbuffers (a new Builder of 1 KB per
//     record, whose bytes are returned as they are); capnp-reuse (one Message whose pooled arena
//     is released and taken again, and, for reading, one Message over one read-only arena, see
//     CapnpReader) against capnp (a new Message per record, and capnp.Unmarshal per read).
//     capnp-packed is the packed stream, for the size and the full decode only. Msgpack and
//     binary have no state to reuse.
//  3. A full decode copies out of the input, as the ValueCodec of kvutil says: the result does not
//     refer to the given slice. A native read does not copy: that is what it is for. The native
//     read of a string is the zero-copy accessor of both (FlatBuffers Customer() and Cap'n Proto
//     CustomerBytes(), both []byte), not the one of Cap'n Proto that returns a string and copies.
//  4. Every result is consumed into a package-level sink, so that nothing is optimized away.
//
// Every benchmark calls b.ReportAllocs.

import (
	"fmt"
	"testing"

	capnp "capnproto.org/go/capnp/v3"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/ehsanranjbar/kvutil/bench/formats/capgen"
	"github.com/ehsanranjbar/kvutil/bench/formats/fbgen"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/codec"
)

// benchRecords is the number of records that a benchmark cycles through.
const benchRecords = 256

// The sinks.
var (
	sinkInt   int
	sinkStr   string
	sinkBytes []byte
	sinkF     float32
	sinkOrder dataset.Order
	sinkBook  dataset.Book
	sinkDoc   dataset.Doc
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// benchCase is one row: setup does what is not measured (it makes the encoder, and encodes the
// records to read) and returns what runs once per iteration, on the i-th record, and returns a
// number that is consumed (the size of an encoding, or what was read).
type benchCase struct {
	op, how, format string
	setup           func() (step func(i int) int)
}

// setupFailure is what a setup panics with (see failSetup) to fail its benchmark.
type setupFailure string

func failSetup(format string, args ...any) { panic(setupFailure(fmt.Sprintf(format, args...))) }

// start runs the setup, and fails the benchmark if the setup says so.
func (c benchCase) start(b *testing.B) func(int) int {
	defer func() {
		if r := recover(); r != nil {
			if f, ok := r.(setupFailure); ok {
				b.Fatal(string(f))
			}
			panic(r)
		}
	}()
	return c.setup()
}

// The ops.
const (
	opEncode     = "encode"
	opDecodeFull = "decode-full"
	opScalar     = "read-scalar"
	opString     = "read-string"
	opNearEmb    = "read-scalar-near-embedding"
	opEmbedding  = "read-embedding"
)

// The values of how.
const (
	howFull    = "full"
	howNative  = "native"
	howPartial = "partial"
)

// fullFormat is a format with the functions that make and read the record of Go. newEnc and
// newDec make a function with its own state (a builder, a message) for each row.
type fullFormat[T any] struct {
	format string
	newEnc func() func(*T) ([]byte, error)
	newDec func() func([]byte) (T, error)
	// What the format is measured in: the encode, the decode-full, and the reads.
	enc, dec, reads bool
}

// reads is how to read a field of the record of Go.
type reads[T any] struct {
	sink      *T
	scalarOp  string
	scalar    func(*T) int
	str       func(*T) string  // nil if the type has no string to read
	embedding func(*T) float32 // nil if the type has no embedding
	// The Go struct fields that the reads read, which are what a partial decoding is asked for.
	scalarField, strField, embField string
}

// partialFormat is a codec that a store is given and that can decode some fields: the rows of
// how=partial. T is the record of the dataset and the codec is of a type with its underlying type.
type partialFormat[T any] struct {
	format string
	// from is the format whose encoding the codec reads (the format of group.formats).
	from string
	// prepare returns the decoder of the fields, or false if the codec has none.
	prepare func(fields []string) (func(bz []byte, v *T) error, bool)
}

// partialOf is the row of a codec of V, where conv views a *T as a *V.
func partialOf[T, V any](format, from string, c codec.ValueCodec[V], conv func(*T) *V) partialFormat[T] {
	return partialFormat[T]{format, from, func(fields []string) (func([]byte, *T) error, bool) {
		dec, ok := codec.PartialOf(c, fields)
		if !ok {
			return nil, false
		}
		return func(bz []byte, v *T) error { return dec(bz, conv(v)) }, true
	}}
}

// native is how to read a field from the bytes of FlatBuffers and of Cap'n Proto.
type native struct {
	fbScalar  func(bz []byte) int
	fbStr     func(bz []byte) []byte
	fbEmb     func(bz []byte) float32
	capScalar func(m *capnp.Message) int
	capStr    func(m *capnp.Message) []byte
	capEmb    func(m *capnp.Message) float32
}

// group is the rows of a type.
type group[T any] struct {
	name    string
	vals    []T
	formats []fullFormat[T]
	reads   reads[T]
	native  native
	// capnpReaders are the rows that read natively from a message, by the format that opens it.
	capnpReaders []capnpReader
	// partials are the rows of how=partial.
	partials []partialFormat[T]

	encoded map[string][][]byte
}

type capnpReader struct {
	format string
	open   func() func([]byte) (*capnp.Message, error)
}

// encodedBy returns the records encoded by the format, once.
func (g *group[T]) encodedBy(f fullFormat[T]) [][]byte {
	if g.encoded == nil {
		g.encoded = map[string][][]byte{}
	}
	if bzs, ok := g.encoded[f.format]; ok {
		return bzs
	}
	enc := f.newEnc()
	bzs := make([][]byte, len(g.vals))
	for i := range g.vals {
		bz, err := enc(&g.vals[i])
		must(err)
		bzs[i] = bz
	}
	g.encoded[f.format] = bzs
	return bzs
}

// encodedNative returns the records as FlatBuffers or Cap'n Proto encodes them.
func (g *group[T]) encodedNative(format string) [][]byte {
	for _, f := range g.formats {
		if f.format == format {
			return g.encodedBy(f)
		}
	}
	panic("no format " + format)
}

func (g *group[T]) cases() []benchCase {
	var cs []benchCase
	r := g.reads
	for _, f := range g.formats {
		if f.enc {
			cs = append(cs, benchCase{opEncode, howFull, f.format, func() func(int) int {
				enc := f.newEnc()
				return func(i int) int {
					bz, err := enc(&g.vals[i])
					must(err)
					sinkBytes = bz
					return len(bz)
				}
			}})
		}
		if f.dec {
			cs = append(cs, benchCase{opDecodeFull, howFull, f.format, func() func(int) int {
				bzs, dec := g.encodedBy(f), f.newDec()
				return func(i int) int {
					v, err := dec(bzs[i])
					must(err)
					*r.sink = v
					return 0
				}
			}})
		}
		if !f.reads {
			continue
		}
		cs = append(cs, benchCase{r.scalarOp, howFull, f.format, func() func(int) int {
			bzs, dec := g.encodedBy(f), f.newDec()
			return func(i int) int {
				v, err := dec(bzs[i])
				must(err)
				return r.scalar(&v)
			}
		}})
		if r.str != nil {
			cs = append(cs, benchCase{opString, howFull, f.format, func() func(int) int {
				bzs, dec := g.encodedBy(f), f.newDec()
				return func(i int) int {
					v, err := dec(bzs[i])
					must(err)
					sinkStr = r.str(&v)
					return len(sinkStr)
				}
			}})
		}
		if r.embedding != nil {
			cs = append(cs, benchCase{opEmbedding, howFull, f.format, func() func(int) int {
				bzs, dec := g.encodedBy(f), f.newDec()
				return func(i int) int {
					v, err := dec(bzs[i])
					must(err)
					sinkF = r.embedding(&v)
					return 0
				}
			}})
		}
	}

	// The native reads, of FlatBuffers and of Cap'n Proto with a message per record (capnp) and
	// with the message reused (capnp-reuse).
	n := g.native
	cs = append(cs, benchCase{r.scalarOp, howNative, "flatbuffers", func() func(int) int {
		bzs := g.encodedNative("flatbuffers")
		return func(i int) int { return n.fbScalar(bzs[i]) }
	}})
	if r.str != nil {
		cs = append(cs, benchCase{opString, howNative, "flatbuffers", func() func(int) int {
			bzs := g.encodedNative("flatbuffers")
			return func(i int) int { sinkBytes = n.fbStr(bzs[i]); return len(sinkBytes) }
		}})
	}
	if r.embedding != nil {
		cs = append(cs, benchCase{opEmbedding, howNative, "flatbuffers", func() func(int) int {
			bzs := g.encodedNative("flatbuffers")
			return func(i int) int { sinkF = n.fbEmb(bzs[i]); return 0 }
		}})
	}
	for _, cr := range g.capnpReaders {
		cs = append(cs, benchCase{r.scalarOp, howNative, cr.format, func() func(int) int {
			bzs, open := g.encodedNative("capnp"), cr.open()
			return func(i int) int {
				m, err := open(bzs[i])
				must(err)
				return n.capScalar(m)
			}
		}})
		if r.str != nil {
			cs = append(cs, benchCase{opString, howNative, cr.format, func() func(int) int {
				bzs, open := g.encodedNative("capnp"), cr.open()
				return func(i int) int {
					m, err := open(bzs[i])
					must(err)
					sinkBytes = n.capStr(m)
					return len(sinkBytes)
				}
			}})
		}
		if r.embedding != nil {
			cs = append(cs, benchCase{opEmbedding, howNative, cr.format, func() func(int) int {
				bzs, open := g.encodedNative("capnp"), cr.open()
				return func(i int) int {
					m, err := open(bzs[i])
					must(err)
					sinkF = n.capEmb(m)
					return 0
				}
			}})
		}
	}
	return append(cs, g.partialCases()...)
}

// partialCases are the rows of how=partial: for each codec and each read, the decoder of the
// field is prepared once, and a record is a zeroed scratch value, a partial decode and the read.
func (g *group[T]) partialCases() []benchCase {
	r := g.reads
	type fieldRead struct {
		op, field string
		read      func(*T) int
		same      func(a, b *T) bool
	}
	ops := []fieldRead{{r.scalarOp, r.scalarField,
		r.scalar,
		func(a, b *T) bool { return r.scalar(a) == r.scalar(b) }}}
	if r.str != nil {
		ops = append(ops, fieldRead{opString, r.strField,
			func(v *T) int { sinkStr = r.str(v); return len(sinkStr) },
			func(a, b *T) bool { return r.str(a) == r.str(b) }})
	}
	if r.embedding != nil {
		ops = append(ops, fieldRead{opEmbedding, r.embField,
			func(v *T) int { sinkF = r.embedding(v); return 0 },
			func(a, b *T) bool { return r.embedding(a) == r.embedding(b) }})
	}

	var cs []benchCase
	for _, p := range g.partials {
		for _, o := range ops {
			cs = append(cs, benchCase{o.op, howPartial, p.format, func() func(int) int {
				dec, ok := p.prepare([]string{o.field})
				if !ok {
					failSetup("codec.PartialOf has no decoder of %s for %s: the row would not be a partial decode", o.field, p.format)
				}
				bzs := g.encodedNative(p.from)
				for i, bz := range bzs { // the decoder reads the field that the row says it does
					var got T
					must(dec(bz, &got))
					if !o.same(&got, &g.vals[i]) {
						failSetup("%s of %s: the partial decode of record %d differs from the source", o.field, p.format, i)
					}
				}
				var v, zero T
				return func(i int) int {
					v = zero
					must(dec(bzs[i], &v))
					return o.read(&v)
				}
			}})
		}
	}
	return cs
}

// runner is a group seen as its name and rows.
type runner interface {
	name() string
	cases() []benchCase
}

type namedGroup struct {
	n  string
	cs func() []benchCase
}

func (g namedGroup) name() string       { return g.n }
func (g namedGroup) cases() []benchCase { return g.cs() }

func BenchmarkFormats(b *testing.B) {
	for _, g := range []runner{
		orderGroup("order-small", dataset.Small),
		orderGroup("order-large", dataset.Large),
		bookGroup(),
		docGroup(),
	} {
		b.Run("type="+g.name(), func(b *testing.B) {
			for _, c := range g.cases() {
				b.Run("op="+c.op+"/how="+c.how+"/format="+c.format, func(b *testing.B) {
					step := c.start(b)
					b.ReportAllocs()
					if c.op == opEncode {
						total := 0
						for i := 0; i < benchRecords; i++ {
							total += step(i)
						}
						defer b.ReportMetric(float64(total)/benchRecords, "bytes/rec")
					}
					b.ResetTimer()
					total, j := 0, 0
					for i := 0; i < b.N; i++ {
						total += step(j)
						if j++; j == benchRecords {
							j = 0
						}
					}
					sinkInt = total
				})
			}
		})
	}
}

// The openers of the messages of Cap'n Proto.
var (
	openCapnp = func() func([]byte) (*capnp.Message, error) { return capnp.Unmarshal }
	openReuse = func() func([]byte) (*capnp.Message, error) { return new(CapnpReader).Open }
	openPack  = func() func([]byte) (*capnp.Message, error) { return capnp.UnmarshalPacked }
)

// capnpFormats are the rows of Cap'n Proto of a type, from the functions that write and read it.
func capnpFormats[T any](
	encode func(*CapnpWriter, *T) ([]byte, error),
	decode func(*capnp.Message) (T, error),
) []fullFormat[T] {
	row := func(format string, w CapnpWriter, open func() func([]byte) (*capnp.Message, error), reads bool) fullFormat[T] {
		return fullFormat[T]{
			format: format,
			newEnc: func() func(*T) ([]byte, error) {
				w := w
				return func(v *T) ([]byte, error) { return encode(&w, v) }
			},
			newDec: func() func([]byte) (T, error) {
				open := open()
				return func(bz []byte) (v T, err error) {
					m, err := open(bz)
					if err != nil {
						return v, err
					}
					return decode(m)
				}
			},
			enc: true, dec: true, reads: reads,
		}
	}
	return []fullFormat[T]{
		row("capnp", CapnpWriter{}, openCapnp, true),
		row("capnp-reuse", CapnpWriter{Reuse: true}, openReuse, true),
		row("capnp-packed", CapnpWriter{Packed: true}, openPack, false),
	}
}

// fbFormats are the rows of FlatBuffers of a type.
func fbFormats[T any](
	build func(*flatbuffers.Builder, *T),
	encode func(*flatbuffers.Builder, *T) []byte,
	decode func([]byte) (T, error),
) []fullFormat[T] {
	newDec := func() func([]byte) (T, error) { return decode }
	return []fullFormat[T]{{
		format: "flatbuffers",
		newEnc: func() func(*T) ([]byte, error) {
			return func(v *T) ([]byte, error) {
				b := flatbuffers.NewBuilder(1024)
				build(b, v)
				return b.FinishedBytes(), nil
			}
		},
		newDec: newDec,
		enc:    true, dec: true, reads: true,
	}, {
		format: "flatbuffers-reuse",
		newEnc: func() func(*T) ([]byte, error) {
			b := flatbuffers.NewBuilder(1024)
			return func(v *T) ([]byte, error) { return encode(b, v), nil }
		},
		enc: true,
	}}
}

// msgpackFormat is the row of the default codec of kvutil.
func msgpackFormat[T any]() fullFormat[T] {
	c := codec.Formatted[T](codec.Msgpack)
	return fullFormat[T]{
		format: "msgpack",
		newEnc: func() func(*T) ([]byte, error) { return func(v *T) ([]byte, error) { return c.Encode(*v) } },
		newDec: func() func([]byte) (T, error) { return c.Decode },
		enc:    true, dec: true, reads: true,
	}
}

func values[T any, P interface{ *T }](n int, gen func(i int) P) []T {
	vs := make([]T, n)
	for i := range vs {
		vs[i] = *gen(i) // the id is not in the value: a zero one is what the codecs get
	}
	return vs
}

func orderGroup(name string, size dataset.ValueSize) runner {
	spec := dataset.Spec{Seed: 1, Scale: 10_000, Values: size}
	vals := values[dataset.Order](benchRecords, func(i int) *dataset.Order {
		o := spec.Gen(int64(i+1), 0)
		o.ID = 0
		return o
	})
	g := &group[dataset.Order]{
		name: name,
		vals: vals,
		reads: reads[dataset.Order]{
			sink:     &sinkOrder,
			scalarOp: opScalar,
			scalar:   func(o *dataset.Order) int { return int(o.Total) },
			str:      func(o *dataset.Order) string { return o.Customer },

			scalarField: "Total", strField: "Customer",
		},
		native: native{
			fbScalar: func(bz []byte) int { return int(fbgen.GetRootAsOrder(bz, 0).Total()) },
			fbStr:    func(bz []byte) []byte { return fbgen.GetRootAsOrder(bz, 0).Customer() },
			capScalar: func(m *capnp.Message) int {
				c, err := capgen.ReadRootOrder(m)
				must(err)
				return int(c.Total())
			},
			capStr: func(m *capnp.Message) []byte {
				c, err := capgen.ReadRootOrder(m)
				must(err)
				s, err := c.CustomerBytes()
				must(err)
				return s
			},
		},
		capnpReaders: []capnpReader{{"capnp", openCapnp}, {"capnp-reuse", openReuse}},
	}
	g.formats = append(g.formats, msgpackFormat[dataset.Order]())
	bin := codec.Binary[OrderBin]()
	g.formats = append(g.formats, fullFormat[dataset.Order]{
		format: "binary",
		newEnc: func() func(*dataset.Order) ([]byte, error) {
			return func(o *dataset.Order) ([]byte, error) { return bin.Encode(OrderBin(*o)) }
		},
		newDec: func() func([]byte) (dataset.Order, error) {
			return func(bz []byte) (dataset.Order, error) {
				o, err := bin.Decode(bz)
				return dataset.Order(o), err
			}
		},
		enc: true, dec: true, reads: true,
	})
	g.formats = append(g.formats, fbFormats(BuildOrderFB, EncodeOrderFB, DecodeOrderFB)...)
	g.formats = append(g.formats, capnpFormats((*CapnpWriter).EncodeOrder, DecodeOrderCapnp)...)
	g.partials = []partialFormat[dataset.Order]{
		partialOf("msgpack", "msgpack", codec.Formatted[dataset.Order](codec.Msgpack), func(o *dataset.Order) *dataset.Order { return o }),
		partialOf("binary", "binary", bin, func(o *dataset.Order) *OrderBin { return (*OrderBin)(o) }),
		partialOf("flatbuffers", "flatbuffers", codec.Binary[OrderFB](), func(o *dataset.Order) *OrderFB { return (*OrderFB)(o) }),
		partialOf("capnp", "capnp", codec.Binary[OrderCapnp](), func(o *dataset.Order) *OrderCapnp { return (*OrderCapnp)(o) }),
		partialOf("capnp-reuse", "capnp", codec.Binary[OrderCapnpReuse](), func(o *dataset.Order) *OrderCapnpReuse { return (*OrderCapnpReuse)(o) }),
	}
	return namedGroup{name, g.cases}
}

func bookGroup() runner {
	spec := dataset.Spec{Seed: 1, Scale: 10_000, Values: dataset.Small}
	vals := values[dataset.Book](benchRecords, func(i int) *dataset.Book {
		b := spec.GenBook(int64(i + 1))
		b.ID = 0
		return b
	})
	g := &group[dataset.Book]{
		name: "book",
		vals: vals,
		reads: reads[dataset.Book]{
			sink:     &sinkBook,
			scalarOp: opScalar,
			scalar:   func(b *dataset.Book) int { return int(b.Year) },
			str:      func(b *dataset.Book) string { return b.Title },

			scalarField: "Year", strField: "Title",
		},
		native: native{
			fbScalar: func(bz []byte) int { return int(fbgen.GetRootAsBook(bz, 0).Year()) },
			fbStr:    func(bz []byte) []byte { return fbgen.GetRootAsBook(bz, 0).Title() },
			capScalar: func(m *capnp.Message) int {
				c, err := capgen.ReadRootBook(m)
				must(err)
				return int(c.Year())
			},
			capStr: func(m *capnp.Message) []byte {
				c, err := capgen.ReadRootBook(m)
				must(err)
				s, err := c.TitleBytes()
				must(err)
				return s
			},
		},
		capnpReaders: []capnpReader{{"capnp", openCapnp}, {"capnp-reuse", openReuse}},
	}
	g.formats = append(g.formats, msgpackFormat[dataset.Book]())
	g.formats = append(g.formats, fbFormats(BuildBookFB, EncodeBookFB, DecodeBookFB)...)
	g.formats = append(g.formats, capnpFormats((*CapnpWriter).EncodeBook, DecodeBookCapnp)...)
	g.partials = []partialFormat[dataset.Book]{
		partialOf("msgpack", "msgpack", codec.Formatted[dataset.Book](codec.Msgpack), func(b *dataset.Book) *dataset.Book { return b }),
		partialOf("flatbuffers", "flatbuffers", codec.Binary[BookFB](), func(b *dataset.Book) *BookFB { return (*BookFB)(b) }),
		partialOf("capnp", "capnp", codec.Binary[BookCapnp](), func(b *dataset.Book) *BookCapnp { return (*BookCapnp)(b) }),
		partialOf("capnp-reuse", "capnp", codec.Binary[BookCapnpReuse](), func(b *dataset.Book) *BookCapnpReuse { return (*BookCapnpReuse)(b) }),
	}
	return namedGroup{"book", g.cases}
}

func docGroup() runner {
	v := dataset.NewVectors(1, benchRecords)
	vals := values[dataset.Doc](benchRecords, func(i int) *dataset.Doc {
		d := v.Gen(int64(i))
		d.ID = 0
		return d
	})
	sum := func(d *dataset.Doc) (s float32) {
		for _, f := range d.Embedding {
			s += f
		}
		return s
	}
	g := &group[dataset.Doc]{
		name: "doc",
		vals: vals,
		reads: reads[dataset.Doc]{
			sink:      &sinkDoc,
			scalarOp:  opNearEmb,
			scalar:    func(d *dataset.Doc) int { return d.Year },
			embedding: sum,

			scalarField: "Year", embField: "Embedding",
		},
		native: native{
			fbScalar: func(bz []byte) int { return int(fbgen.GetRootAsDoc(bz, 0).Year()) },
			fbEmb: func(bz []byte) (s float32) {
				d := fbgen.GetRootAsDoc(bz, 0)
				for j, n := 0, d.EmbeddingLength(); j < n; j++ {
					s += d.Embedding(j)
				}
				return s
			},
			capScalar: func(m *capnp.Message) int {
				c, err := capgen.ReadRootDoc(m)
				must(err)
				return int(c.Year())
			},
			capEmb: func(m *capnp.Message) (s float32) {
				c, err := capgen.ReadRootDoc(m)
				must(err)
				l, err := c.Embedding()
				must(err)
				for j, n := 0, l.Len(); j < n; j++ {
					s += l.At(j)
				}
				return s
			},
		},
		capnpReaders: []capnpReader{{"capnp", openCapnp}, {"capnp-reuse", openReuse}},
	}
	g.formats = append(g.formats, msgpackFormat[dataset.Doc]())
	g.formats = append(g.formats, fbFormats(BuildDocFB, EncodeDocFB, DecodeDocFB)...)
	g.formats = append(g.formats, capnpFormats((*CapnpWriter).EncodeDoc, DecodeDocCapnp)...)
	g.partials = []partialFormat[dataset.Doc]{
		partialOf("msgpack", "msgpack", codec.Formatted[dataset.Doc](codec.Msgpack), func(d *dataset.Doc) *dataset.Doc { return d }),
		partialOf("flatbuffers", "flatbuffers", codec.Binary[DocFB](), func(d *dataset.Doc) *DocFB { return (*DocFB)(d) }),
		partialOf("capnp", "capnp", codec.Binary[DocCapnp](), func(d *dataset.Doc) *DocCapnp { return (*DocCapnp)(d) }),
		partialOf("capnp-reuse", "capnp", codec.Binary[DocCapnpReuse](), func(d *dataset.Doc) *DocCapnpReuse { return (*DocCapnpReuse)(d) }),
	}
	return namedGroup{"doc", g.cases}
}
