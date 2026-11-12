// Package flat is the codecs of the records that the benchmarks keep in kvutil's stores, in
// FlatBuffers: a codec.ValueCodec named "flatbuffers" for each (Order, Book, Doc, Event, and, in
// the packages of the workloads, the records that those keep to themselves), each with partial
// decoding (codec.PartialDecoder), which reads the named fields straight from the bytes.
//
// The ids of the records are not in the values: kvutil keeps the id in the key, and a store sets
// it after it decodes. A decode leaves the ID zero, and a partial decode asked for the ID does
// nothing for it. A partial decode asked for a name that the record has not is not ok.
//
// The mapping of the Go types is the one of the schema (schema/records.fbs):
//   - time.Time is Unix nanoseconds, in UTC when decoded (the years 1678 to 2262);
//   - a *string is an absent field for nil and a present one, even an empty string, otherwise;
//   - an empty []string, []byte or []float32 is absent in the encoding and nil once decoded, as
//     the generators of the dataset make them.
//
// An encode takes a builder from a pool and returns a copy of the bytes, which the caller owns. A
// decode copies out of its input, as the contract of the ValueCodec says, and turns the panic
// that the accessors of FlatBuffers make on bytes that are not valid (they do not check the
// bounds) into an error that wraps ErrCorrupt.
//
// # Generated code
//
// fbgen/ is generated from the schema, committed, and formatted with gofmt. To generate it again,
// with flatc 25.12.19, from this directory (bench/internal/flat):
//
//	flatc --go --gen-object-api --go-namespace fbgen \
//	  --go-module-name github.com/ehsanranjbar/kvutil/bench/internal/flat -o . schema/records.fbs
//	gofmt -w fbgen
//
// The object API (the T types) is generated, but the codecs use the accessors and the builder
// functions: the object API cannot tell an absent string from an empty one.
//
// A table that a workload keeps (relations, lifecycle) has its definition (Def) in the package of
// the workload, next to the type, which is not exported; flat.New makes the codec of it.
package flat
