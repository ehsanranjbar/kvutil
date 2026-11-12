package types

import "github.com/ehsanranjbar/kvutil/codec"

// Codec returns the value codec of a type that encodes itself as FlatBuffers. It is the binary
// codec of the type under the name "flatbuffers", with the partial decoding of the type.
//
// A store records the name of its codec and refuses to open data that another one wrote. The
// binary codec is named "binary", as the codec of the protobuf messages that the pet store
// stored before was, so a database of that version would be read as FlatBuffers. The name
// makes it fail to open instead.
func Codec[V any, PV codec.BinaryPtr[V]]() codec.ValueCodec[V] {
	return flatbuffersCodec[V]{ValueCodec: codec.Binary[V, PV]()}
}

type flatbuffersCodec[V any] struct {
	codec.ValueCodec[V]
}

// Name implements the ValueCodec interface.
func (flatbuffersCodec[V]) Name() string { return "flatbuffers" }

// PartialDecode implements the PartialDecoder interface by the type, which has
// UnmarshalPartial.
func (c flatbuffersCodec[V]) PartialDecode(fields []string) (func(bz []byte, v *V) error, bool) {
	return codec.PartialOf(c.ValueCodec, fields)
}
