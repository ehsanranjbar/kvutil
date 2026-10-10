package codec

import (
	"encoding"
	"encoding/json"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// ValueCodec encodes and decodes the values of a store.
type ValueCodec[V any] interface {
	// Name identifies the encoding. It is recorded with the definition of a store, so that data
	// is not read with another encoding than the one it was written with.
	Name() string
	// Encode encodes the value into a slice that the caller owns.
	Encode(v V) ([]byte, error)
	// Decode decodes the value. The result does not refer to the given slice.
	Decode(bz []byte) (V, error)
}

// Format is a general encoding that can encode any type, which makes it usable as the default
// value encoding of all the stores of a schema.
type Format interface {
	// Name identifies the format.
	Name() string
	// Marshal encodes the value that v points to.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes the data into the value that v points to.
	Unmarshal(data []byte, v any) error
}

var (
	// Msgpack is the MessagePack format. It is the format that is used when none is given.
	Msgpack Format = msgpackFormat{}
	// JSON is the JSON format.
	JSON Format = jsonFormat{}
)

type msgpackFormat struct{}

// Name implements the Format interface.
func (msgpackFormat) Name() string { return "msgpack" }

// Marshal implements the Format interface.
func (msgpackFormat) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

// Unmarshal implements the Format interface.
func (msgpackFormat) Unmarshal(data []byte, v any) error { return msgpack.Unmarshal(data, v) }

type jsonFormat struct{}

// Name implements the Format interface.
func (jsonFormat) Name() string { return "json" }

// Marshal implements the Format interface.
func (jsonFormat) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements the Format interface.
func (jsonFormat) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// From returns a value codec that encodes the values of a type with the given format.
func From[V any](format Format) ValueCodec[V] {
	return formatCodec[V]{format: format}
}

type formatCodec[V any] struct {
	format Format
}

// Name implements the ValueCodec interface.
func (c formatCodec[V]) Name() string {
	return c.format.Name()
}

// Encode implements the ValueCodec interface.
func (c formatCodec[V]) Encode(v V) ([]byte, error) {
	return c.format.Marshal(&v)
}

// Decode implements the ValueCodec interface.
func (c formatCodec[V]) Decode(bz []byte) (V, error) {
	var v V
	err := c.format.Unmarshal(bz, &v)
	return v, err
}

// BinaryPtr is the pointer of a type that encodes and decodes itself.
type BinaryPtr[V any] interface {
	*V
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// Binary returns a value codec for a type that encodes and decodes itself with MarshalBinary
// and UnmarshalBinary.
func Binary[V any, PV BinaryPtr[V]]() ValueCodec[V] {
	return binaryValue[V]{}
}

// binaryValue is a value codec for the types whose pointer is a binary marshaler and unmarshaler.
type binaryValue[V any] struct{}

// Name implements the ValueCodec interface.
func (binaryValue[V]) Name() string {
	return "binary"
}

// Encode implements the ValueCodec interface.
func (binaryValue[V]) Encode(v V) ([]byte, error) {
	return any(&v).(encoding.BinaryMarshaler).MarshalBinary()
}

// Decode implements the ValueCodec interface.
func (binaryValue[V]) Decode(bz []byte) (V, error) {
	var v V
	err := any(&v).(encoding.BinaryUnmarshaler).UnmarshalBinary(bz)
	return v, err
}

// ValueFor returns the default value codec of a type: its own MarshalBinary and UnmarshalBinary
// if it has them, the given format otherwise, and Msgpack if the format is nil.
func ValueFor[V any](format Format) ValueCodec[V] {
	var v V
	_, marshaler := any(&v).(encoding.BinaryMarshaler)
	_, unmarshaler := any(&v).(encoding.BinaryUnmarshaler)
	if marshaler && unmarshaler {
		return binaryValue[V]{}
	}

	if format == nil {
		format = Msgpack
	}
	return From[V](format)
}
