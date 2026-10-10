package codec

import (
	"encoding"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/kvutil/codec/lex"
)

// Codec is an interface for encoding and decoding values.
type Codec[T any] interface {
	Encoder[T]
	Decoder[T]
}

// Encoder is an interface for encoding values.
type Encoder[T any] interface {
	Encode(v T) ([]byte, error)
}

// Decoder is an interface for decoding values.
type Decoder[T any] interface {
	Decode(bz []byte) (T, error)
}

// CodecFor returns the codec for the given type, or an error if the type has none.
// Integers are encoded in lexicographical order with a fixed width.
func CodecFor[T any]() (Codec[T], error) {
	var (
		zero T
		c    any
	)
	switch any(zero).(type) {
	case string:
		c = stringCodec{}
	case []byte:
		c = bytesCodec{}
	case int:
		c = fixedCodec[int]{
			size:   8,
			encode: func(v int) []byte { return lex.EncodeInt64(int64(v)) },
			decode: func(bz []byte) int { return int(lex.DecodeInt64(bz)) },
		}
	case int8:
		c = fixedCodec[int8]{size: 1, encode: lex.EncodeInt8, decode: lex.DecodeInt8}
	case int16:
		c = fixedCodec[int16]{size: 2, encode: lex.EncodeInt16, decode: lex.DecodeInt16}
	case int32:
		c = fixedCodec[int32]{size: 4, encode: lex.EncodeInt32, decode: lex.DecodeInt32}
	case int64:
		c = fixedCodec[int64]{size: 8, encode: lex.EncodeInt64, decode: lex.DecodeInt64}
	case uint:
		c = fixedCodec[uint]{
			size:   8,
			encode: func(v uint) []byte { return lex.EncodeUint64(uint64(v)) },
			decode: func(bz []byte) uint { return uint(lex.DecodeUint64(bz)) },
		}
	case uint8:
		c = fixedCodec[uint8]{
			size:   1,
			encode: func(v uint8) []byte { return []byte{v} },
			decode: func(bz []byte) uint8 { return bz[0] },
		}
	case uint16:
		c = fixedCodec[uint16]{size: 2, encode: lex.EncodeUint16, decode: lex.DecodeUint16}
	case uint32:
		c = fixedCodec[uint32]{size: 4, encode: lex.EncodeUint32, decode: lex.DecodeUint32}
	case uint64:
		c = fixedCodec[uint64]{size: 8, encode: lex.EncodeUint64, decode: lex.DecodeUint64}
	}
	if c != nil {
		return c.(Codec[T]), nil
	}

	rt := reflect.TypeFor[T]()
	if rt.Implements(reflect.TypeFor[encoding.BinaryMarshaler]()) &&
		reflect.TypeFor[*T]().Implements(reflect.TypeFor[encoding.BinaryUnmarshaler]()) {
		return binaryCodec[T]{}, nil
	}

	return nil, fmt.Errorf("no codec for type %s", rt)
}

type stringCodec struct{}

// Encode implements the Codec interface.
func (stringCodec) Encode(v string) ([]byte, error) {
	return []byte(v), nil
}

// Decode implements the Codec interface.
func (stringCodec) Decode(bz []byte) (string, error) {
	return string(bz), nil
}

// fixedCodec is a codec for values that are always encoded to the same number of bytes.
type fixedCodec[T any] struct {
	size   int
	encode func(T) []byte
	decode func([]byte) T
}

// Encode implements the Codec interface.
func (c fixedCodec[T]) Encode(v T) ([]byte, error) {
	return c.encode(v), nil
}

// Decode implements the Codec interface.
func (c fixedCodec[T]) Decode(bz []byte) (T, error) {
	if len(bz) != c.size {
		var zero T
		return zero, fmt.Errorf("invalid length %d for %T, expected %d", len(bz), zero, c.size)
	}
	return c.decode(bz), nil
}

type bytesCodec struct{}

// Encode implements the Codec interface.
func (bytesCodec) Encode(v []byte) ([]byte, error) {
	return v, nil
}

// Decode implements the Codec interface.
func (bytesCodec) Decode(bz []byte) ([]byte, error) {
	return bz, nil
}

type binaryCodec[T any] struct{}

// Encode implements the Codec interface.
func (binaryCodec[T]) Encode(v T) ([]byte, error) {
	return any(v).(encoding.BinaryMarshaler).MarshalBinary()
}

// Decode implements the Codec interface.
func (binaryCodec[T]) Decode(bz []byte) (T, error) {
	var v T
	err := any(&v).(encoding.BinaryUnmarshaler).UnmarshalBinary(bz)
	return v, err
}
