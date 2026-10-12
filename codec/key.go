package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/internal/constraints"
)

// KeyCodec encodes and decodes the keys of a store.
// Unlike the encoding of a value, the encoding of a key must sort as the keys themselves do,
// and every key must have exactly one encoding.
type KeyCodec[K any] interface {
	// The methods of ValueCodec encode a key that is the whole key of a record or its last
	// component, which does not have to mark where it ends.
	ValueCodec[K]
	// Append appends the key to dst in a form that marks where it ends, so that other
	// components can follow it in a composite key.
	Append(dst []byte, k K) ([]byte, error)
	// Read decodes a key that was written by Append from the front of bz, and returns the
	// number of bytes that it took. The key does not refer to the given slice.
	Read(bz []byte) (k K, n int, err error)
}

// KeyFor returns the key codec of a type, or an error if the type has none.
// Integers, floats, booleans, strings, byte slices, byte arrays, times and pairs of these
// have one, and so do the named types that are defined on them.
func KeyFor[K any]() (KeyCodec[K], error) {
	var zero K
	if p, ok := any(zero).(keyCodecProvider); ok {
		c, err := p.keyCodec()
		if err != nil {
			return nil, err
		}
		return c.(KeyCodec[K]), nil
	}

	var c any
	switch any(zero).(type) {
	case string:
		c = StringKey[string]()
	case []byte:
		c = BytesKey[[]byte]()
	case int:
		c = IntKey[int]()
	case int8:
		c = IntKey[int8]()
	case int16:
		c = IntKey[int16]()
	case int32:
		c = IntKey[int32]()
	case int64:
		c = IntKey[int64]()
	case uint:
		c = IntKey[uint]()
	case uint8:
		c = IntKey[uint8]()
	case uint16:
		c = IntKey[uint16]()
	case uint32:
		c = IntKey[uint32]()
	case uint64:
		c = IntKey[uint64]()
	case float32:
		c = FloatKey[float32]()
	case float64:
		c = FloatKey[float64]()
	case bool:
		c = BoolKey[bool]()
	case time.Time:
		c = TimeKey()
	}
	if c != nil {
		return c.(KeyCodec[K]), nil
	}

	// A named type has the encoding of the type that it is defined on.
	rt := reflect.TypeFor[K]()
	switch rt.Kind() {
	case reflect.String:
		return convertedKey[K, string]{base: StringKey[string]()}, nil
	case reflect.Int:
		return convertedKey[K, int]{base: IntKey[int]()}, nil
	case reflect.Int8:
		return convertedKey[K, int8]{base: IntKey[int8]()}, nil
	case reflect.Int16:
		return convertedKey[K, int16]{base: IntKey[int16]()}, nil
	case reflect.Int32:
		return convertedKey[K, int32]{base: IntKey[int32]()}, nil
	case reflect.Int64:
		return convertedKey[K, int64]{base: IntKey[int64]()}, nil
	case reflect.Uint:
		return convertedKey[K, uint]{base: IntKey[uint]()}, nil
	case reflect.Uint8:
		return convertedKey[K, uint8]{base: IntKey[uint8]()}, nil
	case reflect.Uint16:
		return convertedKey[K, uint16]{base: IntKey[uint16]()}, nil
	case reflect.Uint32:
		return convertedKey[K, uint32]{base: IntKey[uint32]()}, nil
	case reflect.Uint64:
		return convertedKey[K, uint64]{base: IntKey[uint64]()}, nil
	case reflect.Float32:
		return convertedKey[K, float32]{base: FloatKey[float32]()}, nil
	case reflect.Float64:
		return convertedKey[K, float64]{base: FloatKey[float64]()}, nil
	case reflect.Bool:
		return convertedKey[K, bool]{base: BoolKey[bool]()}, nil
	case reflect.Slice:
		if rt.Elem().Kind() == reflect.Uint8 {
			return convertedKey[K, []byte]{base: BytesKey[[]byte]()}, nil
		}
	case reflect.Array:
		if rt.Elem().Kind() == reflect.Uint8 {
			return arrayKey[K]{size: rt.Len()}, nil
		}
	}

	return nil, fmt.Errorf("no key codec for type %s", rt)
}

// errShort returns the error of an encoded key that does not have the length that its codec needs.
func errShort(name string, got, want int) error {
	return fmt.Errorf("invalid length %d for %s, expected %d", got, name, want)
}

// keyCodecProvider is implemented by the types of this package that know their own key codec.
type keyCodecProvider interface {
	keyCodec() (any, error)
	// anyKeyCodec returns the same codec as a codec of any value, which must be of the type.
	anyKeyCodec() (KeyCodec[any], error)
}

// IntKey returns the key codec of an integer type. The keys have a fixed width and sort
// numerically. int and uint are always 8 bytes wide, whatever the platform.
func IntKey[K constraints.Integer]() KeyCodec[K] {
	rt := reflect.TypeFor[K]()
	kind := rt.Kind()
	size := int(rt.Size())
	if kind == reflect.Int || kind == reflect.Uint || kind == reflect.Uintptr {
		size = 8
	}

	c := intKey[K]{
		name:  kind.String(),
		size:  size,
		shift: uint(64 - 8*size),
	}
	if kind >= reflect.Int && kind <= reflect.Int64 {
		// Flipping the sign bit makes the negative numbers sort before the others.
		c.sign = 1 << (8*size - 1)
	}
	return c
}

type intKey[K constraints.Integer] struct {
	name  string
	size  int
	shift uint
	sign  uint64
}

// Name implements the KeyCodec interface.
func (c intKey[K]) Name() string {
	return c.name
}

// Encode implements the KeyCodec interface.
func (c intKey[K]) Encode(k K) ([]byte, error) {
	return c.Append(nil, k)
}

// Decode implements the KeyCodec interface.
func (c intKey[K]) Decode(bz []byte) (K, error) {
	if len(bz) != c.size {
		return 0, errShort(c.name, len(bz), c.size)
	}
	k, _, err := c.Read(bz)
	return k, err
}

// Append implements the KeyCodec interface.
func (c intKey[K]) Append(dst []byte, k K) ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], (uint64(k)^c.sign)<<c.shift)
	return append(dst, buf[:c.size]...), nil
}

// Read implements the KeyCodec interface.
func (c intKey[K]) Read(bz []byte) (K, int, error) {
	if len(bz) < c.size {
		return 0, 0, errShort(c.name, len(bz), c.size)
	}

	var buf [8]byte
	copy(buf[:], bz[:c.size])
	return K((binary.BigEndian.Uint64(buf[:]) >> c.shift) ^ c.sign), c.size, nil
}

// FloatKey returns the key codec of a floating-point type. The keys have a fixed width and sort
// numerically, with NaN before every number. -0 and +0 are equal, so they are the same key.
func FloatKey[K ~float32 | ~float64]() KeyCodec[K] {
	rt := reflect.TypeFor[K]()
	return floatKey[K]{name: rt.Kind().String(), size: int(rt.Size())}
}

type floatKey[K ~float32 | ~float64] struct {
	name string
	size int
}

// Name implements the KeyCodec interface.
func (c floatKey[K]) Name() string {
	return c.name
}

// Encode implements the KeyCodec interface.
func (c floatKey[K]) Encode(k K) ([]byte, error) {
	return c.Append(nil, k)
}

// Decode implements the KeyCodec interface.
func (c floatKey[K]) Decode(bz []byte) (K, error) {
	if len(bz) != c.size {
		return 0, errShort(c.name, len(bz), c.size)
	}
	k, _, err := c.Read(bz)
	return k, err
}

// Append implements the KeyCodec interface.
func (c floatKey[K]) Append(dst []byte, k K) ([]byte, error) {
	if c.size == 4 {
		return append(dst, lex.EncodeFloat32(float32(k))...), nil
	}
	return append(dst, lex.EncodeFloat64(float64(k))...), nil
}

// Read implements the KeyCodec interface.
func (c floatKey[K]) Read(bz []byte) (K, int, error) {
	if len(bz) < c.size {
		return 0, 0, errShort(c.name, len(bz), c.size)
	}
	if c.size == 4 {
		return K(lex.DecodeFloat32(bz[:4])), 4, nil
	}
	return K(lex.DecodeFloat64(bz[:8])), 8, nil
}

// BoolKey returns the key codec of a boolean type. False sorts before true.
func BoolKey[K ~bool]() KeyCodec[K] {
	return boolKey[K]{}
}

type boolKey[K ~bool] struct{}

// Name implements the KeyCodec interface.
func (boolKey[K]) Name() string {
	return "bool"
}

// Encode implements the KeyCodec interface.
func (c boolKey[K]) Encode(k K) ([]byte, error) {
	return c.Append(nil, k)
}

// Decode implements the KeyCodec interface.
func (c boolKey[K]) Decode(bz []byte) (K, error) {
	if len(bz) != 1 {
		return false, errShort(c.Name(), len(bz), 1)
	}
	k, _, err := c.Read(bz)
	return k, err
}

// Append implements the KeyCodec interface.
func (boolKey[K]) Append(dst []byte, k K) ([]byte, error) {
	if k {
		return append(dst, 0x01), nil
	}
	return append(dst, 0x00), nil
}

// Read implements the KeyCodec interface.
func (c boolKey[K]) Read(bz []byte) (K, int, error) {
	if len(bz) < 1 {
		return false, 0, errShort(c.Name(), len(bz), 1)
	}
	return bz[0] != 0x00, 1, nil
}

// StringKey returns the key codec of a string type. The keys sort by their bytes.
func StringKey[K ~string]() KeyCodec[K] {
	return stringKey[K]{}
}

type stringKey[K ~string] struct{}

// Name implements the KeyCodec interface.
func (stringKey[K]) Name() string {
	return "string"
}

// Encode implements the KeyCodec interface.
func (stringKey[K]) Encode(k K) ([]byte, error) {
	return []byte(k), nil
}

// Decode implements the KeyCodec interface.
func (stringKey[K]) Decode(bz []byte) (K, error) {
	return K(bz), nil
}

// Append implements the KeyCodec interface.
func (stringKey[K]) Append(dst []byte, k K) ([]byte, error) {
	return lex.AppendTerminated(dst, []byte(k)), nil
}

// Read implements the KeyCodec interface.
func (stringKey[K]) Read(bz []byte) (K, int, error) {
	value, n, err := lex.ReadTerminated(bz)
	return K(value), n, err
}

// BytesKey returns the key codec of a byte slice type.
func BytesKey[K ~[]byte]() KeyCodec[K] {
	return bytesKey[K]{}
}

type bytesKey[K ~[]byte] struct{}

// Name implements the KeyCodec interface.
func (bytesKey[K]) Name() string {
	return "bytes"
}

// Encode implements the KeyCodec interface.
func (bytesKey[K]) Encode(k K) ([]byte, error) {
	return bytes.Clone(k), nil
}

// Decode implements the KeyCodec interface.
func (bytesKey[K]) Decode(bz []byte) (K, error) {
	return K(bytes.Clone(bz)), nil
}

// Append implements the KeyCodec interface.
func (bytesKey[K]) Append(dst []byte, k K) ([]byte, error) {
	return lex.AppendTerminated(dst, k), nil
}

// Read implements the KeyCodec interface.
func (bytesKey[K]) Read(bz []byte) (K, int, error) {
	value, n, err := lex.ReadTerminated(bz)
	return K(value), n, err
}

// timeKeySize is the size of an encoded time: its seconds and then its nanoseconds.
const timeKeySize = 12

// TimeKey returns the key codec of times. The keys have a fixed width and sort chronologically.
// A time is decoded in UTC, without the location and the monotonic clock reading that it had.
func TimeKey() KeyCodec[time.Time] {
	return timeKey{}
}

type timeKey struct{}

// Name implements the KeyCodec interface.
func (timeKey) Name() string {
	return "time"
}

// Encode implements the KeyCodec interface.
func (c timeKey) Encode(k time.Time) ([]byte, error) {
	return c.Append(nil, k)
}

// Decode implements the KeyCodec interface.
func (c timeKey) Decode(bz []byte) (time.Time, error) {
	if len(bz) != timeKeySize {
		return time.Time{}, errShort(c.Name(), len(bz), timeKeySize)
	}
	k, _, err := c.Read(bz)
	return k, err
}

// Append implements the KeyCodec interface.
func (timeKey) Append(dst []byte, k time.Time) ([]byte, error) {
	dst = append(dst, lex.EncodeInt64(k.Unix())...)
	return append(dst, lex.EncodeUint32(uint32(k.Nanosecond()))...), nil
}

// Read implements the KeyCodec interface.
func (c timeKey) Read(bz []byte) (time.Time, int, error) {
	if len(bz) < timeKeySize {
		return time.Time{}, 0, errShort(c.Name(), len(bz), timeKeySize)
	}
	sec, nsec := lex.DecodeInt64(bz[:8]), lex.DecodeUint32(bz[8:timeKeySize])
	return time.Unix(sec, int64(nsec)).UTC(), timeKeySize, nil
}

// arrayKey is the key codec of the byte array types, such as a UUID.
type arrayKey[K any] struct {
	size int
}

// Name implements the KeyCodec interface.
func (c arrayKey[K]) Name() string {
	return fmt.Sprintf("bytes[%d]", c.size)
}

// Encode implements the KeyCodec interface.
func (c arrayKey[K]) Encode(k K) ([]byte, error) {
	return c.Append(nil, k)
}

// Decode implements the KeyCodec interface.
func (c arrayKey[K]) Decode(bz []byte) (K, error) {
	if len(bz) != c.size {
		var zero K
		return zero, errShort(c.Name(), len(bz), c.size)
	}
	k, _, err := c.Read(bz)
	return k, err
}

// Append implements the KeyCodec interface.
func (c arrayKey[K]) Append(dst []byte, k K) ([]byte, error) {
	n := len(dst)
	dst = append(dst, make([]byte, c.size)...)
	reflect.Copy(reflect.ValueOf(dst[n:]), reflect.ValueOf(k))
	return dst, nil
}

// Read implements the KeyCodec interface.
func (c arrayKey[K]) Read(bz []byte) (K, int, error) {
	var k K
	if len(bz) < c.size {
		return k, 0, errShort(c.Name(), len(bz), c.size)
	}
	reflect.Copy(reflect.ValueOf(&k).Elem(), reflect.ValueOf(bz[:c.size]))
	return k, c.size, nil
}

// convertedKey is the key codec of a named type K that is defined on the type U.
type convertedKey[K, U any] struct {
	base KeyCodec[U]
}

func (convertedKey[K, U]) to(k K) U {
	return reflect.ValueOf(k).Convert(reflect.TypeFor[U]()).Interface().(U)
}

func (convertedKey[K, U]) from(u U) K {
	return reflect.ValueOf(u).Convert(reflect.TypeFor[K]()).Interface().(K)
}

// Name implements the KeyCodec interface.
func (c convertedKey[K, U]) Name() string {
	return c.base.Name()
}

// Encode implements the KeyCodec interface.
func (c convertedKey[K, U]) Encode(k K) ([]byte, error) {
	return c.base.Encode(c.to(k))
}

// Decode implements the KeyCodec interface.
func (c convertedKey[K, U]) Decode(bz []byte) (K, error) {
	u, err := c.base.Decode(bz)
	return c.from(u), err
}

// Append implements the KeyCodec interface.
func (c convertedKey[K, U]) Append(dst []byte, k K) ([]byte, error) {
	return c.base.Append(dst, c.to(k))
}

// Read implements the KeyCodec interface.
func (c convertedKey[K, U]) Read(bz []byte) (K, int, error) {
	u, n, err := c.base.Read(bz)
	return c.from(u), n, err
}
