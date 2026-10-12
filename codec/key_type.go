package codec

import (
	"fmt"
	"reflect"
	"time"
)

// KeyForType returns the key codec of a type that is only known when the program runs, like
// the type of a field that is found by reflection. It has the name and the encoding of the
// codec that KeyFor returns for the type, but it works on values of any type: a key that it
// encodes must be of the type, and a key that it decodes is of it.
// It returns an error if the type has no key codec.
func KeyForType(t reflect.Type) (KeyCodec[any], error) {
	if p, ok := reflect.Zero(t).Interface().(keyCodecProvider); ok {
		return p.anyKeyCodec()
	}
	if t == reflect.TypeFor[time.Time]() {
		return anyKeyOf(TimeKey()), nil
	}

	switch t.Kind() {
	case reflect.String:
		return convertedAnyKey[string]{base: StringKey[string](), t: t}, nil
	case reflect.Int:
		return convertedAnyKey[int]{base: IntKey[int](), t: t}, nil
	case reflect.Int8:
		return convertedAnyKey[int8]{base: IntKey[int8](), t: t}, nil
	case reflect.Int16:
		return convertedAnyKey[int16]{base: IntKey[int16](), t: t}, nil
	case reflect.Int32:
		return convertedAnyKey[int32]{base: IntKey[int32](), t: t}, nil
	case reflect.Int64:
		return convertedAnyKey[int64]{base: IntKey[int64](), t: t}, nil
	case reflect.Uint:
		return convertedAnyKey[uint]{base: IntKey[uint](), t: t}, nil
	case reflect.Uint8:
		return convertedAnyKey[uint8]{base: IntKey[uint8](), t: t}, nil
	case reflect.Uint16:
		return convertedAnyKey[uint16]{base: IntKey[uint16](), t: t}, nil
	case reflect.Uint32:
		return convertedAnyKey[uint32]{base: IntKey[uint32](), t: t}, nil
	case reflect.Uint64:
		return convertedAnyKey[uint64]{base: IntKey[uint64](), t: t}, nil
	case reflect.Float32:
		return convertedAnyKey[float32]{base: FloatKey[float32](), t: t}, nil
	case reflect.Float64:
		return convertedAnyKey[float64]{base: FloatKey[float64](), t: t}, nil
	case reflect.Bool:
		return convertedAnyKey[bool]{base: BoolKey[bool](), t: t}, nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return convertedAnyKey[[]byte]{base: BytesKey[[]byte](), t: t}, nil
		}
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return anyArrayKey{t: t, size: t.Len()}, nil
		}
	}
	return nil, fmt.Errorf("no key codec for type %s", t)
}

// anyKeyOf returns a key codec of a type as a codec of any value, which must be of the type.
func anyKeyOf[K any](base KeyCodec[K]) KeyCodec[any] {
	return convertedAnyKey[K]{base: base, t: reflect.TypeFor[K]()}
}

// convertedAnyKey is the key codec of the values of the type t, which is the type U or a
// named type that is defined on it, as a codec of any value.
type convertedAnyKey[U any] struct {
	base KeyCodec[U]
	t    reflect.Type
}

func (c convertedAnyKey[U]) to(k any) (u U, err error) {
	rv := reflect.ValueOf(k)
	if !rv.IsValid() || rv.Type() != c.t {
		return u, fmt.Errorf("a key of the type %T where one of the type %s is expected", k, c.t)
	}
	return rv.Convert(reflect.TypeFor[U]()).Interface().(U), nil
}

func (c convertedAnyKey[U]) from(u U) any {
	return reflect.ValueOf(u).Convert(c.t).Interface()
}

// Name implements the KeyCodec interface.
func (c convertedAnyKey[U]) Name() string {
	return c.base.Name()
}

// Encode implements the KeyCodec interface.
func (c convertedAnyKey[U]) Encode(k any) ([]byte, error) {
	u, err := c.to(k)
	if err != nil {
		return nil, err
	}
	return c.base.Encode(u)
}

// Decode implements the KeyCodec interface.
func (c convertedAnyKey[U]) Decode(bz []byte) (any, error) {
	u, err := c.base.Decode(bz)
	if err != nil {
		return nil, err
	}
	return c.from(u), nil
}

// Append implements the KeyCodec interface.
func (c convertedAnyKey[U]) Append(dst []byte, k any) ([]byte, error) {
	u, err := c.to(k)
	if err != nil {
		return nil, err
	}
	return c.base.Append(dst, u)
}

// Read implements the KeyCodec interface.
func (c convertedAnyKey[U]) Read(bz []byte) (any, int, error) {
	u, n, err := c.base.Read(bz)
	if err != nil {
		return nil, 0, err
	}
	return c.from(u), n, nil
}

// anyArrayKey is the key codec of the values of a byte array type, as a codec of any value.
type anyArrayKey struct {
	t    reflect.Type
	size int
}

// Name implements the KeyCodec interface.
func (c anyArrayKey) Name() string {
	return fmt.Sprintf("bytes[%d]", c.size)
}

// Encode implements the KeyCodec interface.
func (c anyArrayKey) Encode(k any) ([]byte, error) {
	return c.Append(nil, k)
}

// Decode implements the KeyCodec interface.
func (c anyArrayKey) Decode(bz []byte) (any, error) {
	if len(bz) != c.size {
		return nil, errShort(c.Name(), len(bz), c.size)
	}
	k, _, err := c.Read(bz)
	return k, err
}

// Append implements the KeyCodec interface.
func (c anyArrayKey) Append(dst []byte, k any) ([]byte, error) {
	rv := reflect.ValueOf(k)
	if !rv.IsValid() || rv.Type() != c.t {
		return nil, fmt.Errorf("a key of the type %T where one of the type %s is expected", k, c.t)
	}

	n := len(dst)
	dst = append(dst, make([]byte, c.size)...)
	reflect.Copy(reflect.ValueOf(dst[n:]), rv)
	return dst, nil
}

// Read implements the KeyCodec interface.
func (c anyArrayKey) Read(bz []byte) (any, int, error) {
	if len(bz) < c.size {
		return nil, 0, errShort(c.Name(), len(bz), c.size)
	}
	k := reflect.New(c.t).Elem()
	reflect.Copy(k, reflect.ValueOf(bz[:c.size]))
	return k.Interface(), c.size, nil
}
