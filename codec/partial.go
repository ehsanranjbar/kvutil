package codec

import (
	"encoding"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unicode"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

// PartialUnmarshaler is implemented by the types that can decode some of their fields only.
type PartialUnmarshaler interface {
	// UnmarshalPartial decodes the named fields of the data into the receiver, which is zero.
	// The fields are paths of Go struct fields, as field.Path names them. It may decode more
	// fields; the others stay zero. The receiver must not refer to data afterwards.
	UnmarshalPartial(data []byte, fields []string) error
}

// PartialDecoder is an optional capability of a value codec to decode some fields of a value.
// The codecs of the built-in formats have it for the structs that they can decode field by field.
// A custom Format does not have it, and a codec that is built on one implements it itself.
// A struct whose field is registered with msgpack.Register is not detected, and has to be
// decoded in full.
type PartialDecoder[V any] interface {
	// PartialDecode returns the function that decodes the named fields into a zero value,
	// or false if the codec cannot do so for these fields. The function is safe for
	// concurrent use, and what it decodes does not refer to the given slice.
	PartialDecode(fields []string) (decode func(bz []byte, v *V) error, ok bool)
}

// PartialOf returns the partial decoder of a codec for the fields, or false if it has none.
// With no fields it returns a decoder that decodes nothing, if the codec has partial decoding.
func PartialOf[V any](c ValueCodec[V], fields []string) (func(bz []byte, v *V) error, bool) {
	pd, ok := c.(PartialDecoder[V])
	if !ok {
		return nil, false
	}
	if len(fields) == 0 {
		return func([]byte, *V) error { return nil }, true
	}
	return pd.PartialDecode(fields)
}

// WithoutPartial returns the codec without its partial decoding. The name is unchanged.
func WithoutPartial[V any](c ValueCodec[V]) ValueCodec[V] {
	return plainCodec[V]{c: c}
}

// plainCodec exposes the methods of a value codec only.
type plainCodec[V any] struct {
	c ValueCodec[V]
}

// Name implements the ValueCodec interface.
func (p plainCodec[V]) Name() string { return p.c.Name() }

// Encode implements the ValueCodec interface.
func (p plainCodec[V]) Encode(v V) ([]byte, error) { return p.c.Encode(v) }

// Decode implements the ValueCodec interface.
func (p plainCodec[V]) Decode(bz []byte) (V, error) { return p.c.Decode(bz) }

// PartialDecode implements the PartialDecoder interface. A binary value has it if its type
// implements PartialUnmarshaler.
func (binaryValue[V]) PartialDecode(fields []string) (func(bz []byte, v *V) error, bool) {
	var v V
	if _, ok := any(&v).(PartialUnmarshaler); !ok {
		return nil, false
	}

	fields = slices.Clone(fields)
	return func(bz []byte, v *V) error {
		return any(v).(PartialUnmarshaler).UnmarshalPartial(bz, fields)
	}, true
}

// PartialDecode implements the PartialDecoder interface. The built-in formats decode a struct
// into a narrower struct that has the named fields only, which skips the other fields of the
// data without decoding them.
func (c formatCodec[V]) PartialDecode(fields []string) (func(bz []byte, v *V) error, bool) {
	var kind shadowKind
	switch c.format.(type) {
	case msgpackFormat:
		kind = shadowMsgpack
	case jsonFormat:
		kind = shadowJSON
	default:
		return nil, false
	}

	t := reflect.TypeFor[V]()
	if !shadowable(t, kind) {
		return nil, false
	}

	top := make([]string, 0, len(fields))
	for _, f := range fields {
		first, _, _ := strings.Cut(f, ".")
		top = append(top, first)
	}
	slices.Sort(top)
	top = slices.Compact(top)

	s, ok := shadowOf(t, kind, c.format, top)
	if !ok {
		return nil, false
	}
	return func(bz []byte, v *V) error {
		return s.decode(bz, reflect.ValueOf(v).Elem())
	}, true
}

type shadowKind int

const (
	shadowMsgpack shadowKind = iota
	shadowJSON
)

// maxShadows is the number of the shadow types that are made for one type. The types made by
// reflect.StructOf are never freed.
const maxShadows = 64

var (
	shadowMu    sync.Mutex
	shadowCache = map[shadowKey]*shadow{}
	shadowCount = map[reflect.Type]int{}
)

type shadowKey struct {
	typ    reflect.Type
	format string
	fields string
}

// shadow decodes the named fields of a struct through a struct that has these fields only.
type shadow struct {
	typ       reflect.Type
	dst       []int // the index of each field of the shadow in the original struct
	unmarshal func([]byte, any) error
	pool      sync.Pool // of *shadowValue
}

type shadowValue struct {
	ptr   reflect.Value // *typ
	iface any           // ptr as an interface
}

var (
	msgpackDecoderTypes = []reflect.Type{
		reflect.TypeFor[msgpack.CustomDecoder](),
		reflect.TypeFor[msgpack.Unmarshaler](),
		reflect.TypeFor[encoding.BinaryUnmarshaler](),
		reflect.TypeFor[encoding.TextUnmarshaler](),
	}
	jsonDecoderTypes = []reflect.Type{
		reflect.TypeFor[json.Unmarshaler](),
		reflect.TypeFor[encoding.TextUnmarshaler](),
	}
)

// shadowable reports whether a type can be decoded through a narrower struct with the same
// result for the fields that it has.
func shadowable(t reflect.Type, kind shadowKind) bool {
	if t.Kind() != reflect.Struct {
		return false
	}

	decoders := msgpackDecoderTypes
	if kind == shadowJSON {
		decoders = jsonDecoderTypes
	}
	for _, d := range decoders {
		if t.Implements(d) || reflect.PointerTo(t).Implements(d) {
			return false
		}
	}

	// encoding/json matches the keys without regard to case, so a narrower struct could
	// take a key that is meant for a field that it lacks.
	names := map[string]struct{}{}
	for i := range t.NumField() {
		sf := t.Field(i)
		if sf.Anonymous {
			return false
		}
		// The options of a msgpack struct, such as as_array, change how it is encoded. A
		// struct that was encoded as an array and is decoded into a narrower struct fails
		// with errArrayStruct anyway, but the options are not worth the risk.
		if kind == shadowMsgpack && sf.Name == "_msgpack" {
			return false
		}
		if kind != shadowJSON || !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" {
			name = sf.Name
		}
		name = foldName(name)
		if _, ok := names[name]; ok {
			return false
		}
		names[name] = struct{}{}
	}
	return true
}

// foldName returns the name in a form that is the same for the names that strings.EqualFold
// finds equal: each letter replaced by the least of the letters it folds to.
func foldName(name string) string {
	var b strings.Builder
	for _, r := range name {
		least := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			least = min(least, f)
		}
		b.WriteRune(least)
	}
	return b.String()
}

// shadowOf returns the shadow of a struct type for the sorted and distinct names of its fields.
func shadowOf(t reflect.Type, kind shadowKind, format Format, top []string) (*shadow, bool) {
	fields := make([]reflect.StructField, 0, len(top))
	dst := make([]int, 0, len(top))
	for _, name := range top {
		sf, ok := t.FieldByName(name)
		if !ok || !sf.IsExported() || len(sf.Index) != 1 {
			return nil, false
		}
		dst = append(dst, sf.Index[0])
		fields = append(fields, reflect.StructField{Name: sf.Name, Type: sf.Type, Tag: sf.Tag})
	}

	key := shadowKey{typ: t, format: format.Name(), fields: strings.Join(top, ",")}
	shadowMu.Lock()
	defer shadowMu.Unlock()
	if s, ok := shadowCache[key]; ok {
		return s, true
	}
	if shadowCount[t] >= maxShadows {
		return nil, false
	}

	s := &shadow{typ: reflect.StructOf(fields), dst: dst, unmarshal: format.Unmarshal}
	s.pool.New = func() any {
		ptr := reflect.New(s.typ)
		return &shadowValue{ptr: ptr, iface: ptr.Interface()}
	}
	shadowCache[key] = s
	shadowCount[t]++
	return s, true
}

// decode decodes the data into the fields of dst, which is a struct value.
func (s *shadow) decode(bz []byte, dst reflect.Value) error {
	sv := s.pool.Get().(*shadowValue)
	src := sv.ptr.Elem()
	defer func() {
		src.SetZero()
		s.pool.Put(sv)
	}()

	if err := s.unmarshal(bz, sv.iface); err != nil {
		return err
	}
	for i, d := range s.dst {
		dst.Field(d).Set(src.Field(i))
	}
	return nil
}
