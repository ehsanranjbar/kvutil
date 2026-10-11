// Package field describes the fields of records: what indexes find records by, and what
// conditions on records are written with.
package field

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
)

// Field is a named property of the records of the type V, whose values are of the type F.
// It knows how to take its values from a record and how to encode them so that they sort as
// they compare. A record may have no value for a field, or several.
type Field[V, F any] struct {
	path  string
	get   func(*V) []F
	multi bool
	desc  bool
	codec codec.KeyCodec[F]
}

func newField[V, F any](path string, get func(*V) []F, multi bool, c []codec.KeyCodec[F]) Field[V, F] {
	f := Field[V, F]{path: path, get: get, multi: multi}
	switch len(c) {
	case 0:
		kc, err := codec.KeyFor[F]()
		if err != nil {
			// A mistake in the definition, which is made in code when the program starts.
			panic(fmt.Sprintf("field %q: %v; give the field a key codec", path, err))
		}
		f.codec = kc
	case 1:
		f.codec = c[0]
	default:
		panic(fmt.Sprintf("field %q: more than one key codec", path))
	}
	return f
}

// Of defines a field by a function that returns its value for a record.
// The values are encoded with the key codec of their type, unless one is given.
// It panics if the type has no key codec and none is given.
func Of[V, F any](path string, get func(*V) F, c ...codec.KeyCodec[F]) Field[V, F] {
	return newField(path, func(v *V) []F { return []F{get(v)} }, false, c)
}

// Each defines a field by a function that returns several values for a record, like its tags.
// A record is found by each of them, and a record with no value is not found by the field.
// It panics if the type of the values has no key codec and none is given.
func Each[V, F any](path string, get func(*V) []F, c ...codec.KeyCodec[F]) Field[V, F] {
	return newField(path, get, true, c)
}

// Path defines a field by the path of a field of a struct, which is read by reflection.
// The parts of the path are the names of the fields, with dots between them.
// The field of the struct must be of the type F, a pointer to it, in which case a record with
// a nil pointer has no value, or a slice of it, in which case a record has each of its elements
// as a value. It panics if the struct has no such field, if the field is of another type,
// or if the type has no key codec and none is given.
func Path[V, F any](path string, c ...codec.KeyCodec[F]) Field[V, F] {
	rt := reflect.TypeFor[V]()
	ft := reflect.TypeFor[F]()

	var index []int
	t := rt
	for _, part := range strings.Split(path, ".") {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			panic(fmt.Sprintf("field %q: %s has no fields", path, t))
		}
		sf, ok := t.FieldByName(part)
		if !ok {
			panic(fmt.Sprintf("field %q: %s has no field %s", path, t, part))
		}
		if !sf.IsExported() {
			panic(fmt.Sprintf("field %q: the field %s of %s is not exported", path, part, t))
		}
		index = append(index, sf.Index...)
		t = sf.Type
	}

	// value returns the field of a record, unless a pointer on the way to it is nil.
	value := func(v *V) (reflect.Value, bool) {
		rv := reflect.ValueOf(v).Elem()
		for rv.Kind() == reflect.Pointer {
			if rv.IsNil() {
				return rv, false
			}
			rv = rv.Elem()
		}
		fv, err := rv.FieldByIndexErr(index)
		return fv, err == nil
	}

	switch {
	case t == ft:
		return newField(path, func(v *V) []F {
			fv, ok := value(v)
			if !ok {
				return nil
			}
			return []F{fv.Interface().(F)}
		}, false, c)

	case t.Kind() == reflect.Pointer && t.Elem() == ft:
		return newField(path, func(v *V) []F {
			fv, ok := value(v)
			if !ok || fv.IsNil() {
				return nil
			}
			return []F{fv.Elem().Interface().(F)}
		}, false, c)

	case t.Kind() == reflect.Slice && t.Elem() == ft:
		return newField(path, func(v *V) []F {
			fv, ok := value(v)
			if !ok {
				return nil
			}
			values := make([]F, fv.Len())
			for i := range values {
				values[i] = fv.Index(i).Interface().(F)
			}
			return values
		}, true, c)
	}
	panic(fmt.Sprintf("field %q: the field of %s is a %s, not a %s", path, rt, t, ft))
}

// Desc returns the field with its values sorting from the greatest to the least in the keys
// of an index.
func (f Field[V, F]) Desc() Field[V, F] {
	f.desc = true
	return f
}

// Path returns the path of the field, which names it.
func (f Field[V, F]) Path() string {
	return f.path
}

// Descending reports whether the values of the field sort from the greatest to the least.
func (f Field[V, F]) Descending() bool {
	return f.desc
}

// Multi reports whether a record may have several values for the field.
func (f Field[V, F]) Multi() bool {
	return f.multi
}

// Codec returns the key codec of the values of the field.
func (f Field[V, F]) Codec() codec.KeyCodec[F] {
	return f.codec
}

// CodecName returns the name of the key codec of the values of the field.
func (f Field[V, F]) CodecName() string {
	return f.codec.Name()
}

// Values returns the values that a record has for the field.
func (f Field[V, F]) Values(v *V) []F {
	return f.get(v)
}

// Encode encodes a value of the field in the form that marks where it ends, and that sorts as
// the values compare. It is the form that conditions compare the values in.
func (f Field[V, F]) Encode(value F) ([]byte, error) {
	bz, err := f.codec.Append(nil, value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode %s: %w", f.path, err)
	}
	return bz, nil
}

// ascending returns the encoded values that a record has for the field.
func (f Field[V, F]) ascending(v *V) ([][]byte, error) {
	values := f.get(v)
	keys := make([][]byte, 0, len(values))
	for _, value := range values {
		key, err := f.Encode(value)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Keys implements the Component interface.
func (f Field[V, F]) Keys(v *V) ([][]byte, error) {
	keys, err := f.ascending(v)
	if err != nil || !f.desc {
		return keys, err
	}
	for _, key := range keys {
		lex.Invert(key)
	}
	return keys, nil
}

// Component is a field as a part of the keys of an index, whatever the type of its values is.
// It is implemented by Field.
type Component[V any] interface {
	// Path returns the path of the field, which names it.
	Path() string
	// Descending reports whether the values sort from the greatest to the least in the keys.
	Descending() bool
	// Multi reports whether a record may have several values.
	Multi() bool
	// CodecName returns the name of the key codec of the values.
	CodecName() string
	// Keys returns the values that a record has, each encoded as a part of a key: in the form
	// that marks where it ends, with its bytes inverted if the field is descending.
	Keys(v *V) ([][]byte, error)
}

// List returns fields of different types as the parts of the keys of an index.
func List[V any](parts ...Component[V]) []Component[V] {
	return parts
}
