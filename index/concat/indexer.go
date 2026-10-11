// Package concat provides an indexer whose keys are the values of several fields of a record,
// one after the other, which finds records by the first fields of the index.
package concat

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/schema"
)

// Indexer is an indexer for a struct type that generates keys base on struct fields for a btree index.
// A component whose values all have the same length, like a number, is written as it is.
// Any other component marks where it ends, so its values can have any length.
type Indexer[V any] struct {
	extractor  schema.PathExtractor[V]
	encoder    codec.Encoder[any]
	components []Component
	types      []reflect.Type // the types of the values of the components, where they are known
	multi      bool
}

// New creates a new indexer for the given struct type and components.
func New[V any](
	extractor schema.PathExtractor[V],
	encoder codec.Encoder[any],
	comps ...Component,
) (*Indexer[V], error) {
	if len(comps) == 0 {
		return nil, errors.New("an index needs one component at least")
	}

	si := &Indexer[V]{
		encoder:    encoder,
		extractor:  extractor,
		components: comps,
	}

	paths := map[string]struct{}{}
	for _, comp := range comps {
		if _, ok := paths[comp.path]; ok {
			return nil, fmt.Errorf("duplicate path %s", comp.path)
		}
		paths[comp.path] = struct{}{}

		t, known := fieldType(reflect.TypeFor[V](), comp.path)
		switch {
		case !known, t.Kind() == reflect.Interface:
			// Without its type, a component may be a list.
			si.multi = true
		case isList(t):
			si.multi = true
			t = t.Elem()
		}
		si.types = append(si.types, t)
	}
	return si, nil
}

// For creates a new indexer for a struct type, which takes the values of the components from
// the fields of the struct and encodes them so that they sort as they compare.
func For[V any](comps ...Component) (*Indexer[V], error) {
	return New[V](schema.NewReflectPathExtractor[V](false), &lex.Encoder{}, comps...)
}

// fieldType returns the type of the field of a struct type at a path, if it can be told.
func fieldType(rt reflect.Type, path string) (reflect.Type, bool) {
	t := rt
	for _, part := range strings.Split(path, ".") {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return nil, false
		}
		f, ok := t.FieldByName(part)
		if !ok {
			return nil, false
		}
		t = f.Type
	}
	return t, true
}

// fixed reports whether the values of a component are all encoded to the same number of bytes,
// in which case they do not have to mark where they end.
func (si *Indexer[V]) fixed(i int) bool {
	if si.components[i].convertTo != nil {
		return true
	}

	t := si.types[i]
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	case reflect.Array:
		return t.Elem().Kind() == reflect.Uint8
	}
	return false
}

// isList reports whether the values of a type are encoded as one key for each of their elements.
func isList(t reflect.Type) bool {
	return (t.Kind() == reflect.Array || t.Kind() == reflect.Slice) && t.Elem().Kind() != reflect.Uint8
}

// Index implements the Indexer interface.
func (si *Indexer[V]) Index(v *V) ([]index.Entry, error) {
	if v == nil {
		return nil, nil
	}

	keys, err := si.composeKeys(*v)
	if err != nil {
		return nil, err
	}

	entries := make([]index.Entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, index.Entry{Key: key})
	}
	return entries, nil
}

func (si *Indexer[V]) composeKeys(v V) ([][]byte, error) {
	keys := [][]byte{nil}
	for i, comp := range si.components {
		ev, err := si.extractor.ExtractPath(v, comp.path)
		if err != nil {
			return nil, fmt.Errorf("failed to extract path %s: %w", comp.path, err)
		}

		suffixes, err := si.encode(ev, i)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
		}
		keys = propagateKeys(keys, suffixes)
	}

	return keys, nil
}

func (si *Indexer[V]) encode(v any, i int) ([][]byte, error) {
	rv, ok := v.(reflect.Value)
	if !ok {
		rv = reflect.ValueOf(v)
	}
	if rv.IsValid() && rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}

	if rv.IsValid() && isList(rv.Type()) {
		return si.encodeArrayRV(i, rv)
	}

	bz, err := si.encodeSingleRV(i, rv)
	if err != nil {
		return nil, err
	}
	return [][]byte{bz}, nil
}

func (si *Indexer[V]) encodeArrayRV(i int, rv reflect.Value) ([][]byte, error) {
	var keys [][]byte
	for j := 0; j < rv.Len(); j++ {
		k, err := si.encodeSingleRV(i, rv.Index(j))
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}

	return keys, nil
}

// encodeSingleRV encodes a value as the component i of a key.
func (si *Indexer[V]) encodeSingleRV(i int, rv reflect.Value) ([]byte, error) {
	comp := si.components[i]
	if rv.IsValid() && rv.Kind() == reflect.Interface {
		rv = rv.Elem()
	}

	var v any
	switch {
	case comp.convertTo != nil:
		v = convertRVToType(rv, comp.convertTo)
	case rv.IsValid():
		v = rv.Interface()
	}

	var bz []byte
	if v != nil {
		var err error
		bz, err = si.encoder.Encode(v)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value: %w", err)
		}
	}

	if !si.fixed(i) {
		bz = lex.AppendTerminated(nil, bz)
	}
	if comp.descending {
		bz = lex.Invert(bytes.Clone(bz))
	}
	if comp.typed {
		bz = append([]byte{kindOf(rv)}, bz...)
	}

	return bz, nil
}

// encodeCondition encodes a value that is given in a condition as the component i of a key.
// The value is converted to the type of the component first, so that a condition can be
// written with a value of another type, like an int for a field that is an int64.
func (si *Indexer[V]) encodeCondition(i int, v any) ([]byte, error) {
	rv := reflect.ValueOf(v)
	t := si.types[i]
	for t != nil && t.Kind() == reflect.Pointer {
		// A pointer is encoded as what it points to.
		t = t.Elem()
	}
	if t != nil && t.Kind() != reflect.Interface && rv.IsValid() && rv.Type() != t {
		if !rv.Type().ConvertibleTo(t) {
			return nil, fmt.Errorf("a value of type %s cannot be used for a component of type %s", rv.Type(), t)
		}
		rv = rv.Convert(t)
	}
	return si.encodeSingleRV(i, rv)
}

// kindOf returns the byte that the values of a typed component begin with, which is the kind
// of the value itself and not of a pointer or an interface that holds it.
func kindOf(rv reflect.Value) byte {
	for rv.IsValid() && (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) {
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return byte(reflect.Invalid)
	}
	return byte(rv.Kind())
}

func convertRVToType(rv reflect.Value, t reflect.Type) any {
	var irv reflect.Value
	if rv.IsValid() {
		irv = reflect.Indirect(rv)
	}
	if !irv.IsValid() || !irv.Type().ConvertibleTo(t) {
		if t == float64Type {
			return math.NaN()
		}
		return reflect.Zero(t).Interface()
	}

	return irv.Convert(t).Interface()
}

func propagateKeys(keys [][]byte, suffixes [][]byte) [][]byte {
	newKeys := make([][]byte, 0, len(keys)*len(suffixes))
	for _, k := range keys {
		for _, s := range suffixes {
			newKeys = append(newKeys, append(bytes.Clone(k), s...))
		}
	}
	return newKeys
}

// Lookup implements the index.Lookuper interface.
// A condition is an expr.Assigned that gives the path of a component an expr.Exact, an expr.Set
// or an expr.Range. The conditions on the first components of the index are turned into ranges,
// up to the first component that has no condition or that has a range. The conditions on the
// components after it cannot narrow the ranges and are returned as the residual.
func (si *Indexer[V]) Lookup(conds ...any) ([]index.KeyRange, []any, error) {
	exs, err := si.verifyExprs(conds)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid lookup arguments: %w", err)
	}

	var (
		prefixes = [][]byte{nil}
		ranges   []index.KeyRange
		used     = map[string]struct{}{}
	)
components:
	for i, comp := range si.components {
		e, ok := exs[comp.path]
		if !ok {
			break
		}
		used[comp.path] = struct{}{}

		switch e := e.Expression().(type) {
		case expr.Exact[any]:
			bz, err := si.encodeCondition(i, e.Value())
			if err != nil {
				return nil, nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
			}
			prefixes = propagateKeys(prefixes, [][]byte{bz})

		case expr.Set[any]:
			values := make([][]byte, 0, len(e.Values()))
			for _, v := range e.Values() {
				bz, err := si.encodeCondition(i, v)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
				}
				values = append(values, bz)
			}
			prefixes = propagateKeys(prefixes, values)

		case expr.Range[any]:
			for _, prefix := range prefixes {
				r, err := si.encodeRange(prefix, i, e)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to encode range for %s: %w", comp.path, err)
				}
				ranges = append(ranges, r)
			}
			// Nothing after a range can narrow it.
			break components

		default:
			return nil, nil, fmt.Errorf("unsupported expression type %T", e)
		}
	}

	if ranges == nil {
		for _, prefix := range prefixes {
			if len(prefix) == 0 {
				ranges = append(ranges, index.KeyRange{})
				continue
			}
			b := expr.NewBound(prefix, false)
			ranges = append(ranges, expr.NewRange(b, b))
		}
	}

	var residual []any
	for _, cond := range conds {
		if _, ok := used[cond.(expr.Assigned).Name()]; !ok {
			residual = append(residual, cond)
		}
	}
	return ranges, residual, nil
}

func (si *Indexer[V]) verifyExprs(conds []any) (map[string]expr.Assigned, error) {
	exs := make(map[string]expr.Assigned)
	for _, cond := range conds {
		e, ok := cond.(expr.Assigned)
		if !ok {
			return nil, fmt.Errorf("unsupported argument type %T", cond)
		}

		if si.findComponent(e.Name()) == nil {
			return nil, fmt.Errorf("unsupported path %s", e.Name())
		}

		if old, ok := exs[e.Name()]; ok {
			return nil, fmt.Errorf("duplicate path %s in %v and %v", e.Name(), old, e)
		}

		exs[e.Name()] = e
	}

	return exs, nil
}

// encodeRange returns the range of the keys that begin with the prefix and whose next
// component, which is the component i, is in the given range of values.
func (si *Indexer[V]) encodeRange(prefix []byte, i int, r expr.Range[any]) (index.KeyRange, error) {
	low, high := r.Low(), r.High()
	if si.components[i].descending {
		// The greater a value is, the earlier its key comes.
		low, high = high, low
	}

	lowKey, err := si.encodeBound(prefix, i, low, high)
	if err != nil {
		return index.KeyRange{}, fmt.Errorf("failed to encode low value: %w", err)
	}
	highKey, err := si.encodeBound(prefix, i, high, low)
	if err != nil {
		return index.KeyRange{}, fmt.Errorf("failed to encode high value: %w", err)
	}
	return expr.NewRange(lowKey, highKey), nil
}

func (si *Indexer[V]) encodeBound(prefix []byte, i int, b, other *expr.Bound[any]) (*expr.Bound[[]byte], error) {
	if b.IsEmpty() {
		// An open side still ends where the keys of the prefix do and, if the component is typed,
		// where the values of the type of the other bound do.
		key := bytes.Clone(prefix)
		if si.components[i].typed && !other.IsEmpty() {
			bz, err := si.encodeCondition(i, other.Value())
			if err != nil {
				return nil, err
			}
			key = append(key, bz[0])
		}
		if len(key) == 0 {
			return nil, nil
		}
		return expr.NewBound(key, false), nil
	}

	bz, err := si.encodeCondition(i, b.Value())
	if err != nil {
		return nil, err
	}
	return expr.NewBound(append(bytes.Clone(prefix), bz...), b.Exclusive()), nil
}

func (si *Indexer[V]) findComponent(path string) *Component {
	for _, comp := range si.components {
		if comp.path == path {
			return &comp
		}
	}

	return nil
}

// Descriptor implements the Indexer interface.
func (si *Indexer[V]) Descriptor() index.Descriptor {
	fields := make([]index.FieldDescriptor, 0, len(si.components))
	comps := make([]string, 0, len(si.components))
	for i, comp := range si.components {
		fields = append(fields, index.FieldDescriptor{Path: comp.path, Descending: comp.descending})

		// The type is a part of the encoding, as it decides how a component is written.
		c := comp.String()
		if t := si.types[i]; t != nil {
			c += " " + t.Kind().String()
		}
		comps = append(comps, c)
	}

	return index.Descriptor{
		Fields:     fields,
		Operators:  []index.Operator{index.Equal, index.Range},
		MultiEntry: si.multi,
		Encoding:   "concat(" + strings.Join(comps, ",") + ")",
	}
}
