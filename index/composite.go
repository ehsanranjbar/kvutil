package index

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

// FieldsIndexer is an indexer whose keys are the values of fields of a record, one after the
// other. It finds records by the first fields of the index: a field can be used if all the
// fields before it are given exact values.
type FieldsIndexer[V any] struct {
	parts []field.Component[V]
}

// ByFields returns the indexer of the given fields, in the order that they have in the keys.
// It panics if there is no field, or if a field is given twice.
func ByFields[V any](parts ...field.Component[V]) *FieldsIndexer[V] {
	if len(parts) == 0 {
		panic("index: an index needs one field at least")
	}
	paths := map[string]struct{}{}
	for _, part := range parts {
		if _, ok := paths[part.Path()]; ok {
			panic(fmt.Sprintf("index: the field %s is in the index twice", part.Path()))
		}
		paths[part.Path()] = struct{}{}
	}
	return &FieldsIndexer[V]{parts: parts}
}

// Fields returns the fields of the indexer, in the order that they have in the keys.
func (x *FieldsIndexer[V]) Fields() []field.Component[V] {
	return slices.Clone(x.parts)
}

// Index implements the Indexer interface.
// A record that has several values for a field has a key for each of them, and a record that
// has no value for a field is not in the index.
func (x *FieldsIndexer[V]) Index(v *V) ([]Entry, error) {
	if v == nil {
		return nil, nil
	}

	keys := [][]byte{nil}
	for _, part := range x.parts {
		values, err := part.Keys(v)
		if err != nil {
			return nil, err
		}
		keys = product(keys, values)
	}

	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, Entry{Key: key})
	}
	return entries, nil
}

// product returns every key followed by every suffix.
func product(keys, suffixes [][]byte) [][]byte {
	result := make([][]byte, 0, len(keys)*len(suffixes))
	for _, key := range keys {
		for _, suffix := range suffixes {
			result = append(result, append(bytes.Clone(key), suffix...))
		}
	}
	return result
}

// Descriptor implements the Indexer interface.
func (x *FieldsIndexer[V]) Descriptor() Descriptor {
	desc := Descriptor{Operators: []cond.Op{cond.OpEq, cond.OpIn, cond.OpRange}}
	encodings := make([]string, 0, len(x.parts))
	var versions []string
	for _, part := range x.parts {
		desc.Fields = append(desc.Fields, FieldDescriptor{Path: part.Path(), Descending: part.Descending()})
		desc.MultiEntry = desc.MultiEntry || part.Multi()
		if v, ok := part.(interface{ Version() string }); ok && v.Version() != "" {
			versions = append(versions, part.Path()+"@"+v.Version())
		}

		encoding := part.CodecName()
		if part.Descending() {
			encoding += " desc"
		}
		encodings = append(encodings, encoding)
	}
	desc.Encoding = strings.Join(encodings, ",")
	desc.Version = strings.Join(versions, ",")
	return desc
}

// Ranges implements the RangePlanner interface.
// The comparisons on the first fields of the index are turned into ranges, up to the first
// field that has no comparison or that is compared with a range. Every other condition is
// returned as the residual.
func (x *FieldsIndexer[V]) Ranges(conds ...cond.Cond[V]) ([]Bounds, []cond.Cond[V], error) {
	// The comparisons that the index may use, by their fields. Several ranges on a field are
	// one range, unless the field may have several values, as different values can satisfy
	// the ranges. Anything else that comes after the first comparison of a field is left to be
	// checked on the records.
	var residual []cond.Cond[V]
	byPath := map[string]*cond.Comparison[V]{}
	for _, cnd := range flatten(conds) {
		c, ok := cnd.(*cond.Comparison[V])
		if !ok {
			residual = append(residual, cnd)
			continue
		}
		if err := c.Err(); err != nil {
			return nil, nil, err
		}

		part := x.part(c.Path)
		if part == nil || part.CodecName() != c.Codec {
			residual = append(residual, cnd)
			continue
		}

		first, ok := byPath[c.Path]
		switch {
		case !ok:
			byPath[c.Path] = c
		case first.Op == cond.OpRange && c.Op == cond.OpRange && !first.Multi && !c.Multi:
			byPath[c.Path] = intersect(first, c)
		default:
			residual = append(residual, cnd)
		}
	}

	var (
		prefixes = [][]byte{nil}
		ranges   []Bounds
		used     = map[string]struct{}{}
	)
parts:
	for _, part := range x.parts {
		c, ok := byPath[part.Path()]
		if !ok {
			break
		}
		used[part.Path()] = struct{}{}

		switch c.Op {
		case cond.OpEq, cond.OpIn:
			values := make([][]byte, 0, len(c.Values))
			for _, value := range c.Values {
				values = append(values, keyPart(part, value))
			}
			prefixes = product(prefixes, values)

		case cond.OpRange:
			for _, prefix := range prefixes {
				ranges = append(ranges, rangeOf(prefix, part, c))
			}
			// Nothing after a range can narrow it.
			break parts
		}
	}

	if ranges == nil {
		for _, prefix := range prefixes {
			if len(prefix) == 0 {
				ranges = append(ranges, Bounds{})
				continue
			}
			ranges = append(ranges, kvutil.Only(prefix))
		}
	}

	// In the order of the fields, so that the plan of the same conditions is always the same.
	for _, part := range x.parts {
		if _, ok := used[part.Path()]; ok {
			continue
		}
		if c, ok := byPath[part.Path()]; ok {
			residual = append(residual, c)
		}
	}
	return ranges, residual, nil
}

// flatten returns the conditions with the ones that are conjunctions replaced by their conditions.
func flatten[V any](conds []cond.Cond[V]) []cond.Cond[V] {
	var flat []cond.Cond[V]
	for _, cnd := range conds {
		if and, ok := cnd.(*cond.Conjunction[V]); ok {
			flat = append(flat, flatten(and.Conds)...)
			continue
		}
		flat = append(flat, cnd)
	}
	return flat
}

func (x *FieldsIndexer[V]) part(path string) field.Component[V] {
	for _, part := range x.parts {
		if part.Path() == path {
			return part
		}
	}
	return nil
}

// intersect returns the comparison of a field being in both of two ranges.
func intersect[V any](a, b *cond.Comparison[V]) *cond.Comparison[V] {
	c := *a
	if b.Low != nil {
		if cmp := compareBounds(c.Low, b.Low); c.Low == nil || cmp < 0 || (cmp == 0 && b.Low.Exclusive) {
			c.Low = b.Low
		}
	}
	if b.High != nil {
		if cmp := compareBounds(c.High, b.High); c.High == nil || cmp > 0 || (cmp == 0 && b.High.Exclusive) {
			c.High = b.High
		}
	}
	return &c
}

func compareBounds(a, b *cond.Bound) int {
	if a == nil || b == nil {
		return 0
	}
	return bytes.Compare(a.Value, b.Value)
}

// keyPart returns an encoded value of a comparison as it is in the keys of the index.
func keyPart[V any](part field.Component[V], value []byte) []byte {
	if part.Descending() {
		return lex.Invert(bytes.Clone(value))
	}
	return value
}

// rangeOf returns the range of the keys that begin with the prefix and whose next part is in
// the range of a comparison.
func rangeOf[V any](prefix []byte, part field.Component[V], c *cond.Comparison[V]) Bounds {
	low, high := c.Low, c.High
	if part.Descending() {
		// The greater a value is, the earlier its key comes.
		low, high = high, low
	}

	var r Bounds
	switch {
	case low != nil && low.Exclusive:
		r = r.After(append(bytes.Clone(prefix), keyPart(part, low.Value)...))
	case low != nil:
		r = r.From(append(bytes.Clone(prefix), keyPart(part, low.Value)...))
	case len(prefix) > 0:
		// An open side still ends where the keys of the prefix do.
		r = r.From(prefix)
	}
	switch {
	case high != nil && high.Exclusive:
		r = r.Until(append(bytes.Clone(prefix), keyPart(part, high.Value)...))
	case high != nil:
		r = r.Through(append(bytes.Clone(prefix), keyPart(part, high.Value)...))
	case len(prefix) > 0:
		r = r.Through(prefix)
	}
	return r
}

// conditional is an indexer of the records that satisfy a condition only.
type conditional[V any] struct {
	Indexer[V]
	cnd cond.Cond[V]
}

// filtered wraps an indexer so that only the records that satisfy a condition are in its index.
func filtered[V any](indexer Indexer[V], cnd cond.Cond[V]) Indexer[V] {
	return &conditional[V]{Indexer: indexer, cnd: cnd}
}

// Index implements the Indexer interface.
func (c *conditional[V]) Index(v *V) ([]Entry, error) {
	ok, err := c.cnd.Match(v)
	if err != nil {
		return nil, fmt.Errorf("failed to check the condition of the index: %w", err)
	}
	if !ok {
		return nil, nil
	}
	return c.Indexer.Index(v)
}

// Descriptor implements the Indexer interface.
func (c *conditional[V]) Descriptor() Descriptor {
	desc := c.Indexer.Descriptor()
	desc.Where = c.cnd.String()
	desc.Condition = c.cnd
	return desc
}

// Fields returns the fields of the wrapped indexer, if it is one of fields.
func (c *conditional[V]) Fields() []field.Component[V] {
	if f, ok := c.Indexer.(fielded[V]); ok {
		return f.Fields()
	}
	return nil
}

// Ranges implements the RangePlanner interface, if the wrapped indexer does.
func (c *conditional[V]) Ranges(conds ...cond.Cond[V]) ([]Bounds, []cond.Cond[V], error) {
	l, ok := c.Indexer.(RangePlanner[V])
	if !ok {
		return nil, nil, errors.New("the indexer cannot be looked up with conditions")
	}
	return l.Ranges(conds...)
}
