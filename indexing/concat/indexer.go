package concat

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/be"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/schema"
)

// Indexer is an indexer for a struct type that generates keys base on struct fields for a btree index.
type Indexer[T any] struct {
	extractor  schema.PathExtractor[T]
	encoder    codec.Encoder[any]
	components []Component
	queries    []string
}

// New creates a new indexer for the given struct type and components.
func New[T any](
	extractor schema.PathExtractor[T],
	encoder codec.Encoder[any],
	comps ...Component,
) (*Indexer[T], error) {
	return &Indexer[T]{
		encoder:    encoder,
		extractor:  extractor,
		components: comps,
		queries:    calculateQueries(comps),
	}, nil
}

func calculateQueries(comps []Component) []string {
	lookups := make([]string, 0, len(comps))

	lookups = append(lookups, fmt.Sprintf("queryable(%s, '=,>,>=,<,<=')", comps[0].path))

	for i, comp := range comps[1:] {
		parts := make([]string, 0, i+1)
		for _, c := range comps[:i+1] {
			parts = append(parts, fmt.Sprintf("queryable(%s, '=')", c.path))
		}
		parts = append(parts, fmt.Sprintf("queryable(%s, '=,>,>=,<,<=')", comp.path))

		lookups = append(lookups, strings.Join(parts, " and "))
	}

	return lookups
}

// Index implements the Indexer interface.
func (si *Indexer[T]) Index(v *T, set bool) ([]kvutil.RawKVPair, error) {
	if v == nil {
		return nil, nil
	}

	keys, err := si.composeKeys(*v)
	if err != nil {
		return nil, err
	}

	pairs := make([]kvutil.RawKVPair, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, kvutil.RawKVPair{Key: key, Value: nil})
	}
	return pairs, nil
}

func (si *Indexer[T]) composeKeys(v T) ([][]byte, error) {
	var keys [][]byte
	for _, comp := range si.components {
		ev, err := si.extractor.ExtractPath(v, comp.path)
		if err != nil {
			return nil, fmt.Errorf("failed to extract path %s: %w", comp.path, err)
		}

		suffixes, err := si.encode(ev, comp)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
		}

		if len(keys) == 0 {
			keys = suffixes
		} else {
			if len(suffixes) == 1 {
				for i, k := range keys {
					keys[i] = append(k, suffixes[0]...)
				}
			} else {
				keys = propagateKeys(keys, suffixes)
			}
		}
	}

	return keys, nil
}

func (si *Indexer[T]) encode(v any, comp Component) ([][]byte, error) {
	rv, ok := v.(reflect.Value)
	if !ok {
		rv = reflect.ValueOf(v)
	}

	if (rv.Kind() == reflect.Array || rv.Kind() == reflect.Slice) && rv.Type().Elem().Kind() != reflect.Uint8 {
		return si.encodeArrayRV(comp, rv)
	}

	bz, err := si.encodeSingleRV(comp, rv)
	if err != nil {
		return nil, err
	}
	return [][]byte{bz}, nil
}

func (si *Indexer[T]) encodeArrayRV(comp Component, rv reflect.Value) ([][]byte, error) {
	var keys [][]byte
	for i := 0; i < rv.Len(); i++ {
		k, err := si.encodeSingleRV(comp, rv.Index(i))
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}

func (si *Indexer[T]) encodeSingleRV(comp Component, rv reflect.Value) ([]byte, error) {
	v := rv.Interface()

	if comp.convertTo != nil {
		v = convertRVToType(rv, comp.convertTo)
	}

	bz, err := si.encoder.Encode(v)
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}
	bz = be.PadOrTruncRight(bz, comp.size)

	if comp.descending {
		bz = lex.Invert(bz)
	}

	if comp.typed {
		k := rv.Kind()
		if k == reflect.Ptr && rv.IsNil() {
			k = reflect.Invalid
		}
		bz = append([]byte{byte(k)}, bz...)
	}

	return bz, nil
}

func convertRVToType(rv reflect.Value, t reflect.Type) any {
	irv := reflect.Indirect(rv)
	if !irv.Type().ConvertibleTo(t) {
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
			newKeys = append(newKeys, append(k, s...))
		}
	}

	return newKeys
}

// Lookup implements the Indexer interface.
func (si *Indexer[T]) Lookup(args ...any) (kvutil.Iterator[[]byte, expr.Range[[]byte]], error) {
	exs, err := si.verifyExprs(args)
	if err != nil {
		return nil, fmt.Errorf("invalid lookup arguments: %w", err)
	}

	var pars []expr.Range[[]byte]
	for _, comp := range si.components {
		e, ok := exs[comp.path]
		if !ok {
			// TODO: Remove this as it doesn't help with concat index when prior components are missing!
			e = expr.NewRange[any](nil, nil)
		}

		switch e := e.(type) {
		case expr.Exact[any]:
			rv := reflect.ValueOf(e.Value())
			v, err := si.encodeSingleRV(comp, rv)
			if err != nil {
				return nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
			}

			pars = expandRanges(pars, expr.NewRange(expr.NewBound(v, false), expr.NewBound(v, false)))
		case expr.Range[any]:
			r, err := si.encodeRange(comp, e)
			if err != nil {
				return nil, fmt.Errorf("failed to encode range for %s: %w", comp.path, err)
			}

			pars = expandRanges(pars, r)
		case expr.Set[any]:
			ranges := make([]expr.Range[[]byte], 0, len(e.Values()))
			for _, v := range e.Values() {
				rv := reflect.ValueOf(v)
				bz, err := si.encodeSingleRV(comp, rv)
				if err != nil {
					return nil, fmt.Errorf("failed to encode value for %s: %w", comp.path, err)
				}
				ranges = append(ranges, expr.NewRange(expr.NewBound(bz, false), expr.NewBound(bz, false)))
			}

			pars = expandRanges(pars, ranges...)
		default:
			return nil, fmt.Errorf("unsupported expression type %T", e)
		}
	}

	return iters.Slice(pars), nil
}

func (si *Indexer[T]) verifyExprs(args []any) (map[string]any, error) {
	if len(args) > len(si.components) {
		return nil, fmt.Errorf("too many arguments %d, expected %d", len(args), len(si.components))
	}

	exs := make(map[string]any)
	for _, arg := range args {
		e, ok := arg.(expr.Assigned)
		if !ok {
			return nil, fmt.Errorf("unsupported argument type %T", arg)
		}

		if si.findComponent(e.Name()) == nil {
			return nil, fmt.Errorf("unsupported path %s", e.Name())
		}

		if old, ok := exs[e.Name()]; ok {
			return nil, fmt.Errorf("duplicate path %s in %v and %v", e.Name(), old, e)
		}
		exs[e.Name()] = e.Expression()
	}
	return exs, nil
}

func (si *Indexer[T]) encodeRange(comp Component, r expr.Range[any]) (expr.Range[[]byte], error) {
	var low, high []byte
	if r.Low().IsEmpty() {
		if comp.descending {
			low = bytes.Repeat([]byte{0xff}, comp.size)
		} else {
			low = make([]byte, comp.size)
		}
	} else {
		var err error
		rv := reflect.ValueOf(r.Low().Value())
		low, err = si.encodeSingleRV(comp, rv)
		if err != nil {
			return expr.Range[[]byte]{}, fmt.Errorf("failed to encode low value: %w", err)
		}
	}

	if r.High().IsEmpty() {
		if comp.descending {
			high = make([]byte, comp.size)
		} else {
			high = bytes.Repeat([]byte{0xff}, comp.size)
		}
	} else {
		var err error
		rv := reflect.ValueOf(r.High().Value())
		high, err = si.encodeSingleRV(comp, rv)
		if err != nil {
			return expr.Range[[]byte]{}, fmt.Errorf("failed to encode high value: %w", err)
		}
	}

	return expr.NewRange(expr.NewBound(low, r.Low().Exclusive()), expr.NewBound(high, r.High().Exclusive())), nil
}

func (si *Indexer[T]) findComponent(path string) *Component {
	for _, comp := range si.components {
		if comp.path == path {
			return &comp
		}
	}
	return nil
}

func expandRanges(pars []expr.Range[[]byte], elems ...expr.Range[[]byte]) []expr.Range[[]byte] {
	if len(pars) == 0 {
		return elems
	}

	var newPars []expr.Range[[]byte]
	for _, p := range pars {
		for _, e := range elems {
			newPars = append(newPars, appendRange(p, e))
		}
	}
	return newPars
}

func appendRange(p1 expr.Range[[]byte], p2 expr.Range[[]byte]) expr.Range[[]byte] {
	p1Low := p1.Low()
	p1High := p1.High()
	if p1.Low().Exclusive() && !p1Low.IsEmpty() {
		p1Low = expr.NewBound(lex.Increment(p1Low.Value()), false)
	}
	if p1.High().Exclusive() && !p1High.IsEmpty() {
		p1High = expr.NewBound(lex.Decrement(p1High.Value()), false)
	}
	p1 = expr.NewRange(p1Low, p1High)

	return expr.NewRange(
		expr.NewBound(
			append(p1.Low().Value(), p2.Low().Value()...),
			p2.Low().Exclusive(),
		),
		expr.NewBound(
			append(p1.High().Value(), p2.High().Value()...),
			p2.High().Exclusive(),
		),
	)
}

// SupportedQueries implements the Indexer interface.
func (si *Indexer[T]) SupportedQueries() []string {
	return si.queries
}

// SupportedValues implements the Indexer interface.
func (si *Indexer[T]) SupportedValues() []string {
	return nil
}
