package concat_test

import (
	"bytes"
	"cmp"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/index/concat"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

type Foo struct {
	Str   string
	Int   int64
	Small int8
	Float float64
	Ptr   *string
	Any   any
	Tags  []string
	Nums  []int
	Bytes []byte
	Inner Bar
}

type Bar struct {
	N int
}

func comps(paths ...string) []concat.Component {
	var comps []concat.Component
	for _, path := range paths {
		comps = append(comps, concat.NewComponent(path))
	}
	return comps
}

func keysOf(t *testing.T, indexer *concat.Indexer[Foo], v *Foo) [][]byte {
	t.Helper()

	entries, err := indexer.Index(v)
	require.NoError(t, err)

	var keys [][]byte
	for _, e := range entries {
		require.Nil(t, e.Value)
		keys = append(keys, e.Key)
	}
	return keys
}

func cat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func TestNew(t *testing.T) {
	_, err := concat.For[Foo]()
	require.Error(t, err)

	_, err = concat.For[Foo](comps("Str", "Int", "Str")...)
	require.Error(t, err)

	indexer, err := concat.New[Foo](schema.NewReflectPathExtractor[Foo](false), &lex.Encoder{}, comps("Str")...)
	require.NoError(t, err)
	require.NotNil(t, indexer)
}

func TestIndex(t *testing.T) {
	x := "x"
	end := []byte{0x00, 0x01}
	tests := []struct {
		name  string
		comps []concat.Component
		input *Foo
		want  [][]byte
	}{
		{
			name:  "Nil",
			comps: comps("Str"),
			input: nil,
			want:  nil,
		},
		{
			name:  "String",
			comps: comps("Str"),
			input: &Foo{Str: "ab"},
			want:  [][]byte{cat([]byte("ab"), end)},
		},
		{
			name:  "StringWithZero",
			comps: comps("Str"),
			input: &Foo{Str: "a\x00b"},
			want:  [][]byte{{'a', 0x00, 0xff, 'b', 0x00, 0x01}},
		},
		{
			// A number has a fixed size, so it is written as it is.
			name:  "StringAndNumbers",
			comps: comps("Str", "Int", "Small"),
			input: &Foo{Str: "ab", Int: 1, Small: -1},
			want:  [][]byte{cat([]byte("ab"), end, lex.EncodeInt64(1), lex.EncodeInt8(-1))},
		},
		{
			name:  "Descending",
			comps: []concat.Component{concat.NewComponent("Int").Desc(), concat.NewComponent("Str").Desc()},
			input: &Foo{Str: "ab", Int: 1},
			want:  [][]byte{cat(lex.Invert(lex.EncodeInt64(1)), lex.Invert(cat([]byte("ab"), end)))},
		},
		{
			name:  "NilPointer",
			comps: comps("Ptr", "Int"),
			input: &Foo{Int: 1},
			want:  [][]byte{cat(end, lex.EncodeInt64(1))},
		},
		{
			name:  "Pointer",
			comps: comps("Ptr", "Int"),
			input: &Foo{Ptr: &x, Int: 1},
			want:  [][]byte{cat([]byte("x"), end, lex.EncodeInt64(1))},
		},
		{
			name:  "Nested",
			comps: comps("Inner.N"),
			input: &Foo{Inner: Bar{N: 2}},
			want:  [][]byte{lex.EncodeInt64(2)},
		},
		{
			name:  "Bytes",
			comps: comps("Bytes"),
			input: &Foo{Bytes: []byte{1, 0}},
			want:  [][]byte{{1, 0x00, 0xff, 0x00, 0x01}},
		},
		{
			name:  "List",
			comps: comps("Tags", "Int"),
			input: &Foo{Tags: []string{"a", "b"}, Int: 1},
			want: [][]byte{
				cat([]byte("a"), end, lex.EncodeInt64(1)),
				cat([]byte("b"), end, lex.EncodeInt64(1)),
			},
		},
		{
			name:  "TwoLists",
			comps: comps("Tags", "Nums"),
			input: &Foo{Tags: []string{"a", "b"}, Nums: []int{1, 2}},
			want: [][]byte{
				cat([]byte("a"), end, lex.EncodeInt64(1)),
				cat([]byte("a"), end, lex.EncodeInt64(2)),
				cat([]byte("b"), end, lex.EncodeInt64(1)),
				cat([]byte("b"), end, lex.EncodeInt64(2)),
			},
		},
		{
			name:  "EmptyList",
			comps: comps("Str", "Tags"),
			input: &Foo{Str: "ab"},
			want:  nil,
		},
		{
			name:  "AnyOfDifferentTypes",
			comps: []concat.Component{concat.NewComponent("Any").Typed()},
			input: &Foo{Any: "ab"},
			want:  [][]byte{cat([]byte{byte(24)}, []byte("ab"), end)},
		},
		{
			name:  "AnyNil",
			comps: []concat.Component{concat.NewComponent("Any").Typed()},
			input: &Foo{},
			want:  [][]byte{cat([]byte{0}, end)},
		},
		{
			name:  "AnyAsFloat",
			comps: []concat.Component{concat.NewComponent("Any").AsFloat64()},
			input: &Foo{Any: int32(2)},
			want:  [][]byte{lex.EncodeFloat64(2)},
		},
		{
			name:  "NotANumberAsFloat",
			comps: []concat.Component{concat.NewComponent("Any").AsFloat64(), concat.NewComponent("Ptr").AsInt64()},
			input: &Foo{Any: "text"},
			want:  [][]byte{cat(lex.EncodeFloat64(math.NaN()), lex.EncodeInt64(0))},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexer, err := concat.For[Foo](test.comps...)
			require.NoError(t, err)
			require.Equal(t, test.want, keysOf(t, indexer, test.input))
		})
	}

	t.Run("Unsupported", func(t *testing.T) {
		indexer, err := concat.For[Foo](comps("Inner")...)
		require.NoError(t, err)
		_, err = indexer.Index(&Foo{})
		require.Error(t, err)

		indexer, err = concat.For[Foo](comps("Missing")...)
		require.NoError(t, err)
		_, err = indexer.Index(&Foo{})
		require.Error(t, err)
	})
}

func TestDescriptor(t *testing.T) {
	indexer, err := concat.For[Foo](concat.NewComponent("Str"), concat.NewComponent("Int").Desc())
	require.NoError(t, err)
	require.Equal(t, index.Descriptor{
		Fields:    []index.FieldDescriptor{{Path: "Str"}, {Path: "Int", Descending: true}},
		Operators: []index.Operator{index.Equal, index.Range},
		Encoding:  "concat(Str string,Int desc int64)",
	}, indexer.Descriptor())

	// A record has several keys if a component is a list, or might be one.
	for _, path := range []string{"Tags", "Any", "Missing"} {
		indexer, err = concat.For[Foo](comps("Str", path)...)
		require.NoError(t, err)
		require.True(t, indexer.Descriptor().MultiEntry, path)
	}

	indexer, err = concat.For[Foo](concat.NewComponent("Any").Typed().AsFloat64())
	require.NoError(t, err)
	require.Equal(t, "concat(Any typed as float64 interface)", indexer.Descriptor().Encoding)
}

// TestOrder checks that the keys sort as the values of their components do.
func TestOrder(t *testing.T) {
	indexer, err := concat.For[Foo](
		concat.NewComponent("Str"),
		concat.NewComponent("Int").Desc(),
		concat.NewComponent("Ptr").Desc(),
		concat.NewComponent("Small"),
	)
	require.NoError(t, err)

	rng := rand.New(rand.NewPCG(3, 5))
	strs := []string{"", "a", "a\x00", "a\x00\x01", "a\xff", "ab", "b"}
	random := func() *Foo {
		f := &Foo{
			Str:   strs[rng.IntN(len(strs))],
			Int:   rng.Int64N(5) - 2,
			Small: int8(rng.IntN(5) - 2),
		}
		if rng.IntN(3) > 0 {
			f.Ptr = &strs[rng.IntN(len(strs))]
		}
		return f
	}
	// A nil pointer is encoded as nothing, like an empty string.
	ptr := func(f *Foo) string {
		if f.Ptr == nil {
			return ""
		}
		return *f.Ptr
	}

	for range 20000 {
		a, b := random(), random()
		want := cmp.Or(
			cmp.Compare(a.Str, b.Str),
			cmp.Compare(b.Int, a.Int),
			cmp.Compare(ptr(b), ptr(a)),
			cmp.Compare(a.Small, b.Small),
		)
		ka, kb := keysOf(t, indexer, a)[0], keysOf(t, indexer, b)[0]
		require.Equal(t, want, bytes.Compare(ka, kb), "%+v and %+v", a, b)
	}
}

func assigned(path string, e any) expr.Assigned {
	return expr.NewAssigned(path, e)
}

func exact(v any) expr.Exact[any] {
	return expr.NewExact(v)
}

func between(low, high *expr.Bound[any]) expr.Range[any] {
	return expr.NewRange(low, high)
}

func bound(v any, exclusive bool) *expr.Bound[any] {
	return expr.NewBound(v, exclusive)
}

func keyRange(low []byte, lowExclusive bool, high []byte, highExclusive bool) index.KeyRange {
	var l, h *expr.Bound[[]byte]
	if low != nil {
		l = expr.NewBound(low, lowExclusive)
	}
	if high != nil {
		h = expr.NewBound(high, highExclusive)
	}
	return expr.NewRange(l, h)
}

func TestLookup(t *testing.T) {
	end := []byte{0x00, 0x01}
	str := func(s string) []byte { return cat([]byte(s), end) }

	tests := []struct {
		name     string
		comps    []concat.Component
		conds    []any
		want     []index.KeyRange
		residual []any
		wantErr  bool
	}{
		{
			name:  "NoCondition",
			comps: comps("Str", "Int"),
			want:  []index.KeyRange{{}},
		},
		{
			name:  "Exact",
			comps: comps("Str", "Int"),
			conds: []any{assigned("Str", exact("a"))},
			want:  []index.KeyRange{keyRange(str("a"), false, str("a"), false)},
		},
		{
			name:  "ExactOnAll",
			comps: comps("Str", "Int"),
			conds: []any{assigned("Int", exact(int64(7))), assigned("Str", exact("a"))},
			want: []index.KeyRange{
				keyRange(cat(str("a"), lex.EncodeInt64(7)), false, cat(str("a"), lex.EncodeInt64(7)), false),
			},
		},
		{
			// A value is converted to the type of its component.
			name:  "ExactOfAnotherType",
			comps: comps("Int"),
			conds: []any{assigned("Int", exact(7))},
			want:  []index.KeyRange{keyRange(lex.EncodeInt64(7), false, lex.EncodeInt64(7), false)},
		},
		{
			name:  "Set",
			comps: comps("Str", "Int"),
			conds: []any{assigned("Str", expr.NewSet[any]("a", "b")), assigned("Int", exact(1))},
			want: []index.KeyRange{
				keyRange(cat(str("a"), lex.EncodeInt64(1)), false, cat(str("a"), lex.EncodeInt64(1)), false),
				keyRange(cat(str("b"), lex.EncodeInt64(1)), false, cat(str("b"), lex.EncodeInt64(1)), false),
			},
		},
		{
			name:  "Range",
			comps: comps("Str"),
			conds: []any{assigned("Str", between(bound("a", false), bound("c", true)))},
			want:  []index.KeyRange{keyRange(str("a"), false, str("c"), true)},
		},
		{
			name:  "OpenRange",
			comps: comps("Str"),
			conds: []any{assigned("Str", between(bound("a", true), nil))},
			want:  []index.KeyRange{keyRange(str("a"), true, nil, false)},
		},
		{
			// The side that is open still ends with the keys of the first component.
			name:  "RangeAfterExact",
			comps: comps("Str", "Int"),
			conds: []any{assigned("Str", exact("a")), assigned("Int", between(nil, bound(5, false)))},
			want:  []index.KeyRange{keyRange(str("a"), false, cat(str("a"), lex.EncodeInt64(5)), false)},
		},
		{
			// The greater a value is, the earlier its key comes.
			name:  "DescendingRange",
			comps: []concat.Component{concat.NewComponent("Int").Desc()},
			conds: []any{assigned("Int", between(bound(1, false), bound(5, true)))},
			want: []index.KeyRange{
				keyRange(lex.Invert(lex.EncodeInt64(5)), true, lex.Invert(lex.EncodeInt64(1)), false),
			},
		},
		{
			name:  "TypedOpenRange",
			comps: []concat.Component{concat.NewComponent("Any").Typed()},
			conds: []any{assigned("Any", between(bound("a", false), nil))},
			want:  []index.KeyRange{keyRange(cat([]byte{24}, str("a")), false, []byte{24}, false)},
		},
		{
			// What comes after a range cannot narrow it.
			name:     "ConditionAfterRange",
			comps:    comps("Str", "Int"),
			conds:    []any{assigned("Str", between(bound("a", false), nil)), assigned("Int", exact(1))},
			want:     []index.KeyRange{keyRange(str("a"), false, nil, false)},
			residual: []any{assigned("Int", exact(1))},
		},
		{
			name:     "ConditionAfterGap",
			comps:    comps("Str", "Int", "Small"),
			conds:    []any{assigned("Small", exact(1)), assigned("Str", exact("a"))},
			want:     []index.KeyRange{keyRange(str("a"), false, str("a"), false)},
			residual: []any{assigned("Small", exact(1))},
		},
		{
			name:     "NoConditionOnFirst",
			comps:    comps("Str", "Int"),
			conds:    []any{assigned("Int", exact(1))},
			want:     []index.KeyRange{{}},
			residual: []any{assigned("Int", exact(1))},
		},
		{
			name:    "UnknownPath",
			comps:   comps("Str"),
			conds:   []any{assigned("Missing", exact("a"))},
			wantErr: true,
		},
		{
			name:    "SamePathTwice",
			comps:   comps("Str", "Int"),
			conds:   []any{assigned("Str", exact("a")), assigned("Str", exact("b"))},
			wantErr: true,
		},
		{
			name:    "NotACondition",
			comps:   comps("Str"),
			conds:   []any{"Str = a"},
			wantErr: true,
		},
		{
			name:    "UnsupportedExpression",
			comps:   comps("Str"),
			conds:   []any{assigned("Str", "a")},
			wantErr: true,
		},
		{
			name:    "ValueOfAnotherKind",
			comps:   comps("Int"),
			conds:   []any{assigned("Int", exact("seven"))},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexer, err := concat.For[Foo](test.comps...)
			require.NoError(t, err)

			ranges, residual, err := indexer.Lookup(test.conds...)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, ranges)
			require.Equal(t, test.residual, residual)
		})
	}
}

type pet struct {
	Owner string
	Age   int
	Name  string
}

// value returns the field of a pet at a path.
func value(p *pet, path string) any {
	switch path {
	case "Owner":
		return p.Owner
	case "Age":
		return p.Age
	}
	return p.Name
}

func compare(a, b any) int {
	if s, ok := a.(string); ok {
		return strings.Compare(s, b.(string))
	}
	return cmp.Compare(a.(int), b.(int))
}

// satisfies reports whether a pet satisfies conditions.
func satisfies(p *pet, conds []any) bool {
	for _, cond := range conds {
		a := cond.(expr.Assigned)
		v := value(p, a.Name())

		switch e := a.Expression().(type) {
		case expr.Exact[any]:
			if compare(v, e.Value()) != 0 {
				return false
			}
		case expr.Set[any]:
			if !slices.ContainsFunc(e.Values(), func(x any) bool { return compare(v, x) == 0 }) {
				return false
			}
		case expr.Range[any]:
			if low := e.Low(); !low.IsEmpty() {
				if c := compare(v, low.Value()); c < 0 || (c == 0 && low.Exclusive()) {
					return false
				}
			}
			if high := e.High(); !high.IsEmpty() {
				if c := compare(v, high.Value()); c > 0 || (c == 0 && high.Exclusive()) {
					return false
				}
			}
		}
	}
	return true
}

// TestLookupFindsWhatAScanFinds looks records up with random conditions, and checks that the
// index and the residual together find exactly the records that satisfy the conditions.
func TestLookupFindsWhatAScanFinds(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet](catalog.NewSchema().Namespace("pets"))
	indexer, err := concat.For[pet](
		concat.NewComponent("Owner"),
		concat.NewComponent("Age").Desc(),
		concat.NewComponent("Name"),
	)
	require.NoError(t, err)
	idx := index.New(store, "by_owner_age_name", indexer)

	rng := rand.New(rand.NewPCG(13, 17))
	owners := []string{"", "ann", "ann\x00", "bob", "cy"}
	names := []string{"a", "ab", "b", "c"}
	pets := map[int64]*pet{}
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for id := int64(1); id <= 200; id++ {
			pets[id] = &pet{
				Owner: owners[rng.IntN(len(owners))],
				Age:   rng.IntN(7) - 3,
				Name:  names[rng.IntN(len(names))],
			}
			if err := store.Instantiate(tx).Set(id, pets[id]); err != nil {
				return err
			}
		}
		return nil
	}))

	randomBound := func(v func() any) *expr.Bound[any] {
		if rng.IntN(4) == 0 {
			return nil
		}
		return bound(v(), rng.IntN(2) == 0)
	}
	// condition returns a random condition on a field, or none.
	condition := func(path string, v func() any) any {
		switch rng.IntN(4) {
		case 0:
			return assigned(path, exact(v()))
		case 1:
			return assigned(path, expr.NewSet(v(), v()))
		case 2:
			return assigned(path, between(randomBound(v), randomBound(v)))
		}
		return nil
	}

	narrowed := 0
	for range 3000 {
		var conds []any
		for _, cond := range []any{
			condition("Owner", func() any { return owners[rng.IntN(len(owners))] }),
			condition("Age", func() any { return rng.IntN(9) - 4 }),
			condition("Name", func() any { return names[rng.IntN(len(names))] }),
		} {
			if cond != nil {
				conds = append(conds, cond)
			}
		}
		rng.Shuffle(len(conds), func(i, j int) { conds[i], conds[j] = conds[j], conds[i] })

		var want []int64
		for id, p := range pets {
			if satisfies(p, conds) {
				want = append(want, id)
			}
		}
		slices.Sort(want)

		var got []int64
		err := db.View(func(tx kv.Tx) error {
			it, residual, err := idx.Instantiate(tx).Lookup(conds...)
			require.NoError(t, err)

			candidates, err := iters.From(it).Keys()
			require.NoError(t, err)
			if len(candidates) < len(pets) {
				narrowed++
			}
			for _, id := range candidates {
				if satisfies(pets[id], residual) {
					got = append(got, id)
				}
			}
			return nil
		})
		require.NoError(t, err)

		// A record must not come up twice.
		slices.Sort(got)
		require.Equal(t, want, got, "conditions %v", conds)
	}
	// Most of the lookups must have used the index to read less than everything.
	require.Greater(t, narrowed, 1500)
}

func TestUniqueConcat(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet](catalog.NewSchema().Namespace("pets"))
	indexer, err := concat.For[pet](concat.NewComponent("Owner"), concat.NewComponent("Name"))
	require.NoError(t, err)
	idx := index.NewUnique(store, "by_owner_name", indexer)
	require.True(t, idx.Descriptor().Unique)

	err = db.Update(func(tx kv.Tx) error {
		pets := store.Instantiate(tx)
		require.NoError(t, pets.Set(1, &pet{Owner: "ann", Name: "rex"}))
		require.NoError(t, pets.Set(2, &pet{Owner: "bob", Name: "rex"}))
		require.NoError(t, pets.Set(3, &pet{Owner: "ann", Name: "tom"}))
		require.ErrorIs(t, pets.Set(4, &pet{Owner: "ann", Name: "rex"}), index.ErrUniqueViolation)

		it, residual, err := idx.Instantiate(tx).Lookup(assigned("Owner", exact("ann")))
		require.NoError(t, err)
		require.Empty(t, residual)
		keys, err := iters.From(it).Keys()
		require.NoError(t, err)
		require.Equal(t, []int64{1, 3}, keys)
		return nil
	})
	require.NoError(t, err)
}
