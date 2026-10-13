package index_test

import (
	"bytes"
	"cmp"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

func cat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func str(s string) []byte {
	return cat([]byte(s), []byte{0x00, 0x01})
}

func keysOf(t *testing.T, indexer index.Indexer[pet], p *pet) [][]byte {
	t.Helper()

	entries, err := indexer.Index(p)
	require.NoError(t, err)
	var keys [][]byte
	for _, e := range entries {
		keys = append(keys, e.Key)
	}
	return keys
}

func TestFieldsIndexer(t *testing.T) {
	p := &pet{Name: "rex", Owner: "ann", Age: 3, Tags: []string{"a", "b"}}

	tests := []struct {
		name   string
		fields []field.Component[pet]
		want   [][]byte
	}{
		// A string marks where it ends, and a number has a size of its own.
		{"One", field.List(nameField), [][]byte{str("rex")}},
		{"Two", field.List(ownerField, ageField), [][]byte{cat(str("ann"), lex.EncodeInt64(3))}},
		{"Descending", field.List(ageField.Desc(), nameField.Desc()),
			[][]byte{cat(lex.Invert(lex.EncodeInt64(3)), lex.Invert(str("rex")))}},
		// A key for each value of a field that has several.
		{"Several", field.List(tagsField, ageField),
			[][]byte{cat(str("a"), lex.EncodeInt64(3)), cat(str("b"), lex.EncodeInt64(3))}},
		// And no key if a field has no value.
		{"None", field.List(nameField, chipField), nil},
	}
	for _, test := range tests {
		require.Equal(t, test.want, keysOf(t, index.ByFields(test.fields...), p), test.name)
	}

	entries, err := index.ByFields(field.List(nameField)...).Index(nil)
	require.NoError(t, err)
	require.Empty(t, entries)

	require.Equal(t, index.Descriptor{
		Fields:     []index.FieldDescriptor{{Path: "Owner"}, {Path: "Age", Descending: true}, {Path: "Tags"}},
		Operators:  []cond.Op{cond.OpEq, cond.OpIn, cond.OpRange},
		MultiEntry: true,
		Encoding:   "string,int desc,string",
	}, index.ByFields(field.List(ownerField, ageField.Desc(), tagsField)...).Descriptor())
}

// TestCompositeOrder checks that the keys sort as the values of their fields do.
func TestCompositeOrder(t *testing.T) {
	indexer := index.ByFields(field.List(ownerField, ageField.Desc(), nameField.Desc(), ageField2)...)

	rng := rand.New(rand.NewPCG(3, 5))
	strs := []string{"", "a", "a\x00", "a\x00\x01", "a\xff", "ab", "b"}
	random := func() *pet {
		return &pet{
			Owner: strs[rng.IntN(len(strs))],
			Age:   rng.IntN(5) - 2,
			Name:  strs[rng.IntN(len(strs))],
			Chip:  strs[rng.IntN(len(strs))],
		}
	}

	for range 20000 {
		a, b := random(), random()
		want := cmp.Or(
			cmp.Compare(a.Owner, b.Owner),
			cmp.Compare(b.Age, a.Age),
			cmp.Compare(b.Name, a.Name),
			cmp.Compare(len(a.Chip), len(b.Chip)),
		)
		ka, kb := keysOf(t, indexer, a)[0], keysOf(t, indexer, b)[0]
		require.Equal(t, want, bytes.Compare(ka, kb), "%+v and %+v", a, b)
	}
}

// ageField2 is a second number in a key, after the fields that mark where they end.
var ageField2 = field.Of("ChipLength", func(p *pet) int { return len(p.Chip) })

func texts(conds []cond.Cond[pet]) []string {
	texts := []string{}
	for _, cnd := range conds {
		texts = append(texts, cnd.String())
	}
	slices.Sort(texts)
	return texts
}

func TestPlan(t *testing.T) {
	ownerAge := field.List(ownerField, ageField)
	hasTags := cond.Func("has_tags", func(p *pet) bool { return len(p.Tags) > 0 })

	tests := []struct {
		name     string
		fields   []field.Component[pet]
		conds    []cond.Cond[pet]
		want     []index.Bounds
		residual []string
	}{
		{
			name:   "NoCondition",
			fields: ownerAge,
			want:   []index.Bounds{{}},
		},
		{
			name:   "Exact",
			fields: ownerAge,
			conds:  []cond.Cond[pet]{ownerField.Eq("a")},
			want:   []index.Bounds{kvutil.Only(str("a"))},
		},
		{
			name:   "ExactOnAll",
			fields: ownerAge,
			conds:  []cond.Cond[pet]{ageField.Eq(7), ownerField.Eq("a")},
			want:   []index.Bounds{kvutil.Only(cat(str("a"), lex.EncodeInt64(7)))},
		},
		{
			name:   "In",
			fields: ownerAge,
			conds:  []cond.Cond[pet]{ownerField.In("a", "b"), ageField.Eq(1)},
			want: []index.Bounds{
				kvutil.Only(cat(str("a"), lex.EncodeInt64(1))),
				kvutil.Only(cat(str("b"), lex.EncodeInt64(1))),
			},
		},
		{
			name:   "InNothing",
			fields: ownerAge,
			conds:  []cond.Cond[pet]{ownerField.In()},
			want:   nil,
		},
		{
			name:   "Range",
			fields: field.List(nameField),
			conds:  []cond.Cond[pet]{nameField.Range(kvutil.From("a").Until("c"))},
			want:   []index.Bounds{kvutil.From(str("a")).Until(str("c"))},
		},
		{
			name:   "OpenRange",
			fields: field.List(nameField),
			conds:  []cond.Cond[pet]{nameField.Gt("a")},
			want:   []index.Bounds{kvutil.After(str("a"))},
		},
		{
			// The side that is open still ends with the keys of the first field.
			name:   "RangeAfterExact",
			fields: ownerAge,
			conds:  []cond.Cond[pet]{ownerField.Eq("a"), ageField.Lte(5)},
			want:   []index.Bounds{kvutil.From(str("a")).Through(cat(str("a"), lex.EncodeInt64(5)))},
		},
		{
			// Two ranges on a field are one range.
			name:   "TwoRanges",
			fields: field.List(ageField),
			conds:  []cond.Cond[pet]{ageField.Gte(1), ageField.Lt(5), ageField.Gt(0)},
			want:   []index.Bounds{kvutil.From(lex.EncodeInt64(1)).Until(lex.EncodeInt64(5))},
		},
		{
			// The greater a value is, the earlier its key comes.
			name:   "DescendingRange",
			fields: field.List(ageField.Desc()),
			conds:  []cond.Cond[pet]{ageField.Range(kvutil.From(1).Until(5))},
			want: []index.Bounds{
				kvutil.After(lex.Invert(lex.EncodeInt64(5))).Through(lex.Invert(lex.EncodeInt64(1))),
			},
		},
		{
			name:   "DescendingExact",
			fields: field.List(ageField.Desc()),
			conds:  []cond.Cond[pet]{ageField.Eq(5)},
			want:   []index.Bounds{kvutil.Only(lex.Invert(lex.EncodeInt64(5)))},
		},
		{
			// A condition inside a conjunction is used like one that is given on its own.
			name:   "Conjunction",
			fields: ownerAge,
			conds:  []cond.Cond[pet]{cond.And(ownerField.Eq("a"), cond.And(ageField.Eq(7)))},
			want:   []index.Bounds{kvutil.Only(cat(str("a"), lex.EncodeInt64(7)))},
		},
		{
			// What comes after a range cannot narrow it.
			name:     "ConditionAfterRange",
			fields:   ownerAge,
			conds:    []cond.Cond[pet]{ownerField.Gte("a"), ageField.Eq(1)},
			want:     []index.Bounds{kvutil.From(str("a"))},
			residual: []string{"Age = 1"},
		},
		{
			name:     "ConditionAfterGap",
			fields:   field.List(ownerField, nameField, ageField),
			conds:    []cond.Cond[pet]{ageField.Eq(1), ownerField.Eq("a")},
			want:     []index.Bounds{kvutil.Only(str("a"))},
			residual: []string{"Age = 1"},
		},
		{
			name:     "NoConditionOnFirst",
			fields:   ownerAge,
			conds:    []cond.Cond[pet]{ageField.Eq(1)},
			want:     []index.Bounds{{}},
			residual: []string{"Age = 1"},
		},
		{
			name:     "OtherFieldsAndConditions",
			fields:   ownerAge,
			conds:    []cond.Cond[pet]{ownerField.Eq("a"), nameField.Eq("rex"), hasTags, cond.Or(ageField.Eq(1), ageField.Eq(2)), cond.Not(ageField.Eq(3))},
			want:     []index.Bounds{kvutil.Only(str("a"))},
			residual: []string{"(Age = 1) or (Age = 2)", `Name = "rex"`, "has_tags()", "not (Age = 3)"},
		},
		{
			// Only the first comparison of a field is used, unless they are all ranges.
			name:     "SecondComparison",
			fields:   ownerAge,
			conds:    []cond.Cond[pet]{ownerField.Eq("a"), ownerField.Gt("0"), ownerField.In("a", "b")},
			want:     []index.Bounds{kvutil.Only(str("a"))},
			residual: []string{`Owner > "0"`, `Owner in ("a", "b")`},
		},
		{
			// A comparison of the same field with another encoding is of no use to the index.
			name:   "OtherEncoding",
			fields: field.List(nameField),
			conds: []cond.Cond[pet]{
				field.Of("Name", func(p *pet) []byte { return []byte(p.Name) }).Eq([]byte("a")),
			},
			want:     []index.Bounds{{}},
			residual: []string{`Name = []byte{0x61}`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ranges, residual, err := index.ByFields(test.fields...).Ranges(test.conds...)
			require.NoError(t, err)
			require.Equal(t, test.want, ranges)
			if test.residual == nil {
				test.residual = []string{}
			}
			require.Equal(t, test.residual, texts(residual))
		})
	}
}

// TestLookupFindsWhatAScanFinds looks records up with random conditions through several
// indexes, and checks that each of them finds exactly the records that satisfy the conditions.
func TestLookupFindsWhatAScanFinds(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet](schema.New().Namespace("pets"))
	indexes := []*index.Index[int64, pet]{
		index.Composite(store, "owner_age_name", field.List(ownerField, ageField.Desc(), nameField)),
		index.Composite(store, "name_owner", field.List(nameField.Desc(), ownerField)),
		index.Composite(store, "tag_age", field.List(tagsField, ageField)),
		index.On(store, "age", ageField).Index,
	}

	rng := rand.New(rand.NewPCG(13, 17))
	owners := []string{"", "ann", "ann\x00", "bob", "cy"}
	names := []string{"a", "ab", "b", "c"}
	allTags := []string{"cat", "dog", "old"}
	pets := map[int64]*pet{}
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for id := int64(1); id <= 200; id++ {
			p := &pet{
				Owner: owners[rng.IntN(len(owners))],
				Age:   rng.IntN(7) - 3,
				Name:  names[rng.IntN(len(names))],
			}
			for _, tag := range allTags {
				if rng.IntN(2) == 0 {
					p.Tags = append(p.Tags, tag)
				}
			}
			pets[id] = p
			if err := store.Instantiate(tx).Set(id, p); err != nil {
				return err
			}
		}
		return nil
	}))

	// comparison returns a random comparison of a field.
	comparison := func(f interface {
		eq() cond.Cond[pet]
		in() cond.Cond[pet]
		between() cond.Cond[pet]
	}) cond.Cond[pet] {
		switch rng.IntN(3) {
		case 0:
			return f.eq()
		case 1:
			return f.in()
		}
		return f.between()
	}
	owner := func() string { return owners[rng.IntN(len(owners))] }
	ageOf := func() int { return rng.IntN(9) - 4 }
	nameOf := func() string { return names[rng.IntN(len(names))] }
	tagOf := func() string { return allTags[rng.IntN(len(allTags))] }

	fields := []func() cond.Cond[pet]{
		func() cond.Cond[pet] { return comparison(random[string]{ownerField, owner, rng}) },
		func() cond.Cond[pet] { return comparison(random[int]{ageField, ageOf, rng}) },
		func() cond.Cond[pet] { return comparison(random[string]{nameField, nameOf, rng}) },
		func() cond.Cond[pet] { return comparison(random[string]{tagsField, tagOf, rng}) },
		// Two sides of a range, given apart.
		func() cond.Cond[pet] { return cond.And[pet](ageField.Gte(ageOf()), ageField.Lt(ageOf())) },
		func() cond.Cond[pet] { return cond.Or[pet](ownerField.Eq(owner()), nameField.Eq(nameOf())) },
		func() cond.Cond[pet] { return cond.Not[pet](tagsField.Eq(tagOf())) },
		func() cond.Cond[pet] {
			return cond.Func("even_age", func(p *pet) bool { return p.Age%2 == 0 })
		},
	}

	narrowed := 0
	for range 2000 {
		var conds []cond.Cond[pet]
		for _, f := range fields {
			if rng.IntN(3) == 0 {
				conds = append(conds, f())
			}
		}
		rng.Shuffle(len(conds), func(i, j int) { conds[i], conds[j] = conds[j], conds[i] })

		want := []int64{}
		for id, p := range pets {
			ok, err := cond.And(conds...).Match(p)
			require.NoError(t, err)
			if ok {
				want = append(want, id)
			}
		}
		slices.Sort(want)

		err := db.View(func(tx kv.Tx) error {
			for _, idx := range indexes {
				ins := idx.Instantiate(tx)

				// A record with no value for a field of an index is not in the index, so the
				// index can only answer for the records that have one.
				expected := want
				if idx.Name() == "tag_age" {
					expected = slices.DeleteFunc(slices.Clone(want), func(id int64) bool { return len(pets[id].Tags) == 0 })
				}

				// The same records both ways, with no record twice.
				forward := ids(t, must(ins.Lookup(conds...)))
				backward := ids(t, must(ins.ReverseLookup(conds...)))
				slices.Sort(forward)
				slices.Sort(backward)
				require.Equal(t, expected, forward, "%s: %v", idx.Name(), texts(conds))
				require.Equal(t, expected, backward, "%s backwards: %v", idx.Name(), texts(conds))

				ranges, _, err := ins.Plan(conds...)
				require.NoError(t, err)
				if len(ranges) != 1 || ranges[0] != (index.Bounds{}) {
					narrowed++
				}
			}
			return nil
		})
		require.NoError(t, err)
	}
	// The indexes must have been of use for a good part of the lookups.
	require.Greater(t, narrowed, 2000)
}

// random makes random comparisons of a field.
type random[F any] struct {
	field field.Field[pet, F]
	value func() F
	rng   *rand.Rand
}

func (r random[F]) eq() cond.Cond[pet] { return r.field.Eq(r.value()) }
func (r random[F]) in() cond.Cond[pet] { return r.field.In(r.value(), r.value()) }
func (r random[F]) between() cond.Cond[pet] {
	var rg kvutil.Range[F]
	switch r.rng.IntN(3) {
	case 0:
		rg = rg.From(r.value())
	case 1:
		rg = rg.After(r.value())
	}
	switch r.rng.IntN(3) {
	case 0:
		rg = rg.Until(r.value())
	case 1:
		rg = rg.Through(r.value())
	}
	return r.field.Range(rg)
}

func TestWhere(t *testing.T) {
	db := memkv.New()
	sch := schema.New()
	store := extstore.New[int64, pet](sch.Namespace("pets"))
	old := index.On(store, "old_by_name", nameField, index.Where(ageField.Gte(10)))
	tagged := index.New(store, "tagged", index.ByFields(field.List(ownerField)...),
		index.Where(cond.Func("has_tags", func(p *pet) bool { return len(p.Tags) > 0 })))
	require.NoError(t, sch.Check(db))

	// The condition is a part of what the index is.
	require.Equal(t, "Age >= 10", old.Descriptor().Where)
	require.Equal(t, "Age >= 10", old.Descriptor().Condition.(cond.Cond[pet]).String())
	require.Equal(t, "has_tags()", tagged.Descriptor().Where)

	err := db.Update(func(tx kv.Tx) error {
		pets, olds := store.Instantiate(tx), old.Instantiate(tx)
		require.NoError(t, pets.Set(1, &pet{Name: "a", Age: 12, Owner: "ann"}))
		require.NoError(t, pets.Set(2, &pet{Name: "b", Age: 3, Owner: "ann", Tags: []string{"x"}}))
		require.NoError(t, pets.Set(3, &pet{Name: "c", Age: 10, Owner: "bob", Tags: []string{"x"}}))

		// Only the records that satisfy the condition are in the index.
		require.Equal(t, []int64{1, 3}, ids(t, must(olds.Range(kvutil.All[string]()))))
		require.Equal(t, []int64{}, ids(t, must(olds.Match("b"))))
		require.Equal(t, []int64{2}, ids(t, must(tagged.Instantiate(tx).Lookup(ownerField.Eq("ann")))))

		// A record comes into the index when it starts to satisfy the condition, and leaves
		// it when it does not any more.
		require.NoError(t, pets.Set(2, &pet{Name: "b", Age: 11, Owner: "ann"}))
		require.NoError(t, pets.Set(1, &pet{Name: "a", Age: 9, Owner: "ann"}))
		require.Equal(t, []int64{2, 3}, ids(t, must(olds.Range(kvutil.All[string]()))))
		require.Equal(t, []int64{}, ids(t, must(tagged.Instantiate(tx).Lookup(ownerField.Eq("ann")))))

		require.NoError(t, pets.Delete(3))
		require.Equal(t, []int64{2}, ids(t, must(olds.Range(kvutil.All[string]()))))
		return nil
	})
	require.NoError(t, err)

	// An index with another condition has other records in it, so it has to be built again.
	changed := schema.New()
	index.On(extstore.New[int64, pet](changed.Namespace("pets")), "old_by_name", nameField, index.Where(ageField.Gte(11)))
	require.ErrorIs(t, changed.Check(db), schema.ErrNeedsMigration)
}

func TestUniqueComposite(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet](schema.New().Namespace("pets"))
	idx := index.UniqueComposite(store, "by_owner_name", field.List(ownerField, nameField))
	require.True(t, idx.Descriptor().Unique)

	err := db.Update(func(tx kv.Tx) error {
		pets := store.Instantiate(tx)
		require.NoError(t, pets.Set(1, &pet{Owner: "ann", Name: "rex"}))
		require.NoError(t, pets.Set(2, &pet{Owner: "bob", Name: "rex"}))
		require.NoError(t, pets.Set(3, &pet{Owner: "ann", Name: "tom"}))
		require.ErrorIs(t, pets.Set(4, &pet{Owner: "ann", Name: "rex"}), index.ErrUniqueViolation)

		require.Equal(t, []int64{1, 3}, ids(t, must(idx.Instantiate(tx).Lookup(ownerField.Eq("ann")))))
		require.Equal(t, []int64{2}, ids(t, must(idx.Instantiate(tx).Lookup(ownerField.Eq("bob"), nameField.Eq("rex")))))
		return nil
	})
	require.NoError(t, err)
}

func TestFields(t *testing.T) {
	owner := field.Path[pet, string]("Owner")
	age := field.Path[pet, int]("Age")
	tags := field.Path[pet, string]("Tags")

	sch := schema.New()
	pets := extstore.New[int64, pet](sch.Namespace("pets"))
	byOwner := index.On(pets, "by_owner", owner)
	byOwnerAge := index.Composite(pets, "by_owner_age", field.List(owner, age.Desc()),
		// The options wrap the indexer, which still has its fields.
		index.Where(age.Gte(1)),
		index.Covering(func(p *pet) string { return p.Name }, nil, "Name"))
	custom := index.New(pets, "custom", byChipLength{})
	require.NoError(t, sch.Validate())

	paths := func(parts []field.Component[pet]) []string {
		var paths []string
		for _, part := range parts {
			path := part.Path()
			if part.Descending() {
				path += " desc"
			}
			paths = append(paths, path)
		}
		return paths
	}
	require.Equal(t, []string{"Owner"}, paths(byOwner.Fields()))
	require.Equal(t, []string{"Owner", "Age desc"}, paths(byOwnerAge.Fields()))
	require.Equal(t, []string{"Tags"}, paths(index.ByFields[pet](tags).Fields()))
	// An indexer that is not one of fields has none.
	require.Nil(t, custom.Fields())

	// The fields are what conditions are written with, whatever their types are.
	untyped, ok := byOwnerAge.Fields()[1].(field.Untyped[pet])
	require.True(t, ok)
	cnd, err := untyped.EqAny("3")
	require.NoError(t, err)
	require.Equal(t, age.Eq(3).Values, cnd.Values)

	// The indexes of a store are found through the schema, as the things that have the fields.
	var found []string
	for _, def := range pets.Namespace().Definitions() {
		if x, ok := def.Object.(interface{ Fields() []field.Component[pet] }); ok {
			found = append(found, def.Namespace().Name()+": "+strings.Join(paths(x.Fields()), ", "))
		}
	}
	require.Equal(t, []string{"by_owner: Owner", "by_owner_age: Owner, Age desc", "custom: "}, found)
}

// byChipLength is an indexer that is not made of fields.
type byChipLength struct{}

func (byChipLength) Index(p *pet) ([]index.Entry, error) {
	return []index.Entry{{Key: []byte{byte(len(p.Chip))}}}, nil
}

func (byChipLength) Descriptor() index.Descriptor {
	return index.Descriptor{Encoding: "chip-length"}
}
