package index_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

type pet struct {
	Name  string
	Owner string
	Age   int
	Tags  []string
	Chip  string
}

var _ iterutil.Iterator[int64, *pet] = (*index.Iterator[int64, pet])(nil)

// must returns the value of a call that must not fail.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// ids collects the primary keys of an iterator and closes it.
func ids(t *testing.T, it *index.Iterator[int64, pet]) []int64 {
	t.Helper()

	keys, err := iterutil.Of(it).Keys()
	require.NoError(t, err)
	if keys == nil {
		keys = []int64{}
	}
	return keys
}

func name(p *pet) string { return p.Name }
func age(p *pet) int     { return p.Age }

// The fields that the indexes of the tests are on.
var (
	nameField  = field.Of("Name", name)
	ageField   = field.Of("Age", age)
	ownerField = field.Path[pet, string]("Owner")
	tagsField  = field.Path[pet, string]("Tags")
	// The chip of a pet that has one.
	chipField = field.Each("Chip", func(p *pet) []string {
		if p.Chip == "" {
			return nil
		}
		return []string{p.Chip}
	})
)

func TestMulti(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		store := extstore.New[int64, pet](sch.Namespace("pets"))
		byName := index.On(store, "by_name", nameField)
		byAge := index.On(store, "by_age", ageField)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			pets := store.Instantiate(tx)
			require.NoError(t, pets.Set(1, &pet{Name: "foo", Age: 3}))
			require.NoError(t, pets.Set(2, &pet{Name: "foo", Age: -1}))
			require.NoError(t, pets.Set(3, &pet{Name: "bar", Age: 3}))
			require.NoError(t, pets.Set(4, &pet{Name: "fo", Age: 7}))
			require.NoError(t, pets.Set(5, &pet{Name: "", Age: 0}))

			names := byName.Instantiate(tx)
			require.Equal(t, []int64{1, 2}, ids(t, must(names.Match("foo"))))
			require.Equal(t, []int64{4}, ids(t, must(names.Match("fo"))))
			require.Equal(t, []int64{5}, ids(t, must(names.Match(""))))
			require.Equal(t, []int64{}, ids(t, must(names.Match("f"))))

			// In the order of the names, and of the primary keys for the same name.
			require.Equal(t, []int64{5, 3, 4, 1, 2}, ids(t, must(names.Range(kvutil.All[string]()))))
			require.Equal(t, []int64{2, 1, 4, 3, 5}, ids(t, must(names.ReverseRange(kvutil.All[string]()))))
			require.Equal(t, []int64{3, 4}, ids(t, must(names.Range(kvutil.From("bar").Until("foo")))))
			require.Equal(t, []int64{4, 1, 2}, ids(t, must(names.Range(kvutil.After("bar").Through("foo")))))
			require.Equal(t, []int64{4, 1, 2}, ids(t, must(names.Range(kvutil.From("c")))))
			require.Equal(t, []int64{5, 3}, ids(t, must(names.Range(kvutil.Through("c")))))

			ages := byAge.Instantiate(tx)
			require.Equal(t, []int64{2, 5, 1, 3, 4}, ids(t, must(ages.Range(kvutil.All[int]()))))
			require.Equal(t, []int64{1, 3}, ids(t, must(ages.Match(3))))
			require.Equal(t, []int64{5, 1, 3}, ids(t, must(ages.Range(kvutil.From(0).Until(7)))))

			key, found, err := names.Find("bar")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(3), key)
			_, found, err = names.Find("missing")
			require.NoError(t, err)
			require.False(t, found)

			// The records are read from the store when they are asked for.
			it := must(names.Match("foo"))
			defer it.Close()
			record, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, &pet{Name: "foo", Age: 3}, record)
			indexKey, err := it.IndexKey()
			require.NoError(t, err)
			require.Equal(t, []byte("foo\x00\x01"), indexKey)
			rawKey, err := it.RawKey()
			require.NoError(t, err)
			require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 1}, rawKey)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestMaintenance(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byName := index.On(store, "by_name", nameField)

		err := db.Update(func(tx kv.Tx) error {
			pets, names := store.Instantiate(tx), byName.Instantiate(tx)
			require.NoError(t, pets.Set(1, &pet{Name: "foo"}))
			require.NoError(t, pets.Set(2, &pet{Name: "foo"}))
			require.Equal(t, []int64{1, 2}, ids(t, must(names.Match("foo"))))

			// A record that is renamed leaves its old entry, and only its own.
			require.NoError(t, pets.Set(1, &pet{Name: "bar"}))
			require.Equal(t, []int64{2}, ids(t, must(names.Match("foo"))))
			require.Equal(t, []int64{1}, ids(t, must(names.Match("bar"))))

			// A record that is set again as it is stays as it is.
			require.NoError(t, pets.Set(2, &pet{Name: "foo", Age: 9}))
			require.Equal(t, []int64{2}, ids(t, must(names.Match("foo"))))

			// A record that is deleted takes its own entry with it.
			require.NoError(t, pets.Set(3, &pet{Name: "foo"}))
			require.NoError(t, pets.Delete(2))
			require.Equal(t, []int64{3}, ids(t, must(names.Match("foo"))))
			require.Equal(t, []int64{1}, ids(t, must(names.Match("bar"))))

			require.NoError(t, pets.Delete(1))
			require.NoError(t, pets.Delete(3))
			require.Equal(t, []int64{}, ids(t, must(names.Range(kvutil.All[string]()))))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestEach(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byTag := index.On(store, "by_tag", tagsField)
		require.True(t, byTag.Descriptor().MultiEntry)

		err := db.Update(func(tx kv.Tx) error {
			pets, tagged := store.Instantiate(tx), byTag.Instantiate(tx)
			require.NoError(t, pets.Set(1, &pet{Tags: []string{"cat", "old"}}))
			require.NoError(t, pets.Set(2, &pet{Tags: []string{"dog", "old", "old"}}))
			require.NoError(t, pets.Set(3, &pet{}))

			require.Equal(t, []int64{1, 2}, ids(t, must(tagged.Match("old"))))
			require.Equal(t, []int64{2}, ids(t, must(tagged.Match("dog"))))
			// A record with several tags comes up once, and a record with none does not come up.
			require.Equal(t, []int64{1, 2}, ids(t, must(tagged.Range(kvutil.All[string]()))))
			require.Equal(t, []int64{2, 1}, ids(t, must(tagged.ReverseRange(kvutil.All[string]()))))

			require.NoError(t, pets.Set(1, &pet{Tags: []string{"cat", "young"}}))
			require.Equal(t, []int64{2}, ids(t, must(tagged.Match("old"))))
			require.Equal(t, []int64{1}, ids(t, must(tagged.Match("young"))))
			require.Equal(t, []int64{1}, ids(t, must(tagged.Match("cat"))))

			require.NoError(t, pets.Set(2, &pet{}))
			require.Equal(t, []int64{1}, ids(t, must(tagged.Range(kvutil.All[string]()))))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestUnique(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byChip := index.Unique(store, "by_chip", chipField)
		require.True(t, byChip.Descriptor().Unique)

		require.NoError(t, db.Update(func(tx kv.Tx) error {
			pets := store.Instantiate(tx)
			require.NoError(t, pets.Set(1, &pet{Name: "rex", Chip: "a"}))
			require.NoError(t, pets.Set(2, &pet{Name: "tom", Chip: "b"}))
			// Any number of records can be without the value.
			require.NoError(t, pets.Set(3, &pet{Name: "max"}))
			return pets.Set(4, &pet{Name: "sam"})
		}))

		// A value that another record has is refused, and the transaction leaves nothing behind.
		err := db.Update(func(tx kv.Tx) error {
			pets := store.Instantiate(tx)
			require.NoError(t, pets.Set(5, &pet{Name: "new", Chip: "c"}))
			return pets.Set(3, &pet{Name: "max", Chip: "a"})
		})
		require.ErrorIs(t, err, index.ErrUniqueViolation)
		require.ErrorContains(t, err, `"by_chip"`)

		err = db.Update(func(tx kv.Tx) error {
			pets, chips := store.Instantiate(tx), byChip.Instantiate(tx)
			_, found, err := chips.Find("c")
			require.NoError(t, err)
			require.False(t, found)

			key, found, err := chips.Find("a")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(1), key)
			require.Equal(t, []int64{1, 2}, ids(t, must(chips.Range(kvutil.All[string]()))))

			// The record that has the value can be set again.
			require.NoError(t, pets.Set(1, &pet{Name: "rex the second", Chip: "a"}))

			// A value is free again once its record gives it up or is deleted.
			require.NoError(t, pets.Set(1, &pet{Name: "rex", Chip: "z"}))
			require.NoError(t, pets.Set(3, &pet{Name: "max", Chip: "a"}))
			require.NoError(t, pets.Delete(2))
			require.NoError(t, pets.Set(4, &pet{Name: "sam", Chip: "b"}))
			require.Equal(t, []int64{3, 4, 1}, ids(t, must(chips.Range(kvutil.All[string]()))))

			require.ErrorIs(t, pets.Set(2, &pet{Chip: "z"}), index.ErrUniqueViolation)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestUniqueConcurrency(t *testing.T) {
	open := func(t *testing.T, opts ...badgerkv.Option) *badgerkv.DB {
		db, err := badgerkv.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR), opts...)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		return db
	}
	// race sets a record while another transaction commits a record with the same value.
	race := func(t *testing.T, db *badgerkv.DB) (attempts int, err error) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byChip := index.Unique(store, "by_chip", chipField)

		err = db.Update(func(tx kv.Tx) error {
			attempts++
			if err := store.Instantiate(tx).Set(1, &pet{Chip: "a"}); err != nil {
				return err
			}
			if attempts == 1 {
				require.NoError(t, db.Update(func(other kv.Tx) error {
					return store.Instantiate(other).Set(2, &pet{Chip: "a"})
				}))
			}
			return nil
		})

		// Exactly one of the two has the value.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			require.Equal(t, []int64{2}, ids(t, must(byChip.Instantiate(tx).Match("a"))))
			_, err := store.Instantiate(tx).Get(1)
			require.ErrorIs(t, err, kv.ErrNotFound)
			return nil
		}))
		return attempts, err
	}

	t.Run("Conflict", func(t *testing.T) {
		attempts, err := race(t, open(t, badgerkv.WithMaxRetries(0)))
		require.ErrorIs(t, err, kv.ErrConflict)
		require.Equal(t, 1, attempts)
	})

	t.Run("Retried", func(t *testing.T) {
		attempts, err := race(t, open(t))
		require.ErrorIs(t, err, index.ErrUniqueViolation)
		require.Equal(t, 2, attempts)
	})
}

func TestCovering(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byAge := index.On(store, "by_age", ageField, index.Covering(name, nil, "Name"))
		require.Equal(t, []string{"Name"}, byAge.Descriptor().Covered)

		names := func(tx kv.Tx) []string {
			it := must(byAge.Instantiate(tx).Range(kvutil.All[int]()))
			defer it.Close()

			var names []string
			for ; it.Valid(); it.Next() {
				// The name comes from the index, without the record being read.
				name, err := codec.ValueFor[string](nil).Decode(must(it.Covered()))
				require.NoError(t, err)
				names = append(names, name)
			}
			return names
		}

		err := db.Update(func(tx kv.Tx) error {
			pets := store.Instantiate(tx)
			require.NoError(t, pets.Set(1, &pet{Name: "rex", Age: 5}))
			require.NoError(t, pets.Set(2, &pet{Name: "tom", Age: 2}))
			require.Equal(t, []string{"tom", "rex"}, names(tx))

			// The covered value follows the record, also when the key of the entry stays.
			require.NoError(t, pets.Set(1, &pet{Name: "max", Age: 5}))
			require.Equal(t, []string{"tom", "max"}, names(tx))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestDiff(t *testing.T) {
	e := func(key, value string) index.Entry {
		return index.Entry{Key: []byte(key), Value: []byte(value)}
	}
	tests := []struct {
		name              string
		old, new          []index.Entry
		removed, upserted []index.Entry
	}{
		{"Nothing", nil, nil, nil, nil},
		{"New", nil, []index.Entry{e("a", "")}, nil, []index.Entry{e("a", "")}},
		{"Gone", []index.Entry{e("a", "")}, nil, []index.Entry{e("a", "")}, nil},
		{"Same", []index.Entry{e("a", "1"), e("b", "")}, []index.Entry{e("b", ""), e("a", "1")}, nil, nil},
		{"Moved", []index.Entry{e("a", "")}, []index.Entry{e("b", "")}, []index.Entry{e("a", "")}, []index.Entry{e("b", "")}},
		{"ValueChanged", []index.Entry{e("a", "1")}, []index.Entry{e("a", "2")}, nil, []index.Entry{e("a", "2")}},
		{"Mixed",
			[]index.Entry{e("a", ""), e("b", ""), e("c", "1")},
			[]index.Entry{e("b", ""), e("c", "2"), e("d", "")},
			[]index.Entry{e("a", "")},
			[]index.Entry{e("c", "2"), e("d", "")},
		},
		{"Repeated",
			[]index.Entry{e("a", ""), e("a", ""), e("b", "")},
			[]index.Entry{e("c", ""), e("c", "")},
			[]index.Entry{e("a", ""), e("b", "")},
			[]index.Entry{e("c", "")},
		},
	}
	for _, test := range tests {
		removed, upserted := index.Diff(test.old, test.new)
		require.Equal(t, test.removed, removed, test.name)
		require.Equal(t, test.upserted, upserted, test.name)
	}
}

func TestCatalog(t *testing.T) {
	define := func(sch *schema.Schema) *extstore.Store[int64, pet] {
		return extstore.New[int64, pet](sch.Namespace("pets"))
	}

	t.Run("Definition", func(t *testing.T) {
		sch := schema.New()
		byName := index.On(define(sch), "by_name", nameField)
		require.NoError(t, sch.Validate())

		require.Equal(t, "by_name", byName.Name())
		require.Equal(t, "pets/ext/by_name", byName.Namespace().Path())
		def := byName.Namespace().Definition()
		require.Equal(t, "index", def.Kind)
		require.Equal(t, index.Descriptor{
			Fields:    []index.FieldDescriptor{{Path: "Name"}},
			Operators: []cond.Op{cond.OpEq, cond.OpIn, cond.OpRange},
			Encoding:  "string",
		}, def.Descriptor)
		require.Equal(t, def.Descriptor, byName.Descriptor())
	})

	t.Run("Check", func(t *testing.T) {
		db := memkv.New()
		sch := schema.New()
		index.On(define(sch), "by_name", nameField)
		require.NoError(t, sch.Check(db))

		same := schema.New()
		index.On(define(same), "by_name", nameField)
		require.NoError(t, same.Check(db))

		// An index that is defined in another way has to be built again.
		for name, change := range map[string]func(store *extstore.Store[int64, pet]){
			"Unique": func(store *extstore.Store[int64, pet]) {
				index.Unique(store, "by_name", nameField)
			},
			"Field": func(store *extstore.Store[int64, pet]) {
				index.On(store, "by_name", ownerField)
			},
			"Encoding": func(store *extstore.Store[int64, pet]) {
				index.On(store, "by_name", field.Of("Name", func(p *pet) []byte { return []byte(p.Name) }))
			},
			"Descending": func(store *extstore.Store[int64, pet]) {
				index.On(store, "by_name", nameField.Desc())
			},
			"Covering": func(store *extstore.Store[int64, pet]) {
				index.On(store, "by_name", nameField, index.Covering(age, nil, "Age"))
			},
			"Where": func(store *extstore.Store[int64, pet]) {
				index.On(store, "by_name", nameField, index.Where(ageField.Gt(1)))
			},
		} {
			changed := schema.New()
			change(define(changed))
			require.ErrorIs(t, changed.Check(db), schema.ErrChanged, name)
		}
	})

	t.Run("SameNameTwice", func(t *testing.T) {
		sch := schema.New()
		store := define(sch)
		index.On(store, "by_name", nameField)
		index.On(store, "by_name", ageField)
		require.Error(t, sch.Validate())
	})

	t.Run("NoField", func(t *testing.T) {
		store := define(schema.New())
		require.Panics(t, func() { index.Composite(store, "by_nothing", nil) })
		require.Panics(t, func() { index.Composite(store, "by_name_twice", field.List(nameField, nameField.Desc())) })
	})
}

func TestScanAndLookup(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet](schema.New().Namespace("pets"))
	byName := index.On(store, "by_name", nameField)
	byOwner := index.Composite(store, "by_owner", field.List(ownerField, ageField))
	custom := index.New(store, "custom", fixedIndexer{})

	err := db.Update(func(tx kv.Tx) error {
		pets := store.Instantiate(tx)
		require.NoError(t, pets.Set(1, &pet{Name: "a", Owner: "ann", Age: 3}))
		require.NoError(t, pets.Set(2, &pet{Name: "b", Owner: "bob", Age: 5}))
		require.NoError(t, pets.Set(3, &pet{Name: "c", Owner: "ann", Age: 1}))

		// Several ranges come one after the other, and a record comes up once.
		names := byName.Instantiate(tx)
		key := func(s string) []byte { return must(nameField.Encode(s)) }
		it, err := names.ScanAll([]index.Bounds{
			kvutil.Between(key("b"), key("c")),
			kvutil.Between(key("a"), key("b")),
		}, false)
		require.NoError(t, err)
		require.Equal(t, []int64{2, 3, 1}, ids(t, it))

		// An index on one field takes conditions too, on any field: what the index cannot
		// serve is checked on the records.
		require.Equal(t, []int64{1}, ids(t, must(names.Lookup(nameField.Lte("b"), ownerField.Eq("ann")))))
		require.Equal(t, []int64{3, 2, 1}, ids(t, must(names.ReverseLookup())))

		owners := byOwner.Instantiate(tx)
		require.Equal(t, []int64{3, 1}, ids(t, must(owners.Lookup(ownerField.Eq("ann")))))
		require.Equal(t, []int64{1, 3}, ids(t, must(owners.ReverseLookup(ownerField.Eq("ann")))))
		require.Equal(t, []int64{1}, ids(t, must(owners.Lookup(ownerField.Eq("ann"), ageField.Gt(1)))))

		// An indexer that does not translate conditions cannot be looked up with them.
		_, err = custom.Instantiate(tx).Lookup(nameField.Eq("a"))
		require.Error(t, err)
		_, _, err = custom.Instantiate(tx).Plan(nameField.Eq("a"))
		require.Error(t, err)
		require.Equal(t, []int64{1, 2, 3}, ids(t, must(custom.Instantiate(tx).Scan(index.Bounds{}))))
		return nil
	})
	require.NoError(t, err)
}

// fixedIndexer gives every record the same key.
type fixedIndexer struct{}

func (fixedIndexer) Index(*pet) ([]index.Entry, error) {
	return []index.Entry{{Key: []byte("k")}}, nil
}

func (fixedIndexer) Descriptor() index.Descriptor {
	return index.Descriptor{Encoding: "fixed"}
}

// fixture is a store with one index of every kind.
type fixture struct {
	sch   *schema.Schema
	store *extstore.Store[int64, pet]
}

func newFixture() fixture {
	sch := schema.New()
	store := extstore.New[int64, pet](sch.Namespace("pets"))
	index.On(store, "by_name", nameField)
	index.On(store, "by_name_desc", nameField.Desc())
	index.On(store, "by_tag", tagsField)
	index.On(store, "by_age", ageField, index.Covering(name, nil, "Name"))
	index.Unique(store, "by_chip", chipField)
	index.Composite(store, "by_owner_age", field.List(ownerField, ageField.Desc()))
	index.Composite(store, "by_tag_name", field.List(tagsField, nameField))
	// Only the pets of an owner that are old enough, which records move in and out of.
	index.On(store, "old_by_name", nameField, index.Where(cond.And(ageField.Gte(1), ownerField.Eq("ann"))))
	index.Composite(store, "tagged_by_owner", field.List(ownerField, nameField),
		index.Where(cond.Func("has_tags", func(p *pet) bool { return len(p.Tags) > 0 })))
	return fixture{sch: sch, store: store}
}

// dump returns everything that is stored for the store: its records and its indexes.
func (f fixture) dump(t *testing.T, db kv.DB) []string {
	var entries []string
	err := db.View(func(tx kv.Tx) error {
		it, err := f.store.Namespace().Open(tx).Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()

		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			require.NoError(t, err)
			entries = append(entries, fmt.Sprintf("%x=%x", it.Key(), value))
		}
		return nil
	})
	require.NoError(t, err)
	return entries
}

// TestIndexesEqualRebuild writes, changes and deletes records at random, and checks that the
// indexes are what they would be if they were built from the records that are left.
func TestIndexesEqualRebuild(t *testing.T) {
	run := func(t *testing.T, db kv.DB, ops int) {
		f := newFixture()
		require.NoError(t, f.sch.Check(db))

		rng := rand.New(rand.NewPCG(7, 11))
		pick := func(values ...string) string { return values[rng.IntN(len(values))] }
		model := map[int64]pet{}

		for op := 1; op <= ops; op++ {
			id := int64(1 + rng.IntN(25))
			err := db.Update(func(tx kv.Tx) error {
				pets := f.store.Instantiate(tx)
				if rng.IntN(10) < 3 {
					delete(model, id)
					return pets.Delete(id)
				}

				p := pet{
					Name:  pick("", "a", "ab", "b", "b\x00"),
					Owner: pick("ann", "bob", "cy"),
					Age:   rng.IntN(5) - 2,
				}
				for _, tag := range []string{"cat", "dog", "old"} {
					if rng.IntN(3) == 0 {
						p.Tags = append(p.Tags, tag)
					}
				}
				if rng.IntN(2) == 0 {
					// A chip that no other pet can have.
					p.Chip = fmt.Sprintf("chip-%d-%d", id, rng.IntN(2))
				}
				model[id] = p
				return pets.Set(id, &p)
			})
			require.NoError(t, err)

			if op%25 != 0 && op != ops {
				continue
			}

			rebuilt, fresh := newFixture(), memkv.New()
			err = fresh.Update(func(tx kv.Tx) error {
				pets := rebuilt.store.Instantiate(tx)
				for _, id := range slices.Sorted(func(yield func(int64) bool) {
					for id := range model {
						if !yield(id) {
							return
						}
					}
				}) {
					p := model[id]
					if err := pets.Set(id, &p); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, rebuilt.dump(t, fresh), f.dump(t, db), "after %d operations", op)
		}
		require.NotEmpty(t, model)
	}

	t.Run("Mem", func(t *testing.T) { run(t, memkv.New(), 1000) })
	t.Run("Badger", func(t *testing.T) { run(t, testkv.NewBadger(t), 200) })
}
