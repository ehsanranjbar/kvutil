package index_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

var (
	// A value of the pets that no test index is on.
	chipLen = index.Cover("ChipLen", func(p *pet) int { return len(p.Chip) })
	// A cover with a codec of its own.
	nameJSON = index.Cover("NameJSON", name, codec.Formatted[string](codec.JSON))
	// A field that is not held by the indexes of the tests.
	otherField = field.Of("Other", func(p *pet) int { return p.Age * 2 })
)

// TestEntryValue checks the value that an entry holds, byte by byte.
func TestEntryValue(t *testing.T) {
	store := extstore.New[int64, pet]("pets")
	x := index.New(store, "by", index.ByFields[pet](ownerField, ageField.Desc()),
		index.Including(tagsField, chipField), index.Covering(petName))

	entries, err := x.Indexer().Index(&pet{Name: "rex", Owner: "ann", Age: 3, Tags: []string{"a", "b"}})
	require.NoError(t, err)
	require.Len(t, entries, 1) // the tags are held, not in the key: one entry

	encoded := must(codec.ValueFor[string](nil).Encode("rex"))
	var want []byte
	// Tags: two values, each ascending and marked at its end.
	want = append(want, 2)
	want = append(want, must(tagsField.Encode("a"))...)
	want = append(want, must(tagsField.Encode("b"))...)
	// Chip: the pet has none.
	want = append(want, 0)
	// The cover: its length, and the bytes of its codec.
	want = append(want, byte(len(encoded)))
	want = append(want, encoded...)
	require.Equal(t, want, entries[0].Value)

	desc := x.Descriptor()
	require.Equal(t, []string{"Tags", "Chip", "Name"}, desc.Covered)
	require.Equal(t, "string,int desc+inc:Tags=string+inc:Chip=string+cover:Name=msgpack", desc.Encoding)
}

func TestIteratorGet(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		pets := extstore.New[int64, pet]("pets")
		byOwner := index.Composite(pets, "by_owner_age", field.List(ownerField, ageField.Desc()),
			index.Including(nameField, tagsField, chipField),
			index.Covering(chipLen, nameJSON))
		byTag := index.On(pets, "by_tag", tagsField)
		plain := index.On(pets, "plain", ownerField)
		require.NoError(t, pets.Check(db))
		require.Equal(t, []string{"Name", "Tags", "Chip", "ChipLen", "NameJSON"}, byOwner.Descriptor().Covered)

		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := pets.Instantiate(tx)
			require.NoError(t, store.Set(1, &pet{Name: "rex", Owner: "ann", Age: 3, Tags: []string{"big", "old"}, Chip: "c1"}))
			require.NoError(t, store.Set(2, &pet{Name: "tom", Owner: "ann", Age: 9}))
			return nil
		}))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := must(byOwner.Instantiate(tx).Scan(index.Bounds{}))
			defer it.Close()

			// The oldest first, as the age is descending: key fields, ascending and descending.
			require.True(t, it.Valid())
			require.Equal(t, int64(2), must(it.Key()))
			owner, ok, err := it.Get(ownerField)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, "ann", owner)
			age, ok, err := it.Get(ageField)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, 9, age)

			// Included: a field, ones that the pet has no value for, and covers.
			n, ok, err := it.Get(nameField)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, "tom", n)
			_, ok, err = it.Get(chipField)
			require.NoError(t, err)
			require.False(t, ok)
			all, err := it.GetAll(tagsField)
			require.NoError(t, err)
			require.Empty(t, all)
			length, _, err := it.Get(chipLen)
			require.NoError(t, err)
			require.Zero(t, length)

			it.Next()
			require.Equal(t, int64(1), must(it.Key()))
			age, _, err = it.Get(ageField)
			require.NoError(t, err)
			require.Equal(t, 3, age)
			first, ok, err := it.Get(tagsField)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, "big", first)
			all, err = it.GetAll(tagsField)
			require.NoError(t, err)
			require.Equal(t, []string{"big", "old"}, all)
			chip, ok, err := it.Get(chipField)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, "c1", chip)
			length, _, err = it.Get(chipLen)
			require.NoError(t, err)
			require.Equal(t, 2, length)
			viaJSON, _, err := it.Get(nameJSON)
			require.NoError(t, err)
			require.Equal(t, "rex", viaJSON)

			// An item that the index does not hold, or one by a name that nothing is covered by.
			_, _, err = it.Get(otherField)
			require.ErrorIs(t, err, index.ErrNotHeld)
			_, _, err = it.Get(index.Cover("Name", name))
			require.ErrorIs(t, err, index.ErrNotHeld)
			_, ok = byOwner.Access(otherField)
			require.False(t, ok)

			// A field of the key that has several values has the value of the entry.
			tags := must(byTag.Instantiate(tx).Scan(index.Bounds{}))
			defer tags.Close()
			var seen []string
			for ; tags.Valid(); tags.Next() {
				tag, _, err := tags.Get(tagsField)
				require.NoError(t, err)
				seen = append(seen, tag)
			}
			// A record is returned once, with the value of its first entry.
			require.Equal(t, []string{"big"}, seen)
			byEntry, ok := byTag.Access(tagsField)
			require.True(t, ok)
			require.True(t, byEntry.ByEntry())
			included, ok := byOwner.Access(tagsField)
			require.True(t, ok)
			require.False(t, included.ByEntry())

			// A plain index holds its key field only.
			p := must(plain.Instantiate(tx).Scan(index.Bounds{}))
			defer p.Close()
			_, _, err = p.Get(nameField)
			require.ErrorIs(t, err, index.ErrNotHeld)

			// An iterator that is done holds nothing.
			it.Next()
			require.False(t, it.Valid())
			_, _, err = it.Get(ownerField)
			require.Error(t, err)
			return nil
		}))
	})
}

func TestHoldDefinitions(t *testing.T) {
	define := func(opts ...index.Option[pet]) func() {
		return func() {
			index.Composite(extstore.New[int64, pet]("pets"), "by_owner_age", field.List(ownerField, ageField), opts...)
		}
	}
	require.NotPanics(t, define(index.Including(nameField), index.Covering(chipLen)))
	// Twice.
	require.Panics(t, define(index.Including(nameField, nameField)))
	require.Panics(t, define(index.Including(nameField), index.Including(nameField)))
	require.Panics(t, define(index.Covering(chipLen, chipLen)))
	require.Panics(t, define(index.Covering(chipLen), index.Covering(chipLen)))
	// A field of the key.
	require.Panics(t, define(index.Including(ownerField)))
	require.Panics(t, define(index.Covering(index.Cover("Age", age))))
	// A cover with the name of an included field, and the other way round.
	require.Panics(t, define(index.Including(nameField), index.Covering(index.Cover("Name", name))))
	require.Panics(t, define(index.Covering(index.Cover("Name", name)), index.Including(nameField)))
	// Not a cover, not a field.
	require.Panics(t, func() { index.Cover("", name) })
	require.Panics(t, func() { index.Cover[pet, string]("x", nil) })
	require.Panics(t, func() { index.Including[pet](nil) })
	require.Panics(t, func() { index.Covering[pet](nil) })
}

func TestHoldIdentifiesTheIndex(t *testing.T) {
	define := func() *extstore.Store[int64, pet] { return extstore.New[int64, pet]("pets") }
	db := memkv.New()

	base := define()
	index.Composite(base, "by", field.List(ownerField), index.Including(nameField), index.Covering(chipLen))
	require.NoError(t, base.Check(db))
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		return base.Instantiate(tx).Set(1, &pet{Name: "rex", Owner: "ann", Chip: "abc"})
	}))

	same := define()
	index.Composite(same, "by", field.List(ownerField), index.Including(nameField), index.Covering(chipLen))
	require.NoError(t, same.Check(db))

	for name, opts := range map[string][]index.Option[pet]{
		"another included field": {index.Including(ageField), index.Covering(chipLen)},
		"one more included":      {index.Including(nameField, ageField), index.Covering(chipLen)},
		"no included":            {index.Covering(chipLen)},
		"no cover":               {index.Including(nameField)},
		"another cover name":     {index.Including(nameField), index.Covering(index.Cover("Chip", func(p *pet) int { return len(p.Chip) }))},
		"another cover codec": {index.Including(nameField), index.Covering(
			index.Cover("ChipLen", func(p *pet) int { return len(p.Chip) }, codec.Formatted[int](codec.JSON)))},
		"a versioned included field": {index.Including(nameField.WithVersion("2")), index.Covering(chipLen)},
		"another order":              {index.Covering(chipLen), index.Including(nameField)},
	} {
		changed := define()
		index.Composite(changed, "by", field.List(ownerField), opts...)
		require.ErrorIs(t, changed.Check(db), migrate.ErrNeedsMigration, name)
	}

	// Migrating builds the index again, with what it holds now.
	changed := define()
	by := index.Composite(changed, "by", field.List(ownerField), index.Including(ageField), index.Covering(chipLen))
	_, err := changed.Migrate(context.Background(), db)
	require.NoError(t, err)
	require.NoError(t, changed.Check(db))
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it := must(by.Instantiate(tx).Scan(index.Bounds{}))
		defer it.Close()
		require.True(t, it.Valid())
		_, _, err := it.Get(nameField)
		require.ErrorIs(t, err, index.ErrNotHeld)
		length, _, err := it.Get(chipLen)
		require.NoError(t, err)
		require.Equal(t, 3, length)
		return nil
	}))
}

func TestWhereFunc(t *testing.T) {
	adult := func(p *pet) bool { return p.Age >= 3 }
	db := memkv.New()

	store := extstore.New[int64, pet]("pets")
	byFunc := index.On(store, "by_func", nameField, index.WhereFunc("adult", adult))
	byCond := index.On(store, "by_cond", nameField, index.Where(cond.Func("adult", adult)))
	require.NoError(t, store.Check(db))
	require.Equal(t, byCond.Descriptor().Where, byFunc.Descriptor().Where)
	require.Equal(t, "adult()", byFunc.Descriptor().Where)
	require.Equal(t, byCond.Descriptor().Encoding, byFunc.Descriptor().Encoding)

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		pets := store.Instantiate(tx)
		for id, age := range []int{1, 3, 5, 2} {
			require.NoError(t, pets.Set(int64(id+1), &pet{Name: "p", Age: age}))
		}
		return nil
	}))
	require.NoError(t, db.View(func(tx kv.Tx) error {
		a := ids(t, must(byFunc.Instantiate(tx).Match("p")))
		b := ids(t, must(byCond.Instantiate(tx).Match("p")))
		require.Equal(t, []int64{2, 3}, a)
		require.Equal(t, a, b)
		return nil
	}))
}
