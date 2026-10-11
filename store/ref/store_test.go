package ref_test

import (
	"bytes"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
	"github.com/stretchr/testify/require"
)

type entry struct {
	key, primaryKey, value string
}

// scan returns the references of a range.
func scan(t *testing.T, refs *refstore.Instance, start, end []byte, reverse bool) []entry {
	t.Helper()

	iterate := refs.Iterator
	if reverse {
		iterate = refs.ReverseIterator
	}
	it, err := iterate(start, end)
	require.NoError(t, err)
	defer it.Close()

	entries := []entry{}
	for ; it.Valid(); it.Next() {
		key, err := it.Key()
		require.NoError(t, err)
		pk, err := it.PrimaryKey()
		require.NoError(t, err)
		value, err := it.Value()
		require.NoError(t, err)
		entries = append(entries, entry{string(key), string(pk), string(value)})
	}
	return entries
}

func TestStore(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		store := refstore.New(sch.Namespace("refs"), schema.Definition{Kind: "test"})
		require.NoError(t, sch.Validate())
		require.False(t, store.Unique())
		require.Equal(t, "test", store.Namespace().Definition().Kind)
		require.True(t, store.Namespace().Definition().Keys)

		err := db.Update(func(tx kv.Tx) error {
			refs := store.Instantiate(tx)
			// The keys all have two bytes, so none is the beginning of another one.
			require.NoError(t, refs.Set([]byte("k1"), []byte("b"), []byte("v1")))
			require.NoError(t, refs.Set([]byte("k1"), []byte("a"), nil))
			require.NoError(t, refs.Set([]byte("k2"), []byte("a"), nil))
			require.NoError(t, refs.Set([]byte("k2"), []byte("long primary key"), nil))
			require.NoError(t, refs.Set([]byte("k3"), nil, []byte("v3")))

			ok, err := refs.Has([]byte("k1"), []byte("a"))
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = refs.Has([]byte("k1"), []byte("c"))
			require.NoError(t, err)
			require.False(t, ok)

			value, err := refs.Get([]byte("k1"), []byte("b"))
			require.NoError(t, err)
			require.Equal(t, []byte("v1"), value)
			value, err = refs.Get([]byte("k1"), []byte("a"))
			require.NoError(t, err)
			require.Empty(t, value)
			_, err = refs.Get([]byte("k1"), []byte("c"))
			require.ErrorIs(t, err, kv.ErrNotFound)

			all := []entry{
				{"k1", "a", ""},
				{"k1", "b", "v1"},
				{"k2", "a", ""},
				{"k2", "long primary key", ""},
				{"k3", "", "v3"},
			}
			require.Equal(t, all, scan(t, refs, nil, nil, false))
			require.Equal(t, all[:2], scan(t, refs, []byte("k1"), []byte("k2"), false))
			require.Equal(t, all[2:], scan(t, refs, []byte("k2"), nil, false))
			// A bound may be the beginning of a key.
			require.Equal(t, all, scan(t, refs, []byte("k"), []byte("l"), false))

			reversed := scan(t, refs, []byte("k1"), []byte("k3"), true)
			require.Equal(t, []entry{all[3], all[2], all[1], all[0]}, reversed)

			// Setting a reference again replaces its value.
			require.NoError(t, refs.Set([]byte("k1"), []byte("a"), []byte("new")))
			require.Equal(t, []entry{{"k1", "a", "new"}, all[1]}, scan(t, refs, []byte("k1"), []byte("k2"), false))

			require.NoError(t, refs.Delete([]byte("k1"), []byte("a")))
			require.NoError(t, refs.Delete([]byte("k1"), []byte("missing")))
			require.Equal(t, all[1:2], scan(t, refs, []byte("k1"), []byte("k2"), false))

			it, err := refs.Iterator(nil, nil)
			require.NoError(t, err)
			defer it.Close()
			it.Seek([]byte("k3"))
			require.True(t, it.Valid())
			key, err := it.Key()
			require.NoError(t, err)
			require.Equal(t, []byte("k3"), key)

			long := bytes.Repeat([]byte("x"), 256)
			require.ErrorIs(t, refs.Set([]byte("k1"), long, nil), refstore.ErrPrimaryKeyTooLong)
			require.ErrorIs(t, refs.Delete([]byte("k1"), long), refstore.ErrPrimaryKeyTooLong)
			_, err = refs.Has([]byte("k1"), long)
			require.ErrorIs(t, err, refstore.ErrPrimaryKeyTooLong)
			require.NoError(t, refs.Set([]byte("k4"), long[:255], nil))

			// A reference is stored under its key, its primary key and the length of the primary key.
			stored, err := store.Namespace().Open(tx).Get([]byte("k1b\x01"))
			require.NoError(t, err)
			require.Equal(t, []byte("v1"), stored)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestUniqueStore(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := refstore.NewUnique(schema.New().Namespace("refs"), schema.Definition{Kind: "test"})
		require.True(t, store.Unique())

		err := db.Update(func(tx kv.Tx) error {
			refs := store.Instantiate(tx)
			require.NoError(t, refs.Set([]byte("k1"), []byte("a"), []byte("v1")))
			require.NoError(t, refs.Set([]byte("k2"), []byte("b"), nil))
			require.NoError(t, refs.Set([]byte("k3"), nil, []byte("v3")))

			// A key refers to one primary key at most.
			require.ErrorIs(t, refs.Set([]byte("k1"), []byte("b"), nil), refstore.ErrDuplicate)
			require.NoError(t, refs.Set([]byte("k1"), []byte("a"), []byte("new")))

			ok, err := refs.Has([]byte("k1"), []byte("a"))
			require.NoError(t, err)
			require.True(t, ok)
			ok, err = refs.Has([]byte("k1"), []byte("b"))
			require.NoError(t, err)
			require.False(t, ok)
			ok, err = refs.Has([]byte("missing"), []byte("a"))
			require.NoError(t, err)
			require.False(t, ok)

			value, err := refs.Get([]byte("k1"), []byte("a"))
			require.NoError(t, err)
			require.Equal(t, []byte("new"), value)
			_, err = refs.Get([]byte("k1"), []byte("b"))
			require.ErrorIs(t, err, kv.ErrNotFound)
			_, err = refs.Get([]byte("missing"), []byte("a"))
			require.ErrorIs(t, err, kv.ErrNotFound)

			all := []entry{{"k1", "a", "new"}, {"k2", "b", ""}, {"k3", "", "v3"}}
			require.Equal(t, all, scan(t, refs, nil, nil, false))
			require.Equal(t, all[1:2], scan(t, refs, []byte("k2"), []byte("k3"), false))
			require.Equal(t, []entry{all[2], all[1], all[0]}, scan(t, refs, nil, nil, true))

			// Deleting the reference of another primary key does nothing.
			require.NoError(t, refs.Delete([]byte("k1"), []byte("b")))
			require.NoError(t, refs.Delete([]byte("missing"), []byte("a")))
			require.Equal(t, all, scan(t, refs, nil, nil, false))

			require.NoError(t, refs.Delete([]byte("k1"), []byte("a")))
			require.NoError(t, refs.Set([]byte("k1"), []byte("b"), nil))
			require.Equal(t, []entry{{"k1", "b", ""}}, scan(t, refs, []byte("k1"), []byte("k2"), false))

			// A reference is stored under its key alone.
			stored, err := store.Namespace().Open(tx).Get([]byte("k3"))
			require.NoError(t, err)
			require.Equal(t, []byte("\x00v3"), stored)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestCorruptEntries(t *testing.T) {
	db := testkv.NewMem(t)
	multi := refstore.New(schema.New().Namespace("multi"), schema.Definition{Kind: "test"})
	unique := refstore.NewUnique(schema.New().Namespace("unique"), schema.Definition{Kind: "test"})

	err := db.Update(func(tx kv.Tx) error {
		// The length says that the primary key is longer than the whole key.
		require.NoError(t, multi.Namespace().Open(tx).Set([]byte("k\x09"), nil))
		it, err := multi.Instantiate(tx).Iterator(nil, nil)
		require.NoError(t, err)
		defer it.Close()
		require.True(t, it.Valid())
		_, err = it.PrimaryKey()
		require.ErrorIs(t, err, refstore.ErrCorrupt)
		_, err = it.Key()
		require.ErrorIs(t, err, refstore.ErrCorrupt)

		require.NoError(t, unique.Namespace().Open(tx).Set([]byte("k"), []byte{0x09, 'a'}))
		refs := unique.Instantiate(tx)
		require.ErrorIs(t, refs.Set([]byte("k"), []byte("a"), nil), refstore.ErrCorrupt)
		require.ErrorIs(t, refs.Delete([]byte("k"), []byte("a")), refstore.ErrCorrupt)
		_, err = refs.Has([]byte("k"), []byte("a"))
		require.ErrorIs(t, err, refstore.ErrCorrupt)

		uit, err := refs.Iterator(nil, nil)
		require.NoError(t, err)
		defer uit.Close()
		_, err = uit.PrimaryKey()
		require.ErrorIs(t, err, refstore.ErrCorrupt)
		_, err = uit.Value()
		require.ErrorIs(t, err, refstore.ErrCorrupt)
		return nil
	})
	require.NoError(t, err)
}
