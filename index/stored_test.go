package index_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

// plainStore is a store whose getter cannot give a record as it is stored.
type plainStore struct {
	*extstore.Store[int64, pet]
}

func (s plainStore) Getter(tx kv.Tx) extstore.Getter[int64, pet] {
	return plainGetter{s.Store.Getter(tx)}
}

type plainGetter struct {
	g extstore.Getter[int64, pet]
}

func (g plainGetter) Get(key int64) (*pet, error) { return g.g.Get(key) }

func TestIndexIteratorStoredThenValue(t *testing.T) {
	testkv.Each(t, func(t *testing.T, base kv.DB) {
		petsOf := map[int64]pet{
			1: {Name: "rex", Owner: "ann", Age: 3, Tags: []string{"cat"}},
			2: {Name: "tom", Owner: "ann", Age: 9, Tags: []string{"dog"}},
			3: {Name: "max", Owner: "bob", Age: 3, Tags: []string{"cat"}},
			4: {Name: "kit", Owner: "ann", Age: 3, Tags: []string{"cat", "dog"}},
		}
		store := extstore.New[int64, pet]("pets")
		plain := plainStore{extstore.New[int64, pet]("plain")}
		byOwner := index.On(store, "by_owner", ownerField)
		byAge := index.On(store, "by_age", ageField)
		byPlain := index.On[int64, pet](plain, "by_owner", ownerField)

		db, counts := testkv.Count(base)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id, p := range petsOf {
				if err := store.Instantiate(tx).Set(id, &p); err != nil {
					return err
				}
				if err := plain.Instantiate(tx).Set(id, &p); err != nil {
					return err
				}
			}
			return nil
		}))

		decode, ok := codec.PartialOf(store.ValueCodec(), []string{"Name"})
		require.True(t, ok)

		// each goes over the records of ann, in the order of their keys, and checks the reads.
		type stored = extstore.StoredIterator[int64, pet]
		each := func(t *testing.T, tx kv.Tx, open func(tx kv.Tx) index.Keyed[int64, pet], wantKeys []int64,
			visit func(t *testing.T, it index.Keyed[int64, pet], id int64)) {
			t.Helper()

			it := open(tx)
			defer it.Close()
			_, ok := it.(stored)
			require.True(t, ok)

			var keys []int64
			for ; it.Valid(); it.Next() {
				key, err := it.Key()
				require.NoError(t, err)
				keys = append(keys, key)

				counts.Reset()
				visit(t, it, key)
				require.EqualValues(t, 1, counts.Gets.Load(), "the record is got once")
			}
			require.Equal(t, wantKeys, keys)
		}

		stage := func(t *testing.T, name string, open func(tx kv.Tx) index.Keyed[int64, pet], wantKeys []int64, partial bool) {
			t.Run(name+"/stored then value", func(t *testing.T) {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					each(t, tx, open, wantKeys, func(t *testing.T, it index.Keyed[int64, pet], id int64) {
						st, err := it.(stored).Stored()
						require.NoError(t, err)
						got, err := st.Partial(decode, &pet{Age: 99})
						require.NoError(t, err)
						want := petsOf[id]
						require.Equal(t, want.Name, got.Name)
						if partial {
							require.Equal(t, &pet{Name: want.Name}, got)
						}

						full, err := it.Value()
						require.NoError(t, err)
						require.Equal(t, &want, full)
						again, err := it.Value()
						require.NoError(t, err)
						require.Same(t, full, again)
					})
					return nil
				}))
			})

			t.Run(name+"/value then stored", func(t *testing.T) {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					each(t, tx, open, wantKeys, func(t *testing.T, it index.Keyed[int64, pet], id int64) {
						full, err := it.Value()
						require.NoError(t, err)
						want := petsOf[id]
						require.Equal(t, &want, full)

						st, err := it.(stored).Stored()
						require.NoError(t, err)
						got, err := st.Partial(decode, &pet{})
						require.NoError(t, err)
						require.Same(t, full, got, "the record that was read is not decoded again")
						rec, err := st.Record()
						require.NoError(t, err)
						require.Same(t, full, rec)
					})
					return nil
				}))
			})

			t.Run(name+"/stored twice then record", func(t *testing.T) {
				require.NoError(t, db.View(func(tx kv.Tx) error {
					each(t, tx, open, wantKeys, func(t *testing.T, it index.Keyed[int64, pet], id int64) {
						_, err := it.(stored).Stored()
						require.NoError(t, err)
						st, err := it.(stored).Stored()
						require.NoError(t, err)
						rec, err := st.Record()
						require.NoError(t, err)
						want := petsOf[id]
						require.Equal(t, &want, rec)
					})
					return nil
				}))
			})
		}

		annKeys := []int64{1, 2, 4}
		stage(t, "index", func(tx kv.Tx) index.Keyed[int64, pet] {
			return must(byOwner.Instantiate(tx).Match("ann"))
		}, annKeys, true)

		stage(t, "plain getter", func(tx kv.Tx) index.Keyed[int64, pet] {
			return must(byPlain.Instantiate(tx).Match("ann"))
		}, annKeys, false)

		stage(t, "intersect", func(tx kv.Tx) index.Keyed[int64, pet] {
			return must(index.Intersect[int64, pet](
				must(byOwner.Instantiate(tx).Match("ann")),
				must(byAge.Instantiate(tx).Match(3)),
			))
		}, []int64{1, 4}, true)

		stage(t, "union", func(tx kv.Tx) index.Keyed[int64, pet] {
			return must(index.Union[int64, pet](
				must(byAge.Instantiate(tx).Match(9)),
				must(byOwner.Instantiate(tx).Match("bob")),
			))
		}, []int64{2, 3}, true)

		stage(t, "bitmap", func(tx kv.Tx) index.Keyed[int64, pet] {
			bm, exact, err := must(byOwner.Instantiate(tx).Match("ann")).Bitmap()
			require.NoError(t, err)
			require.True(t, exact)
			return must(index.FromBitmap[int64, pet](store, tx, bm))
		}, annKeys, true)

		stage(t, "bitmap of a plain getter", func(tx kv.Tx) index.Keyed[int64, pet] {
			bm, _, err := must(byPlain.Instantiate(tx).Match("ann")).Bitmap()
			require.NoError(t, err)
			return must(index.FromBitmap[int64, pet](plain, tx, bm))
		}, annKeys, false)

		stage(t, "in a bitmap", func(tx kv.Tx) index.Keyed[int64, pet] {
			bm, exact, err := must(byAge.Instantiate(tx).Match(3)).Bitmap()
			require.NoError(t, err)
			return index.InBitmap(must(byOwner.Instantiate(tx).Match("ann")), bm, exact)
		}, []int64{1, 4}, true)
	})
}

// TestIndexIteratorLookupFilterStored checks that a record that a filter read is not got again.
func TestIndexIteratorLookupFilterStored(t *testing.T) {
	testkv.Each(t, func(t *testing.T, base kv.DB) {
		store := extstore.New[int64, pet]("pets")
		byOwner := index.On(store, "by_owner", ownerField)
		db, counts := testkv.Count(base)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id, p := range map[int64]pet{
				1: {Name: "rex", Owner: "ann", Age: 3},
				2: {Name: "tom", Owner: "ann", Age: 9},
			} {
				if err := store.Instantiate(tx).Set(id, &p); err != nil {
					return err
				}
			}
			return nil
		}))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			it, err := byOwner.Instantiate(tx).Lookup(ownerField.Eq("ann"), ageField.Gt(5))
			require.NoError(t, err)
			defer it.Close()
			require.True(t, it.Valid())

			// The filter read the record to check it.
			counts.Reset()
			st, err := it.Stored()
			require.NoError(t, err)
			rec, err := st.Record()
			require.NoError(t, err)
			require.Equal(t, "tom", rec.Name)
			v, err := it.Value()
			require.NoError(t, err)
			require.Same(t, rec, v)
			require.Zero(t, counts.Gets.Load())
			return nil
		}))
	})
}
