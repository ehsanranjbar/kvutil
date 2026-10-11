package index_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

// keysOfSource collects the primary keys of a source and closes it.
func keysOfSource[K, V any](t *testing.T, src index.Keyed[K, V], err error) []K {
	t.Helper()
	require.NoError(t, err)

	keys, err := iterutil.Of[K, *V](src).Keys()
	require.NoError(t, err)
	if keys == nil {
		keys = []K{}
	}
	return keys
}

func TestCombine(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byOwner := index.On(store, "by_owner", ownerField)
		byTag := index.On(store, "by_tag", tagsField)
		byAge := index.On(store, "by_age", ageField)

		err := db.Update(func(tx kv.Tx) error {
			pets := store.Instantiate(tx)
			for id, p := range map[int64]pet{
				1: {Owner: "ann", Age: 3, Tags: []string{"cat", "old"}},
				2: {Owner: "ann", Age: 9, Tags: []string{"dog"}},
				3: {Owner: "bob", Age: 9, Tags: []string{"cat"}},
				4: {Owner: "bob", Age: 1},
				5: {Owner: "ann", Age: 1, Tags: []string{"cat", "dog"}},
			} {
				require.NoError(t, pets.Set(id, &p))
			}
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(tx kv.Tx) error {
			owners, tags, ages := byOwner.Instantiate(tx), byTag.Instantiate(tx), byAge.Instantiate(tx)
			ann := func() index.Keyed[int64, pet] { return must(owners.Match("ann")) }
			cats := func() index.Keyed[int64, pet] { return must(tags.Match("cat")) }
			dogs := func() index.Keyed[int64, pet] { return must(tags.Match("dog")) }
			young := func() index.Keyed[int64, pet] { return must(ages.Range(kvutil.Until(5))) }

			for name, c := range map[string]index.Combiner{"Exact": {}, "Lossy": index.Lossy} {
				// In the order of the first source.
				got, err := c.Intersect(ann(), cats())
				require.Equal(t, []int64{1, 5}, keysOfSource(t, got, err), name)
				got, err = c.Intersect(cats(), ann())
				require.Equal(t, []int64{1, 5}, keysOfSource(t, got, err), name)
				got, err = c.Intersect(young(), ann(), cats())
				require.Equal(t, []int64{5, 1}, keysOfSource(t, got, err), name)
				got, err = c.Intersect(ann())
				require.Equal(t, []int64{1, 2, 5}, keysOfSource(t, got, err), name)

				// The first source, then what the next ones add to it.
				got, err = c.Union(cats(), dogs())
				require.Equal(t, []int64{1, 3, 5, 2}, keysOfSource(t, got, err), name)
				got, err = c.Union(dogs(), cats(), ann())
				require.Equal(t, []int64{2, 5, 1, 3}, keysOfSource(t, got, err), name)
				got, err = c.Union[int64, pet]()
				require.Equal(t, []int64{}, keysOfSource(t, got, err), name)

				got, err = c.Except(ann(), cats())
				require.Equal(t, []int64{2}, keysOfSource(t, got, err), name)
				got, err = c.Except(cats(), ann())
				require.Equal(t, []int64{3}, keysOfSource(t, got, err), name)

				// A combination is a source of another one.
				catsOrDogs, err := c.Union(cats(), dogs())
				require.NoError(t, err)
				got, err = c.Intersect(ann(), catsOrDogs)
				require.Equal(t, []int64{1, 2, 5}, keysOfSource(t, got, err), name)

				annCats, err := c.Intersect(ann(), cats())
				require.NoError(t, err)
				got, err = c.Except(young(), annCats)
				require.Equal(t, []int64{4}, keysOfSource(t, got, err), name)
			}

			// The records are read when they are asked for.
			both, err := index.Intersect(ann(), cats())
			require.NoError(t, err)
			defer both.Close()
			require.True(t, both.Valid())
			record, err := both.Value()
			require.NoError(t, err)
			require.Equal(t, "ann", record.Owner)
			raw, err := both.RawKey()
			require.NoError(t, err)
			require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 1}, raw)

			_, err = index.Intersect[int64, pet]()
			require.Error(t, err)
			return nil
		})
		require.NoError(t, err)
	})
}

// TestCombineFindsWhatAScanFinds combines random lookups in random ways, with keys that are
// numbers and with keys that are not, and checks the result against the records themselves.
// The hash of the keys is made to be the same for many keys, so that the records that only seem
// to be in a lossy set are met all the time.
func TestCombineFindsWhatAScanFinds(t *testing.T) {
	defer index.SetKeyHash(func(raw []byte) uint64 { return uint64(len(raw)+int(raw[len(raw)-1])) % 3 })()

	run := func(t *testing.T, keyOf func(i int) string, exactBitmap bool) {
		db := memkv.New()
		store := extstore.New[string, pet](schema.New().Namespace("pets"))
		byOwner := index.On(store, "by_owner", ownerField)
		byTag := index.On(store, "by_tag", tagsField)
		byAge := index.On(store, "by_age", ageField)
		oldByName := index.On(store, "old_by_name", nameField, index.Where(ageField.Gte(2)))

		rng := rand.New(rand.NewPCG(31, 37))
		owners := []string{"ann", "bob", "cy"}
		names := []string{"a", "b", "c"}
		allTags := []string{"cat", "dog", "old"}
		pets := map[string]*pet{}
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for i := range 120 {
				p := &pet{Owner: owners[rng.IntN(3)], Name: names[rng.IntN(3)], Age: rng.IntN(5)}
				for _, tag := range allTags {
					if rng.IntN(2) == 0 {
						p.Tags = append(p.Tags, tag)
					}
				}
				pets[keyOf(i)] = p
				if err := store.Instantiate(tx).Set(keyOf(i), p); err != nil {
					return err
				}
			}
			return nil
		}))

		// lookup is a lookup in an index, with the records that it must find.
		type lookup struct {
			open func(tx kv.Tx) index.Keyed[string, pet]
			has  func(p *pet) bool
		}
		lookups := []func() lookup{
			func() lookup {
				owner := owners[rng.IntN(3)]
				return lookup{
					open: func(tx kv.Tx) index.Keyed[string, pet] { return must(byOwner.Instantiate(tx).Match(owner)) },
					has:  func(p *pet) bool { return p.Owner == owner },
				}
			},
			func() lookup {
				tag := allTags[rng.IntN(3)]
				return lookup{
					open: func(tx kv.Tx) index.Keyed[string, pet] { return must(byTag.Instantiate(tx).Match(tag)) },
					has:  func(p *pet) bool { return slices.Contains(p.Tags, tag) },
				}
			},
			func() lookup {
				// Every record that has a tag, which is every record of the index.
				return lookup{
					open: func(tx kv.Tx) index.Keyed[string, pet] {
						return must(byTag.Instantiate(tx).Scan(index.Bounds{}))
					},
					has: func(p *pet) bool { return len(p.Tags) > 0 },
				}
			},
			func() lookup {
				age := rng.IntN(5)
				return lookup{
					open: func(tx kv.Tx) index.Keyed[string, pet] {
						// With a condition that the index does not serve.
						return must(byAge.Instantiate(tx).Lookup(ageField.Gte(age), cond.Not(nameField.Eq("b"))))
					},
					has: func(p *pet) bool { return p.Age >= age && p.Name != "b" },
				}
			},
			func() lookup {
				name := names[rng.IntN(3)]
				return lookup{
					// An index that only has the records that satisfy its own condition.
					open: func(tx kv.Tx) index.Keyed[string, pet] { return must(oldByName.Instantiate(tx).Match(name)) },
					has:  func(p *pet) bool { return p.Age >= 2 && p.Name == name },
				}
			},
		}

		want := func(has func(p *pet) bool) []string {
			keys := []string{}
			for key, p := range pets {
				if has(p) {
					keys = append(keys, key)
				}
			}
			slices.Sort(keys)
			return keys
		}
		sorted := func(src index.Keyed[string, pet], err error) []string {
			keys := keysOfSource(t, src, err)
			slices.Sort(keys)
			return keys
		}

		for range 150 {
			a, b, c := lookups[rng.IntN(len(lookups))](), lookups[rng.IntN(len(lookups))](), lookups[rng.IntN(len(lookups))]()

			err := db.View(func(tx kv.Tx) error {
				for name, cmb := range map[string]index.Combiner{"exact": {}, "lossy": index.Lossy} {
					require.Equal(t, want(func(p *pet) bool { return a.has(p) && b.has(p) }),
						sorted(cmb.Intersect(a.open(tx), b.open(tx))), name)
					require.Equal(t, want(func(p *pet) bool { return a.has(p) || b.has(p) || c.has(p) }),
						sorted(cmb.Union(a.open(tx), b.open(tx), c.open(tx))), name)
					require.Equal(t, want(func(p *pet) bool { return a.has(p) && !b.has(p) }),
						sorted(cmb.Except(a.open(tx), b.open(tx))), name)

					// (a or b) and not c, and a and (b and not c).
					union, err := cmb.Union(a.open(tx), b.open(tx))
					require.NoError(t, err)
					require.Equal(t, want(func(p *pet) bool { return (a.has(p) || b.has(p)) && !c.has(p) }),
						sorted(cmb.Except(union, c.open(tx))), name)

					except, err := cmb.Except(b.open(tx), c.open(tx))
					require.NoError(t, err)
					require.Equal(t, want(func(p *pet) bool { return a.has(p) && b.has(p) && !c.has(p) }),
						sorted(cmb.Intersect(a.open(tx), except)), name)

					// c and not (a and b): the second source is a combination.
					both, err := cmb.Intersect(a.open(tx), b.open(tx))
					require.NoError(t, err)
					require.Equal(t, want(func(p *pet) bool { return c.has(p) && !(a.has(p) && b.has(p)) }),
						sorted(cmb.Except(c.open(tx), both)), name)
				}

				// A bitmap of what a lookup finds.
				bm, exact, err := index.Bitmap(a.open(tx))
				require.NoError(t, err)
				require.Equal(t, exactBitmap, exact)
				inBitmap := sorted(index.InBitmap(b.open(tx), bm, exact), nil)
				both := want(func(p *pet) bool { return a.has(p) && b.has(p) })
				if exact {
					require.Equal(t, both, inBitmap)
				} else {
					// With hashes, the records that are in the bitmap and maybe some others.
					for _, key := range both {
						require.Contains(t, inBitmap, key)
					}
					require.Subset(t, want(b.has), inBitmap)
				}
				return nil
			})
			require.NoError(t, err)
		}
	}

	// Keys of one size, of 8 bytes at most, are numbers in a bitmap.
	t.Run("ShortKeys", func(t *testing.T) {
		run(t, func(i int) string { return fmt.Sprintf("k%03d", i) }, true)
	})
	// Other keys are kept as they are, or as hashes.
	t.Run("LongKeys", func(t *testing.T) {
		run(t, func(i int) string { return fmt.Sprintf("a-long-key-%03d", i) }, false)
	})
	t.Run("KeysOfSeveralSizes", func(t *testing.T) {
		run(t, func(i int) string { return fmt.Sprintf("k%d", i*i) }, false)
	})
}

func TestBitmap(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := extstore.New[int64, pet](schema.New().Namespace("pets"))
		byOwner := index.On(store, "by_owner", ownerField)
		byTag := index.On(store, "by_tag", tagsField)
		byAge := index.On(store, "by_age", ageField)
		named := extstore.New[string, pet](schema.New().Namespace("named"))
		namedByOwner := index.On(named, "by_owner", ownerField)

		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id, p := range map[int64]pet{
				-3: {Owner: "ann", Age: 3, Tags: []string{"cat"}},
				2:  {Owner: "ann", Age: 9, Tags: []string{"dog"}},
				3:  {Owner: "bob", Age: 9, Tags: []string{"cat"}},
				40: {Owner: "ann", Age: 1, Tags: []string{"cat", "dog"}},
			} {
				require.NoError(t, store.Instantiate(tx).Set(id, &p))
				require.NoError(t, named.Instantiate(tx).Set(fmt.Sprint("pet number ", id), &p))
			}
			return nil
		}))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ann, exact, err := index.Bitmap[int64, pet](must(byOwner.Instantiate(tx).Match("ann")))
			require.NoError(t, err)
			require.True(t, exact)
			cats, _, err := index.Bitmap[int64, pet](must(byTag.Instantiate(tx).Match("cat")))
			require.NoError(t, err)
			old, _, err := index.Bitmap[int64, pet](must(byAge.Instantiate(tx).Lookup(ageField.Gt(5))))
			require.NoError(t, err)
			require.Equal(t, uint64(3), ann.GetCardinality())

			// The bitmaps are combined as bitmaps are, and the records of the result are read.
			records := func(bm *roaring64.Bitmap) []int64 {
				return keysOfSource(t, must(index.FromBitmap(store, tx, bm)), nil)
			}
			// In the order of the keys, with the negative ones first.
			require.Equal(t, []int64{-3, 2, 40}, records(ann))
			require.Equal(t, []int64{-3, 40}, records(roaring64.And(ann, cats)))
			require.Equal(t, []int64{-3, 2, 3, 40}, records(roaring64.Or(ann, old)))
			require.Equal(t, []int64{-3, 40}, records(roaring64.AndNot(ann, old)))
			require.Equal(t, []int64{}, records(roaring64.New()))

			it := must(index.FromBitmap(store, tx, roaring64.And(ann, old)))
			defer it.Close()
			require.True(t, it.Valid())
			record, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, &pet{Owner: "ann", Age: 9, Tags: []string{"dog"}}, record)

			// A bitmap is a source like the others.
			got, err := index.Except(must(index.FromBitmap(store, tx, ann)), must(byTag.Instantiate(tx).Match("dog")))
			require.Equal(t, []int64{-3}, keysOfSource(t, got, err))
			require.Equal(t, []int64{-3, 40},
				keysOfSource(t, index.InBitmap[int64, pet](must(byTag.Instantiate(tx).Match("cat")), ann, true), nil))

			// Keys that are not numbers are in a bitmap as hashes, which cannot be turned back
			// into keys.
			annNamed, exact, err := index.Bitmap[string, pet](must(namedByOwner.Instantiate(tx).Match("ann")))
			require.NoError(t, err)
			require.False(t, exact)
			require.Equal(t, uint64(3), annNamed.GetCardinality())
			_, err = index.FromBitmap(named, tx, annNamed)
			require.ErrorIs(t, err, index.ErrNotExact)

			candidates := keysOfSource(t, index.InBitmap[string, pet](must(namedByOwner.Instantiate(tx).Scan(index.Bounds{})), annNamed, false), nil)
			require.Equal(t, []string{"pet number -3", "pet number 2", "pet number 40"}, candidates)
			return nil
		}))
	})
}

func TestContains(t *testing.T) {
	db := memkv.New()
	store := extstore.New[int64, pet](schema.New().Namespace("pets"))
	byOwnerAge := index.Composite(store, "by_owner_age", field.List(ownerField, ageField))
	byTag := index.On(store, "by_tag", tagsField)

	require.NoError(t, db.View(func(tx kv.Tx) error {
		contains := func(it *index.Iterator[int64, pet], p *pet) bool {
			defer it.Close()
			ok, err := it.Contains(nil, p)
			require.NoError(t, err)
			return ok
		}

		lookup := func() *index.Iterator[int64, pet] {
			return must(byOwnerAge.Instantiate(tx).Lookup(ownerField.Eq("ann"), ageField.Gt(2), nameField.Eq("rex")))
		}
		require.True(t, contains(lookup(), &pet{Owner: "ann", Age: 3, Name: "rex"}))
		require.False(t, contains(lookup(), &pet{Owner: "ann", Age: 2, Name: "rex"}))
		require.False(t, contains(lookup(), &pet{Owner: "bob", Age: 3, Name: "rex"}))
		// A condition that the index does not serve counts too.
		require.False(t, contains(lookup(), &pet{Owner: "ann", Age: 3, Name: "tom"}))

		// A record that has no entry in an index is not in a scan of the whole index.
		all := func() *index.Iterator[int64, pet] { return must(byTag.Instantiate(tx).Scan(index.Bounds{})) }
		require.True(t, contains(all(), &pet{Tags: []string{"cat"}}))
		require.False(t, contains(all(), &pet{}))
		return nil
	}))
}
