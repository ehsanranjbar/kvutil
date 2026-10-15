package index_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/stretchr/testify/require"
)

// kennel is a version of a program with a store of pets and the indexes that it defines.
type kennel struct {
	sch     *schema.Schema
	pets    *extstore.Store[int64, pet]
	byName  *index.FieldIndex[int64, pet, string]
	byTag   *index.FieldIndex[int64, pet, string]
	byChip  *index.FieldIndex[int64, pet, string]
	byOwner *index.Index[int64, pet]
	olds    *index.FieldIndex[int64, pet, int]
}

// newKennel defines the store, and the indexes if they are asked for. The old pets are the
// ones from an age on.
func newKennel(indexes bool, oldFrom int) *kennel {
	k := &kennel{sch: schema.New()}
	k.pets = extstore.New[int64, pet](k.sch.Namespace("pets"))
	if indexes {
		k.byName = index.On(k.pets, "by_name", nameField)
		k.byTag = index.On(k.pets, "by_tag", tagsField)
		k.byChip = index.Unique(k.pets, "by_chip", chipField)
		k.byOwner = index.Composite(k.pets, "by_owner_age", field.List(ownerField, ageField.Desc()),
			index.Covering(name, nil, "Name"))
		k.olds = index.On(k.pets, "olds", ageField, index.Where(ageField.Gte(oldFrom)))
	}
	return k
}

// verify checks every index against the pets that the store has.
func (k *kennel) verify(t *testing.T, db kv.DB, oldFrom int) {
	t.Helper()

	require.NoError(t, db.View(func(tx kv.Tx) error {
		all := map[int64]*pet{}
		it := must(k.pets.Instantiate(tx).Iterator(kvutil.All[int64]()))
		var err error
		for id, p := range iterutil.Of[int64, *pet](it).All(&err) {
			all[id] = p
		}
		require.NoError(t, err)

		// want returns the keys of the pets that satisfy a condition, in the order of the keys.
		want := func(match func(*pet) bool) []int64 {
			keys := []int64{}
			for id, p := range all {
				if match(p) {
					keys = append(keys, id)
				}
			}
			slices.Sort(keys)
			return keys
		}
		sorted := func(keys []int64) []int64 {
			slices.Sort(keys)
			return keys
		}

		for _, n := range []string{"rex", "tom", "kit", "none"} {
			require.Equal(t, want(func(p *pet) bool { return p.Name == n }),
				sorted(ids(t, must(k.byName.Instantiate(tx).Match(n)))), n)
		}
		for _, tag := range []string{"big", "old", "shy"} {
			require.Equal(t, want(func(p *pet) bool { return slices.Contains(p.Tags, tag) }),
				sorted(ids(t, must(k.byTag.Instantiate(tx).Match(tag)))), tag)
		}
		for _, owner := range []string{"ann", "bob"} {
			require.Equal(t, want(func(p *pet) bool { return p.Owner == owner && p.Age >= 5 }),
				sorted(ids(t, must(k.byOwner.Instantiate(tx).Lookup(ownerField.Eq(owner), ageField.Gte(5))))), owner)
		}
		require.Equal(t, want(func(p *pet) bool { return p.Age >= oldFrom }),
			sorted(ids(t, must(k.olds.Instantiate(tx).Range(kvutil.All[int]())))))
		for id, p := range all {
			if p.Chip == "" {
				continue
			}
			found, ok, err := k.byChip.Instantiate(tx).Find(p.Chip)
			require.NoError(t, err)
			require.True(t, ok, p.Chip)
			require.Equal(t, id, found)
		}

		// The index has no entries of pets that are not there, and has the values that are
		// read from it without the pets.
		named := must(k.byOwner.Instantiate(tx).Scan(index.Bounds{}))
		defer named.Close()
		entries := 0
		for ; named.Valid(); named.Next() {
			entries++
			id := must(named.Key())
			covered := must(named.Covered())
			require.Contains(t, string(covered), all[id].Name)
		}
		require.NoError(t, named.Err())
		require.Equal(t, len(all), entries)
		return nil
	}))
}

func randomPet(rng *rand.Rand, id int64) *pet {
	p := &pet{
		Name:  []string{"rex", "tom", "kit"}[rng.IntN(3)],
		Owner: []string{"ann", "bob"}[rng.IntN(2)],
		Age:   rng.IntN(12),
		Chip:  fmt.Sprint("chip", id),
	}
	for _, tag := range []string{"big", "old", "shy"} {
		if rng.IntN(3) == 0 {
			p.Tags = append(p.Tags, tag)
		}
	}
	if rng.IntN(5) == 0 {
		p.Chip = ""
	}
	return p
}

func migrated(t *testing.T, db kv.DB, sch *schema.Schema) string {
	t.Helper()

	report, err := sch.Migrate(context.Background(), db)
	require.NoError(t, err)
	return report.String()
}

// TestBackfill checks that the indexes that are defined on a store with records in it are
// built from the records, and are kept up to date from then on.
func TestBackfill(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// More pets than a batch of a build has.
		const n = 1300
		rng := rand.New(rand.NewPCG(1, 2))

		// A program without indexes writes the pets.
		v1 := newKennel(false, 0)
		migrated(t, db, v1.sch)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= n; id++ {
				if err := v1.pets.Instantiate(tx).Set(id, randomPet(rng, id)); err != nil {
					return err
				}
			}
			return nil
		}))

		// The next one defines them.
		v2 := newKennel(true, 8)
		require.ErrorIs(t, v2.sch.Check(db), schema.ErrNeedsMigration)
		require.Equal(t, `built index "pets/ext/by_name"
built index "pets/ext/by_tag"
built index "pets/ext/by_chip"
built index "pets/ext/by_owner_age"
built index "pets/ext/olds"`, migrated(t, db, v2.sch))
		v2.verify(t, db, 8)
		require.NoError(t, v2.sch.Check(db))
		require.Empty(t, migrated(t, db, v2.sch))

		// They are kept up to date as any index is.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := v2.pets.Instantiate(tx)
			for range 300 {
				id := int64(1 + rng.IntN(n+50))
				if rng.IntN(4) == 0 {
					if err := store.Delete(id); err != nil {
						return err
					}
					continue
				}
				if err := store.Set(id, randomPet(rng, id)); err != nil {
					return err
				}
			}
			return nil
		}))
		v2.verify(t, db, 8)

		// An index whose definition has changed is built again: the old pets are others.
		v3 := newKennel(true, 3)
		require.ErrorIs(t, v3.sch.Check(db), schema.ErrNeedsMigration)
		require.Equal(t, `built index "pets/ext/olds"`, migrated(t, db, v3.sch))
		v3.verify(t, db, 3)

		// A program without the indexes deletes them, as it would not keep them up to date.
		v4 := newKennel(false, 0)
		require.Contains(t, migrated(t, db, v4.sch), `dropped index "pets/ext/by_name"`)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it, err := v2.byName.Namespace().Open(tx).Iterator(nil, nil)
			require.NoError(t, err)
			defer it.Close()
			require.False(t, it.Valid())
			return nil
		}))
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := v4.pets.Instantiate(tx)
			for id := int64(1); id <= 200; id++ {
				if err := store.Set(id, randomPet(rng, id)); err != nil {
					return err
				}
			}
			return store.Delete(n)
		}))

		// So they are built again when they are defined again.
		v5 := newKennel(true, 3)
		require.ErrorIs(t, v5.sch.Check(db), schema.ErrNeedsMigration)
		require.Contains(t, migrated(t, db, v5.sch), `built index "pets/ext/by_chip"`)
		v5.verify(t, db, 3)
	})
}

func TestRebuild(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		k := newKennel(true, 8)
		require.NoError(t, k.sch.Check(db))

		rng := rand.New(rand.NewPCG(3, 4))
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= 40; id++ {
				if err := k.pets.Instantiate(tx).Set(id, randomPet(rng, id)); err != nil {
					return err
				}
			}
			// Entries that no pet has, and a pet without its entries.
			refs := k.byName.Namespace().Open(tx)
			if err := refs.Set([]byte("zzz\x00\x01\x80\x00\x00\x00\x00\x00\x00\x63\x08"), nil); err != nil {
				return err
			}
			return k.pets.Data().Instantiate(tx).Set(99, &pet{Name: "kit", Owner: "ann", Age: 9, Chip: "c99"})
		}))

		ctx := context.Background()
		require.NoError(t, index.Rebuild(ctx, db, k.byName.Index))
		require.NoError(t, index.Rebuild(ctx, db, k.byTag.Index))
		require.NoError(t, index.Rebuild(ctx, db, k.byChip.Index))
		require.NoError(t, index.Rebuild(ctx, db, k.byOwner))
		require.NoError(t, index.Rebuild(ctx, db, k.olds.Index))
		k.verify(t, db, 8)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			require.Empty(t, ids(t, must(k.byName.Instantiate(tx).Match("zzz"))))
			return nil
		}))

		// A build that is cancelled stops.
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, index.Rebuild(cancelled, db, k.byName.Index), context.Canceled)
	})
}

// TestBackfillUnique checks that a unique index cannot be built on records that do not all
// have keys of their own, and can be once they have.
func TestBackfillUnique(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		define := func(unique bool) (*schema.Schema, *extstore.Store[int64, pet], *index.FieldIndex[int64, pet, string]) {
			sch := schema.New()
			pets := extstore.New[int64, pet](sch.Namespace("pets"))
			if !unique {
				return sch, pets, nil
			}
			return sch, pets, index.Unique(pets, "by_chip", field.Of("Chip", func(p *pet) string { return p.Chip }))
		}

		sch, pets, _ := define(false)
		migrated(t, db, sch)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id, chip := range map[int64]string{1: "a", 2: "b", 3: "c", 7: "b"} {
				if err := pets.Instantiate(tx).Set(id, &pet{Chip: chip}); err != nil {
					return err
				}
			}
			return nil
		}))

		// The two records are named.
		sch, _, _ = define(true)
		_, err := sch.Migrate(context.Background(), db)
		require.ErrorIs(t, err, index.ErrUniqueViolation)
		require.ErrorContains(t, err, `index "pets/ext/by_chip"`)
		require.ErrorContains(t, err, "the record 7 has a key that the record 2 has")

		// The index is not for use, and the same program cannot go on with a check.
		sch, _, _ = define(true)
		require.ErrorIs(t, sch.Check(db), schema.ErrNeedsMigration)
		_, err = sch.Migrate(context.Background(), db)
		require.ErrorIs(t, err, index.ErrUniqueViolation)

		// Once the records have keys of their own, it is built.
		sch, pets, _ = define(false)
		migrated(t, db, sch)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return pets.Instantiate(tx).Set(7, &pet{Chip: "d"})
		}))
		sch, pets, byChip := define(true)
		require.Equal(t, `built index "pets/ext/by_chip"`, migrated(t, db, sch))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			id, ok, err := byChip.Instantiate(tx).Find("d")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, int64(7), id)
			return nil
		}))
		err = db.Update(func(tx kv.Tx) error {
			return pets.Instantiate(tx).Set(8, &pet{Chip: "a"})
		})
		require.ErrorIs(t, err, index.ErrUniqueViolation)
	})
}
