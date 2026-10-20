package migrate_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/stretchr/testify/require"
)

// TestRecordsRebuildWhatIsDerived checks that the indexes and the aggregations of a store are
// what its records give after the records were migrated, with nothing said of them.
func TestRecordsRebuildWhatIsDerived(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		const n = 60

		// The first version, with an index and an aggregation by the names of the pets.
		old := record.New[int64, PetV1]("pets")
		index.On(old, "by_name", field.Path[PetV1, string]("Name"))
		agg.Count(old, "per_name", func(p *PetV1) (string, bool) { return p.Name, true })
		run(t, db, old)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= n; id++ {
				p := &PetV1{id: id, Name: []string{"rex", "tom", ""}[id%3], Kind: "dog"}
				if err := old.Instantiate(tx).Set(p); err != nil {
					return err
				}
			}
			return nil
		}))

		// The second one defines the same two, and one more of each, on the new type. The
		// migration changes the names and drops the pets that have none.
		define := func() (*record.Store[int64, Pet],
			*index.FieldIndex[int64, Pet, string], *index.FieldIndex[int64, Pet, int],
			*agg.Aggregation[int64, Pet, string, int64, int64], *agg.Aggregation[int64, Pet, agg.NoGroup, int, int]) {
			pets := record.New[int64, Pet]("pets").WithVersion(2)
			byName := index.On(pets, "by_name", field.Path[Pet, string]("Name"))
			byLegs := index.On(pets, "by_legs", field.Path[Pet, int]("Legs"))
			perName := agg.Count(pets, "per_name", func(p *Pet) (string, bool) { return p.Name, true })
			legs := agg.Sum(pets, "legs", func(p *Pet) (agg.NoGroup, int, bool) { return agg.NoGroup{}, p.Legs, true })
			migrate.Records(pets, 1, func(id int64, old *PetV1) (*Pet, error) {
				p, err := toV2(id, old)
				if p != nil {
					p.Name = strings.ToUpper(p.Name)
				}
				return p, err
			}, migrate.WithBatchSize(9))
			return pets, byName, byLegs, perName, legs
		}

		pets, byName, byLegs, perName, legs := define()
		require.ErrorIs(t, pets.Check(db), migrate.ErrNeedsMigration)
		report, err := pets.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated record "pets" from version 1 to 2
built index "pets/by_name"
built index "pets/by_legs"
built agg "pets/per_name"
built agg "pets/legs"`, report.String())

		verify := func(rex, tom int) {
			t.Helper()

			require.NoError(t, db.View(func(tx kv.Tx) error {
				count := func(it *index.Iterator[int64, Pet], err error) int {
					require.NoError(t, err)
					return len(must(iterutil.Of(it).Keys()))
				}
				require.Equal(t, rex, count(byName.Instantiate(tx).Match("REX")))
				require.Equal(t, tom, count(byName.Instantiate(tx).Match("TOM")))
				require.Zero(t, count(byName.Instantiate(tx).Match("rex")))
				require.Zero(t, count(byName.Instantiate(tx).Match("")))
				require.Equal(t, rex+tom, count(byLegs.Instantiate(tx).Match(4)))

				for name, want := range map[string]int{"REX": rex, "TOM": tom, "rex": 0, "": 0} {
					got, _, err := perName.Instantiate(tx).Get(name)
					require.NoError(t, err)
					require.Equal(t, int64(want), got, name)
				}
				total, _, err := legs.Instantiate(tx).Get(agg.NoGroup{})
				require.NoError(t, err)
				require.Equal(t, 4*(rex+tom), total)
				return nil
			}))
		}
		verify(n/3, n/3)

		// They are kept up to date from then on.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			if err := pets.Instantiate(tx).Set(&Pet{id: n + 1, Name: "REX", Species: "dog", Legs: 4}); err != nil {
				return err
			}
			// The first pet is a tom.
			return pets.Instantiate(tx).Delete(1)
		}))
		verify(n/3+1, n/3-1)

		// And there is nothing left to do.
		pets, _, _, _, _ = define()
		require.NoError(t, pets.Check(db))
		report, err = pets.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Empty(t, report.String())
	})
}
