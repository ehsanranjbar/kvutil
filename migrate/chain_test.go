package migrate_test

import (
	"context"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

// PetV2 is a pet as the second version of the program wrote it, which is read in the type of
// today but is not the same type.
type PetV2 struct {
	id      int64
	Name    string
	Species string
	Legs    int
}

func (p *PetV2) GetId() int64   { return p.id }
func (p *PetV2) SetId(id int64) { p.id = id }

// The two steps of a chain: from version 1, whose type is PetV1, and from version 2, whose type
// is PetV2. The legs are counted in tens in the third one.
func fromV1(id int64, old *PetV1) (*Pet, error) { return toV2(id, old) }

func fromV2(_ int64, old *PetV2) (*Pet, error) {
	return &Pet{Name: old.Name, Species: old.Species, Legs: old.Legs * 10}, nil
}

func checkChained(t *testing.T, got []*Pet) {
	t.Helper()
	require.Len(t, got, 3)
	for i, p := range got {
		require.Equal(t, []string{"a", "b", "c"}[i], p.Name)
		require.Equal(t, "dog", p.Species)
		require.Equal(t, 40, p.Legs)
	}
}

var v1Pets = []*PetV1{{id: 1, Name: "a", Kind: "dog"}, {id: 2, Name: "b", Kind: "dog"}, {id: 3, Name: "c", Kind: "dog"}}

func TestMigrateFromTyped(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		old := typed.New[int64, PetV1]("pets")
		run(t, db, old)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for _, p := range v1Pets {
				if err := old.Instantiate(tx).Set(p.id, p); err != nil {
					return err
				}
			}
			return nil
		}))

		pets := typed.New[int64, Pet]("pets").WithVersion(3).
			MigrateFrom(1, fromV1).
			MigrateFrom(2, fromV2)
		require.ErrorIs(t, pets.Check(db), migrate.ErrNeedsMigration)
		report, err := pets.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated typed "pets" from version 1 to 3`, report.String())

		require.NoError(t, db.View(func(tx kv.Tx) error {
			checkChained(t, must(iterutil.Of(must(pets.Instantiate(tx).All())).Values()))
			return nil
		}))
	})
}

func TestMigrateFromExt(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		old := extstore.New[int64, PetV1]("pets")
		run(t, db, old)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for _, p := range v1Pets {
				if err := old.Instantiate(tx).Set(p.id, p); err != nil {
					return err
				}
			}
			return nil
		}))

		pets := extstore.New[int64, Pet]("pets").WithVersion(3).
			MigrateFrom(1, fromV1).
			MigrateFrom(2, fromV2)
		require.ErrorIs(t, pets.Check(db), migrate.ErrNeedsMigration)
		report, err := pets.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated ext "pets" from version 1 to 3`, report.String())

		require.NoError(t, db.View(func(tx kv.Tx) error {
			checkChained(t, must(iterutil.Of(must(pets.Instantiate(tx).All())).Values()))
			return nil
		}))
	})
}

func TestMigrateFromRecord(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		old := record.New[int64, PetV1]("pets")
		run(t, db, old)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for _, p := range v1Pets {
				if err := old.Instantiate(tx).Set(p); err != nil {
					return err
				}
			}
			return nil
		}))

		pets := record.New[int64, Pet]("pets").WithVersion(3).
			MigrateFrom(1, fromV1).
			MigrateFrom(2, fromV2)
		require.ErrorIs(t, pets.Check(db), migrate.ErrNeedsMigration)
		report, err := pets.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated record "pets" from version 1 to 3`, report.String())

		require.NoError(t, db.View(func(tx kv.Tx) error {
			checkChained(t, must(iterutil.Of(must(pets.Instantiate(tx).All())).Values()))
			return nil
		}))
	})
}
