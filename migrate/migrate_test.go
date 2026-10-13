package migrate_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

// PetV1 is a pet as the first version of the program wrote it.
type PetV1 struct {
	id   int64
	Name string
	Kind string
}

func (p *PetV1) GetId() int64   { return p.id }
func (p *PetV1) SetId(id int64) { p.id = id }

// Pet is a pet as the program has it now.
type Pet struct {
	id      int64
	Name    string
	Species string
	Legs    int
}

func (p *Pet) GetId() int64   { return p.id }
func (p *Pet) SetId(id int64) { p.id = id }

// The stores of both kinds can be migrated.
var (
	_ migrate.Store[int64, Pet] = (*typed.Store[int64, Pet])(nil)
	_ migrate.Store[int64, Pet] = (*extstore.Store[int64, Pet])(nil)
	_ migrate.Store[int64, Pet] = (*record.Store[int64, Pet])(nil)
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func run(t *testing.T, db kv.DB, sch *schema.Schema) {
	t.Helper()

	_, err := sch.Migrate(context.Background(), db)
	require.NoError(t, err)
}

// toV2 converts a pet. A pet without a name is one that was never finished, and is dropped.
func toV2(id int64, old *PetV1) (*Pet, error) {
	if old.Name == "" {
		return nil, nil
	}
	legs := 4
	if old.Kind == "bird" {
		legs = 2
	}
	return &Pet{Name: old.Name, Species: old.Kind, Legs: legs}, nil
}

func TestRecords(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		const n = 300

		// The first version of the program writes its pets.
		v1 := schema.New()
		old := record.New[int64, PetV1](v1.Namespace("pets"))
		run(t, db, v1)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= n; id++ {
				p := &PetV1{id: id, Name: fmt.Sprint("pet", id), Kind: []string{"dog", "bird"}[id%2]}
				if id%10 == 0 {
					p.Name = ""
				}
				if err := old.Instantiate(tx).Set(p); err != nil {
					return err
				}
			}
			return nil
		}))

		// The second one has another type, and says how the pets get there.
		v2 := schema.New()
		pets := record.New[int64, Pet](v2.Namespace("pets", schema.Version(2)))
		var seen []int64
		migrate.Records(pets, 1, func(id int64, old *PetV1) (*Pet, error) {
			seen = append(seen, id)
			return toV2(id, old)
		}, migrate.WithBatchSize(7))

		require.ErrorIs(t, v2.Check(db), schema.ErrNeedsMigration)
		report, err := v2.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated unit "pets" from version 1 to 2`, report.String())

		// Every pet was given once, in the order of the ids, with its id.
		require.Len(t, seen, n)
		for i, id := range seen {
			require.Equal(t, int64(i+1), id)
		}

		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := must(pets.Instantiate(tx).Iterator(kvutil.All[int64]()))
			got := must(iterutil.Of[int64, *Pet](it).Values())
			require.Len(t, got, n-n/10)
			for _, p := range got {
				require.NotZero(t, p.id%10)
				require.Equal(t, fmt.Sprint("pet", p.id), p.Name)
				require.Equal(t, []string{"dog", "bird"}[p.id%2], p.Species)
				require.Equal(t, []int{4, 2}[p.id%2], p.Legs)
			}
			return nil
		}))

		// It is done once.
		seen = nil
		run(t, db, v2)
		require.NoError(t, v2.Check(db))
		require.Empty(t, seen)
	})
}

// Counter is a record whose conversion shows if it is done more than once.
type Counter struct {
	N int
}

func TestRecordsInterrupted(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		const n = 100

		v1 := schema.New()
		old := typed.New[string, Counter](v1.Namespace("counters"))
		run(t, db, v1)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for i := range n {
				if err := old.Instantiate(tx).Set(fmt.Sprintf("c%03d", i), &Counter{N: i}); err != nil {
					return err
				}
			}
			return nil
		}))

		errStopped := errors.New("stopped")
		define := func(failAt string) (*schema.Schema, *typed.Store[string, Counter]) {
			sch := schema.New()
			counters := typed.New[string, Counter](sch.Namespace("counters", schema.Version(2)))
			migrate.Records(counters, 1, func(key string, old *Counter) (*Counter, error) {
				if key == failAt {
					return nil, errStopped
				}
				return &Counter{N: old.N + 1000}, nil
			}, migrate.WithBatchSize(8))
			return sch, counters
		}
		values := func(counters *typed.Store[string, Counter]) (ns []int) {
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it := must(counters.Instantiate(tx).Iterator(kvutil.All[string]()))
				for _, c := range must(iterutil.Of[string, *Counter](it).Values()) {
					ns = append(ns, c.N)
				}
				return nil
			}))
			return ns
		}

		// It stops in the middle of a batch, twice, after the batches before it were written.
		for _, failAt := range []string{"c037", "c038", "c090"} {
			sch, counters := define(failAt)
			_, err := sch.Migrate(context.Background(), db)
			require.ErrorIs(t, err, errStopped)
			require.ErrorContains(t, err, `"counters"`)

			converted := 0
			for _, n := range values(counters) {
				if n >= 1000 {
					converted++
				}
			}
			require.Positive(t, converted)
			require.Less(t, converted, n)
			require.Zero(t, converted%8)
		}

		// It goes on, and no counter is converted twice.
		sch, counters := define("")
		run(t, db, sch)
		for i, n := range values(counters) {
			require.Equal(t, i+1000, n)
		}
		require.NoError(t, sch.Check(db))
	})
}

func TestRecordsWithAnotherCodec(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// The first version wrote JSON, and the second one writes the default format.
		v1 := schema.New()
		old := extstore.NewWithCodecs[int64, PetV1](v1.Namespace("pets"), nil, codec.Formatted[PetV1](codec.JSON))
		run(t, db, v1)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return old.Instantiate(tx).Set(7, &PetV1{Name: "rex", Kind: "dog"})
		}))

		// Another codec without another version is a mistake.
		same := schema.New()
		extstore.New[int64, Pet](same.Namespace("pets"))
		require.ErrorIs(t, same.Check(db), schema.ErrChanged)

		v2 := schema.New()
		pets := extstore.New[int64, Pet](v2.Namespace("pets", schema.Version(2)))
		migrate.Records(pets, 1, toV2, migrate.WithOldCodec(codec.Formatted[PetV1](codec.JSON)))
		run(t, db, v2)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			p := must(pets.Instantiate(tx).Get(7))
			require.Equal(t, Pet{Name: "rex", Species: "dog", Legs: 4}, *p)
			return nil
		}))

		// Without the codec that the records were written with, they cannot be read.
		db2 := testkv.NewMem(t)
		run(t, db2, v1)
		require.NoError(t, db2.Update(func(tx kv.Tx) error {
			return old.Instantiate(tx).Set(7, &PetV1{Name: "rex", Kind: "dog"})
		}))
		wrong := schema.New()
		migrate.Records(extstore.New[int64, Pet](wrong.Namespace("pets", schema.Version(2))), 1, toV2)
		_, err := wrong.Migrate(context.Background(), db2)
		require.ErrorContains(t, err, "failed to decode the record as it was")

		// The codec of the option is one of the old records.
		require.Panics(t, func() {
			sch := schema.New()
			migrate.Records(extstore.New[int64, Pet](sch.Namespace("pets", schema.Version(2))), 1, toV2,
				migrate.WithOldCodec(codec.Formatted[Pet](codec.JSON)))
		})
	})
}

func TestCustom(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := schema.New()
		old := record.New[int64, PetV1](v1.Namespace("pets"))
		run(t, db, v1)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for id := int64(1); id <= 20; id++ {
				if err := old.Instantiate(tx).Set(&PetV1{id: id, Name: "pet", Kind: "dog"}); err != nil {
					return err
				}
			}
			return nil
		}))

		// A migration that has something to do before the records are converted, which it
		// must not do again when it goes on after it was interrupted.
		errStopped := errors.New("stopped")
		prepared := 0
		define := func(failAt int64) (*schema.Schema, *record.Store[int64, Pet]) {
			sch := schema.New()
			pets := record.New[int64, Pet](sch.Namespace("pets", schema.Version(3)))
			migrate.Custom(pets, 1, func(ctx context.Context, m *migrate.Migrator) error {
				if m.Cursor() == nil {
					prepared++
				}
				return migrate.Convert(ctx, m, pets, func(id int64, old *PetV1) (*Pet, error) {
					if id == failAt {
						return nil, errStopped
					}
					return toV2(id, old)
				}, migrate.WithBatchSize(5))
			})
			// And one from the type of today to itself, for the next version.
			migrate.Records(pets, 2, func(_ int64, p *Pet) (*Pet, error) {
				p.Legs *= 10
				return p, nil
			})
			return sch, pets
		}

		sch, _ := define(13)
		_, err := sch.Migrate(context.Background(), db)
		require.ErrorIs(t, err, errStopped)

		sch, pets := define(0)
		report, err := sch.Migrate(context.Background(), db)
		require.NoError(t, err)
		require.Equal(t, `migrated unit "pets" from version 1 to 3`, report.String())
		require.Equal(t, 1, prepared)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := must(pets.Instantiate(tx).Iterator(kvutil.All[int64]()))
			got := must(iterutil.Of[int64, *Pet](it).Values())
			require.Len(t, got, 20)
			for _, p := range got {
				require.Equal(t, Pet{id: p.id, Name: "pet", Species: "dog", Legs: 40}, *p)
			}
			return nil
		}))
	})
}
