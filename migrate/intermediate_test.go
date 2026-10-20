package migrate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

// PetMid is a pet as the second version of the program wrote it, in a type of its own: the
// name is a title there, and the legs are paws. Reading it as any other type of the chain
// would give pets without a name.
type PetMid struct {
	id    int64
	Title string
	Kind  string
	Paws  int
}

func (p *PetMid) GetId() int64   { return p.id }
func (p *PetMid) SetId(id int64) { p.id = id }

var midPets = []*PetMid{
	{id: 1, Title: "a", Kind: "dog", Paws: 4},
	{id: 2, Title: "b", Kind: "dog", Paws: 4},
	{id: 3, Title: "c", Kind: "dog", Paws: 4},
}

func stepV1ToMid(_ int64, old *PetV1) (*PetMid, error) {
	return &PetMid{Title: old.Name, Kind: old.Kind, Paws: 4}, nil
}

func stepMidToPet(_ int64, old *PetMid) (*Pet, error) {
	return &Pet{Name: old.Title, Species: old.Kind, Legs: old.Paws * 10}, nil
}

// steps is what a program of the third version defines for its pets.
type steps struct {
	version uint64 // 3 if it is 0
	only1   bool   // without the step from version 2
	s1      func(int64, *PetV1) (*PetMid, error)
	s2      func(int64, *PetMid) (*Pet, error)
	o1, o2  []migrate.Option
}

func (st steps) withDefaults() steps {
	if st.version == 0 {
		st.version = 3
	}
	if st.s1 == nil {
		st.s1 = stepV1ToMid
	}
	if st.s2 == nil {
		st.s2 = stepMidToPet
	}
	return st
}

type prog interface {
	migrate.Migratable
	Check(kv.DB) error
	Migrate(context.Context, kv.DB) (*migrate.Report, error)
}

// chainKind is a kind of store, with what a chain of migrations needs of it.
type chainKind struct {
	name string
	// seedV1 writes the pets of the program of version 1.
	seedV1 func(t *testing.T, db kv.DB)
	// seedMid writes the pets of the program of version 2, whose type is PetMid.
	seedMid func(t *testing.T, db kv.DB, c codec.ValueCodec[PetMid])
	// program defines the pets of the program of version 3.
	program func(st steps) (prog, func(tx kv.Tx) []*Pet)
	// programV1 defines the pets of the program of version 1.
	programV1 func() (prog, func(tx kv.Tx) []*PetV1)
}

func seedTyped[V any](t *testing.T, db kv.DB, version uint64, c codec.ValueCodec[V], vals []*V) {
	t.Helper()

	s := typed.NewWithCodecs[int64, V]("pets", nil, c).WithVersion(version)
	run(t, db, s)
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for _, v := range vals {
			if err := s.Instantiate(tx).Set(any(v).(interface{ GetId() int64 }).GetId(), v); err != nil {
				return err
			}
		}
		return nil
	}))
}

func seedExt[V any](t *testing.T, db kv.DB, version uint64, c codec.ValueCodec[V], vals []*V) {
	t.Helper()

	s := extstore.NewWithCodecs[int64, V]("pets", nil, c).WithVersion(version)
	run(t, db, s)
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for _, v := range vals {
			if err := s.Instantiate(tx).Set(any(v).(interface{ GetId() int64 }).GetId(), v); err != nil {
				return err
			}
		}
		return nil
	}))
}

func seedRecord[V any, PV record.Record[int64, V]](
	t *testing.T, db kv.DB, version uint64, c codec.ValueCodec[V], vals []*V,
) {
	t.Helper()

	s := record.NewWithCodecs[int64, V, PV]("pets", nil, c).WithVersion(version)
	run(t, db, s)
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for _, v := range vals {
			if err := s.Instantiate(tx).Set(v); err != nil {
				return err
			}
		}
		return nil
	}))
}

var chainKinds = []chainKind{
	{
		name:   "typed",
		seedV1: func(t *testing.T, db kv.DB) { seedTyped[PetV1](t, db, 1, nil, v1Pets) },
		programV1: func() (prog, func(tx kv.Tx) []*PetV1) {
			s := typed.New[int64, PetV1]("pets")
			return s, func(tx kv.Tx) []*PetV1 { return must(iterutil.Of(must(s.Instantiate(tx).All())).Values()) }
		},
		seedMid: func(t *testing.T, db kv.DB, c codec.ValueCodec[PetMid]) {
			seedTyped(t, db, 2, c, midPets)
		},
		program: func(st steps) (prog, func(tx kv.Tx) []*Pet) {
			st = st.withDefaults()
			s := typed.New[int64, Pet]("pets").WithVersion(st.version).MigrateFrom(1, st.s1, st.o1...)
			if !st.only1 {
				s.MigrateFrom(2, st.s2, st.o2...)
			}
			return s, func(tx kv.Tx) []*Pet {
				return must(iterutil.Of(must(s.Instantiate(tx).All())).Values())
			}
		},
	},
	{
		name:   "ext",
		seedV1: func(t *testing.T, db kv.DB) { seedExt[PetV1](t, db, 1, nil, v1Pets) },
		programV1: func() (prog, func(tx kv.Tx) []*PetV1) {
			s := extstore.New[int64, PetV1]("pets")
			return s, func(tx kv.Tx) []*PetV1 { return must(iterutil.Of(must(s.Instantiate(tx).All())).Values()) }
		},
		seedMid: func(t *testing.T, db kv.DB, c codec.ValueCodec[PetMid]) {
			seedExt(t, db, 2, c, midPets)
		},
		program: func(st steps) (prog, func(tx kv.Tx) []*Pet) {
			st = st.withDefaults()
			s := extstore.New[int64, Pet]("pets").WithVersion(st.version).MigrateFrom(1, st.s1, st.o1...)
			if !st.only1 {
				s.MigrateFrom(2, st.s2, st.o2...)
			}
			return s, func(tx kv.Tx) []*Pet {
				return must(iterutil.Of(must(s.Instantiate(tx).All())).Values())
			}
		},
	},
	{
		name:   "record",
		seedV1: func(t *testing.T, db kv.DB) { seedRecord[PetV1](t, db, 1, nil, v1Pets) },
		programV1: func() (prog, func(tx kv.Tx) []*PetV1) {
			s := record.New[int64, PetV1]("pets")
			return s, func(tx kv.Tx) []*PetV1 { return must(iterutil.Of(must(s.Instantiate(tx).All())).Values()) }
		},
		seedMid: func(t *testing.T, db kv.DB, c codec.ValueCodec[PetMid]) {
			seedRecord(t, db, 2, c, midPets)
		},
		program: func(st steps) (prog, func(tx kv.Tx) []*Pet) {
			st = st.withDefaults()
			s := record.New[int64, Pet]("pets").WithVersion(st.version).MigrateFrom(1, st.s1, st.o1...)
			if !st.only1 {
				s.MigrateFrom(2, st.s2, st.o2...)
			}
			return s, func(tx kv.Tx) []*Pet {
				return must(iterutil.Of(must(s.Instantiate(tx).All())).Values())
			}
		},
	},
}

func readPets(t *testing.T, db kv.DB, read func(tx kv.Tx) []*Pet) (pets []*Pet) {
	t.Helper()

	require.NoError(t, db.View(func(tx kv.Tx) error {
		pets = read(tx)
		return nil
	}))
	return pets
}

func TestMigrateFromThroughAType(t *testing.T) {
	for _, k := range chainKinds {
		t.Run(k.name, func(t *testing.T) {
			// A database of version 1 goes through both steps.
			t.Run("FromV1", func(t *testing.T) {
				testkv.Each(t, func(t *testing.T, db kv.DB) {
					k.seedV1(t, db)

					var s2 int
					pets, read := k.program(steps{s2: func(id int64, old *PetMid) (*Pet, error) {
						s2++
						require.NotEmpty(t, old.Title)
						require.NotEmpty(t, old.Kind)
						require.Equal(t, 4, old.Paws)
						return stepMidToPet(id, old)
					}})
					require.ErrorIs(t, pets.Check(db), migrate.ErrNeedsMigration)
					report, err := pets.Migrate(context.Background(), db)
					require.NoError(t, err)
					require.Equal(t, `migrated `+k.name+` "pets" from version 1 to 3`, report.String())
					require.Equal(t, 3, s2)

					checkChained(t, readPets(t, db, read))
					require.NoError(t, pets.Check(db))
				})
			})

			// A database that a program of version 2 wrote with the type of that version
			// goes through the second one, with the same program.
			t.Run("NativeV2", func(t *testing.T) {
				testkv.Each(t, func(t *testing.T, db kv.DB) {
					k.seedMid(t, db, nil)

					var s1 int
					pets, read := k.program(steps{s1: func(id int64, old *PetV1) (*PetMid, error) {
						s1++
						return stepV1ToMid(id, old)
					}})
					report, err := pets.Migrate(context.Background(), db)
					require.NoError(t, err)
					require.Equal(t, `migrated `+k.name+` "pets" from version 2 to 3`, report.String())
					require.Zero(t, s1)

					checkChained(t, readPets(t, db, read))
				})
			})
		})
	}
}

// TestMigrateFromOfTheTypeOfTheStore checks that records of the type of the store are read with
// the codec of the store.
func TestMigrateFromOfTheTypeOfTheStore(t *testing.T) {
	double := func(_ int64, p *Pet) (*Pet, error) {
		p.Legs *= 10
		return p, nil
	}
	pet := &Pet{Name: "a", Species: "dog", Legs: 4}

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		jsonPets := func(version uint64) *extstore.Store[int64, Pet] {
			return extstore.NewWithCodecs[int64, Pet]("pets", nil, codec.Formatted[Pet](codec.JSON)).
				WithVersion(version)
		}
		old := jsonPets(1)
		run(t, db, old)
		require.NoError(t, db.Update(func(tx kv.Tx) error { return old.Instantiate(tx).Set(1, pet) }))

		pets := jsonPets(2).MigrateFrom(1, double)
		run(t, db, pets)

		// What was written is read with the codec of the store, which is the one that wrote it.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			require.Equal(t, Pet{Name: "a", Species: "dog", Legs: 40}, *must(pets.Instantiate(tx).Get(1)))
			return nil
		}))
	})

	// The codec of the old records is the one of the option when there is one, as the codec of
	// the store has changed in this version.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		old := extstore.New[int64, Pet]("pets")
		run(t, db, old)
		require.NoError(t, db.Update(func(tx kv.Tx) error { return old.Instantiate(tx).Set(1, pet) }))

		jsonPets := extstore.NewWithCodecs[int64, Pet]("pets", nil, codec.Formatted[Pet](codec.JSON)).
			WithVersion(2).
			MigrateFrom(1, double, migrate.WithOldCodec(codec.ValueFor[Pet](nil)))
		run(t, db, jsonPets)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			require.Equal(t, Pet{Name: "a", Species: "dog", Legs: 40}, *must(jsonPets.Instantiate(tx).Get(1)))
			return nil
		}))

		// And it cannot be read with the codec of the store.
		db2 := testkv.NewMem(t)
		run(t, db2, old)
		require.NoError(t, db2.Update(func(tx kv.Tx) error { return old.Instantiate(tx).Set(1, pet) }))
		wrong := extstore.NewWithCodecs[int64, Pet]("pets", nil, codec.Formatted[Pet](codec.JSON)).
			WithVersion(2).
			MigrateFrom(1, double)
		_, err := wrong.Migrate(context.Background(), db2)
		require.ErrorContains(t, err, "failed to decode the record as it was")
	})
}

func TestMigrateFromWithCodecsOfAType(t *testing.T) {
	midJSON := codec.Formatted[PetMid](codec.JSON)

	for _, k := range chainKinds {
		t.Run(k.name, func(t *testing.T) {
			testkv.Each(t, func(t *testing.T, db kv.DB) {
				// The program of version 2 wrote its type with the codec of JSON. The steps
				// that write and read it are given the codec.
				k.seedV1(t, db)
				pets, read := k.program(steps{
					o1: []migrate.Option{migrate.WithNewCodec(midJSON)},
					o2: []migrate.Option{migrate.WithOldCodec(midJSON)},
				})
				run(t, db, pets)
				checkChained(t, readPets(t, db, read))
			})

			testkv.Each(t, func(t *testing.T, db kv.DB) {
				// Written with another codec than it is read with, the type is not read.
				k.seedV1(t, db)
				pets, _ := k.program(steps{o1: []migrate.Option{migrate.WithNewCodec(midJSON)}})
				_, err := pets.Migrate(context.Background(), db)
				require.ErrorContains(t, err, "failed to decode the record as it was")
			})

			testkv.Each(t, func(t *testing.T, db kv.DB) {
				// A database that the program of version 2 wrote with JSON.
				k.seedMid(t, db, midJSON)
				pets, _ := k.program(steps{})
				_, err := pets.Migrate(context.Background(), db)
				require.ErrorContains(t, err, "failed to decode the record as it was")

				pets, read := k.program(steps{o2: []migrate.Option{migrate.WithOldCodec(midJSON)}})
				run(t, db, pets)
				checkChained(t, readPets(t, db, read))
			})
		})
	}
}

func TestWithNewCodecOfAnotherType(t *testing.T) {
	// A step to a type in between, and one to the type of the store.
	mid := func() *extstore.Store[int64, Pet] { return extstore.New[int64, Pet]("pets").WithVersion(3) }

	require.NotPanics(t, func() {
		mid().MigrateFrom(1, stepV1ToMid, migrate.WithNewCodec(codec.Formatted[PetMid](codec.JSON)))
		mid().MigrateFrom(1, stepMidToPet, migrate.WithOldCodec(codec.Formatted[PetMid](codec.JSON)))
	})
	require.Panics(t, func() {
		mid().MigrateFrom(1, stepV1ToMid, migrate.WithNewCodec(codec.Formatted[Pet](codec.JSON)))
	})
	require.Panics(t, func() {
		mid().MigrateFrom(2, stepMidToPet, migrate.WithNewCodec(codec.Formatted[PetMid](codec.JSON)))
	})
	require.Panics(t, func() {
		mid().MigrateFrom(2, stepMidToPet, migrate.WithOldCodec(codec.Formatted[Pet](codec.JSON)))
	})
}

func TestMigrateFromLastStepNotOfTheStore(t *testing.T) {
	for _, k := range chainKinds {
		t.Run(k.name, func(t *testing.T) {
			testkv.Each(t, func(t *testing.T, db kv.DB) {
				k.seedV1(t, db)

				var s1 int
				pets, _ := k.program(steps{version: 2, only1: true, s1: func(id int64, old *PetV1) (*PetMid, error) {
					s1++
					return stepV1ToMid(id, old)
				}})
				_, err := pets.Migrate(context.Background(), db)
				require.ErrorContains(t, err, "migrate_test.PetMid")
				require.ErrorContains(t, err, "migrate_test.Pet, which is the type of the store")
				require.Zero(t, s1)

				// Nothing was converted: the database is as the program of version 1 left it.
				old, readOld := k.programV1()
				require.NoError(t, old.Check(db))
				require.NoError(t, db.View(func(tx kv.Tx) error {
					got := readOld(tx)
					require.Len(t, got, len(v1Pets))
					for i, p := range got {
						require.Equal(t, v1Pets[i].Name, p.Name)
						require.Equal(t, v1Pets[i].Kind, p.Kind)
					}
					return nil
				}))
			})
		})
	}

	// The version of the store may be set after the migrations.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		seedExt[PetV1](t, db, 1, nil, v1Pets)
		pets := extstore.New[int64, Pet]("pets")
		pets.MigrateFrom(1, stepV1ToMid)
		pets.WithVersion(2)
		_, err := pets.Migrate(context.Background(), db)
		require.ErrorContains(t, err, "which is the type of the store")
	})
}

func TestMigrateFromThroughATypeInterrupted(t *testing.T) {
	for _, k := range chainKinds {
		t.Run(k.name, func(t *testing.T) {
			testkv.Each(t, func(t *testing.T, db kv.DB) {
				k.seedV1(t, db)

				var s1 int
				countS1 := func(id int64, old *PetV1) (*PetMid, error) {
					s1++
					return stepV1ToMid(id, old)
				}
				batch := migrate.WithBatchSize(1)
				errStopped := errors.New("stopped")

				// The second step stops after the first record, when the first one is done.
				pets, _ := k.program(steps{
					s1: countS1,
					s2: func(id int64, old *PetMid) (*Pet, error) {
						if id == 2 {
							return nil, errStopped
						}
						return stepMidToPet(id, old)
					},
					o1: []migrate.Option{batch},
					o2: []migrate.Option{batch},
				})
				_, err := pets.Migrate(context.Background(), db)
				require.ErrorIs(t, err, errStopped)
				require.ErrorContains(t, err, "from version 2 to 3")
				require.Equal(t, 3, s1)

				rec, ok := readRecord(t, db, "pets")
				require.True(t, ok)
				require.Equal(t, uint64(2), rec.Version)

				// It goes on from there: the first step is not done again, and no pet goes
				// through the second one twice.
				pets, read := k.program(steps{s1: countS1, o1: []migrate.Option{batch}, o2: []migrate.Option{batch}})
				report, err := pets.Migrate(context.Background(), db)
				require.NoError(t, err)
				require.Equal(t, `migrated `+k.name+` "pets" from version 2 to 3`, report.String())
				require.Equal(t, 3, s1)

				checkChained(t, readPets(t, db, read))
				require.NoError(t, pets.Check(db))
			})
		})
	}
}
