package schema_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/stretchr/testify/require"
)

// program is the schema of a version of a program: a store of pets with an index, which
// counts how often it is built.
type program struct {
	*schema.Schema
	pets, data, exts *schema.Namespace
	builds           map[string]int
}

func newProgram(version uint64, codec string) *program {
	p := &program{Schema: schema.New(), builds: map[string]int{}}
	p.pets = p.Namespace("pets", schema.Version(version))
	p.pets.Define(schema.Definition{Kind: "store"})
	p.data = p.pets.Sub("data")
	p.data.Define(schema.Definition{
		Kind:       "data",
		Keys:       true,
		Attributes: func() map[string]string { return map[string]string{"codec": codec} },
	})
	p.exts = p.pets.Sub("ext", schema.Dependent())
	return p
}

// index defines something that is derived from the pets: an entry for every pet, which has
// the value of the pet and the fields that the index is on.
func (p *program) index(name, fields string) *schema.Namespace {
	ns := p.exts.Sub(name)
	ns.Define(schema.Definition{
		Kind:       "index",
		Keys:       true,
		Attributes: func() map[string]string { return map[string]string{"fields": fields} },
		Rebuild: func(ctx context.Context, db kv.DB) error {
			p.builds[name]++
			r := ns.Range()
			if err := kv.DeleteRange(ctx, db, r.Start, r.End); err != nil {
				return err
			}
			return db.Update(func(tx kv.Tx) error {
				it, err := p.data.Open(tx).Iterator(nil, nil)
				if err != nil {
					return err
				}
				defer it.Close()

				for ; it.Valid(); it.Next() {
					value, err := it.Value()
					if err != nil {
						return err
					}
					if err := ns.Open(tx).Set(it.Key(), append([]byte(fields+":"), value...)); err != nil {
						return err
					}
				}
				return nil
			})
		},
	})
	return ns
}

func set(t *testing.T, db kv.DB, ns *schema.Namespace, pairs ...string) {
	t.Helper()

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for i := 0; i < len(pairs); i += 2 {
			if err := ns.Open(tx).Set([]byte(pairs[i]), []byte(pairs[i+1])); err != nil {
				return err
			}
		}
		return nil
	}))
}

// dump returns the keys and the values of a namespace.
func dump(t *testing.T, db kv.DB, ns *schema.Namespace) []string {
	t.Helper()

	pairs := []string{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := ns.Open(tx).Iterator(nil, nil)
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			if err != nil {
				return err
			}
			pairs = append(pairs, fmt.Sprintf("%s=%s", it.Key(), value))
		}
		return nil
	}))
	return pairs
}

func migrate(t *testing.T, db kv.DB, sch *schema.Schema) []string {
	t.Helper()

	report, err := sch.Migrate(context.Background(), db)
	require.NoError(t, err)
	if report.String() == "" {
		return []string{}
	}
	return strings.Split(report.String(), "\n")
}

// upper is a migration that makes the values of the pets upper case, a pet in a step.
// It fails before the pet failAt, to be run again, and records the pets that it converts.
func upper(p *program, failAt string, done *[]string) schema.Migration {
	return func(ctx context.Context, m *schema.Migrator) error {
		for {
			var key []byte
			err := m.DB().View(func(tx kv.Tx) error {
				start := m.Cursor()
				if start != nil {
					start = append(slices.Clone(start), 0x00)
				}
				it, err := p.data.Open(tx).Iterator(start, nil)
				if err != nil {
					return err
				}
				defer it.Close()

				if it.Valid() {
					key = slices.Clone(it.Key())
				}
				return nil
			})
			if err != nil || key == nil {
				return err
			}
			if string(key) == failAt {
				return errors.New("interrupted")
			}

			err = m.Step(key, func(tx kv.Tx) error {
				value, err := p.data.Open(tx).Get(key)
				if err != nil {
					return err
				}
				return p.data.Open(tx).Set(key, []byte(strings.ToUpper(string(value))))
			})
			if err != nil {
				return err
			}
			*done = append(*done, string(key))
		}
	}
}

func TestVersions(t *testing.T) {
	sch := schema.New()
	pets := sch.Namespace("pets", schema.Version(3))
	owners := sch.Namespace("owners")
	require.NoError(t, sch.Validate())

	require.Equal(t, uint64(3), pets.Version())
	require.Equal(t, uint64(1), owners.Version())
	// What is under a namespace at the root is of its version, and of its unit.
	require.Equal(t, uint64(3), pets.Sub("ext").Sub("by_name").Version())
	require.Same(t, pets, pets.Sub("data").Unit())
	require.Same(t, pets, pets.Unit())

	for name, define := range map[string]func(sch *schema.Schema){
		"VersionBelowTheRoot": func(sch *schema.Schema) {
			sch.Namespace("pets").Sub("data", schema.Version(2))
		},
		"TwoMigrationsFromAVersion": func(sch *schema.Schema) {
			ns := sch.Namespace("pets", schema.Version(2))
			ns.AddMigration(1, func(context.Context, *schema.Migrator) error { return nil })
			ns.Sub("data").AddMigration(1, func(context.Context, *schema.Migrator) error { return nil })
		},
		"MigrationFromNoVersion": func(sch *schema.Schema) {
			sch.Namespace("pets").AddMigration(0, func(context.Context, *schema.Migrator) error { return nil })
		},
	} {
		sch := schema.New()
		define(sch)
		require.Error(t, sch.Validate(), name)
	}
}

func TestMigrateRecords(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		p := newProgram(1, "v1")
		p.index("by_name", "name")
		require.Equal(t, []string{
			`recorded schema "_schema"`,
			`recorded store "pets"`,
			`recorded data "pets/data"`,
			`built index "pets/ext/by_name"`,
		}, migrate(t, db, p.Schema))
		require.Equal(t, 1, p.builds["by_name"])

		// The same program has nothing to do the next time, whichever of the two it calls.
		p = newProgram(1, "v1")
		p.index("by_name", "name")
		require.Empty(t, migrate(t, db, p.Schema))
		require.NoError(t, p.Check(db))
		require.Empty(t, p.builds)
	})
}

func TestCheckRecordsNewUnits(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// A database that has nothing of a unit has nothing to build for it either.
		p := newProgram(1, "v1")
		p.index("by_name", "name")
		require.NoError(t, p.Check(db))
		require.Empty(t, p.builds)

		// A unit that is new next to the others is recorded too.
		p = newProgram(1, "v1")
		p.index("by_name", "name")
		p.Namespace("owners", schema.Version(4)).Define(schema.Definition{Kind: "store", Keys: true})
		require.NoError(t, p.Check(db))
		require.Empty(t, migrate(t, db, p.Schema))
	})
}

func TestMigrateVersions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		by := v1.index("by_name", "name")
		migrate(t, db, v1.Schema)
		set(t, db, v1.data, "1", "rex", "2", "tom", "3", "kit")
		set(t, db, by, "1", "name:rex", "2", "name:tom", "3", "name:kit")

		// A program of a newer version that does not say how to get there.
		v3 := newProgram(3, "v3")
		v3.index("by_name", "name")
		_, err := v3.Migrate(context.Background(), db)
		require.ErrorIs(t, err, schema.ErrNoMigration)
		require.ErrorContains(t, err, `"pets" from version 1 to 2`)
		require.ErrorIs(t, v3.Check(db), schema.ErrNoMigration)

		var first, second []string
		define := func(failAt string) *program {
			v3 := newProgram(3, "v3")
			v3.index("by_name", "name")
			v3.pets.AddMigration(1, upper(v3, failAt, &first))
			v3.pets.AddMigration(2, func(ctx context.Context, m *schema.Migrator) error {
				require.Equal(t, uint64(2), m.From())
				require.Same(t, v3.pets, m.Namespace())
				require.Nil(t, m.Cursor())
				second = append(second, "ran")
				return m.Step(nil, func(tx kv.Tx) error {
					return v3.data.Open(tx).Set([]byte("4"), []byte("NEW"))
				})
			})
			return v3
		}

		// Check does not migrate.
		require.ErrorIs(t, define("").Check(db), schema.ErrNeedsMigration)
		require.Empty(t, first)

		// A migration that is interrupted has done its steps, and nothing else is done.
		v3 = define("3")
		_, err = v3.Migrate(context.Background(), db)
		require.ErrorContains(t, err, "interrupted")
		require.ErrorContains(t, err, `"pets" from version 1 to 2`)
		require.Equal(t, []string{"1", "2"}, first)
		require.Empty(t, second)
		require.Equal(t, []string{"1=REX", "2=TOM", "3=kit"}, dump(t, db, v3.data))
		require.Empty(t, v3.builds)

		// A unit that is in the middle of a migration is not for a program to use, not even
		// the one of the version that it comes from.
		require.ErrorIs(t, define("").Check(db), schema.ErrNeedsMigration)
		old := newProgram(1, "v1")
		old.index("by_name", "name")
		require.Error(t, old.Check(db))
		_, err = old.Migrate(context.Background(), db)
		require.Error(t, err)

		// It goes on from where it stopped, without doing a step again, and the next
		// migration follows it. The index is built from the new data.
		v3 = define("")
		require.Equal(t, []string{
			`migrated unit "pets" from version 1 to 3`,
			`built index "pets/ext/by_name"`,
		}, migrate(t, db, v3.Schema))
		require.Equal(t, []string{"1", "2", "3"}, first)
		require.Equal(t, []string{"ran"}, second)
		require.Equal(t, []string{"1=REX", "2=TOM", "3=KIT", "4=NEW"}, dump(t, db, v3.data))
		require.Equal(t, []string{"1=name:REX", "2=name:TOM", "3=name:KIT", "4=name:NEW"}, dump(t, db, by))
		require.Equal(t, 1, v3.builds["by_name"])

		// The definitions of the new version are the ones that are recorded.
		v3 = define("")
		require.Empty(t, migrate(t, db, v3.Schema))
		require.NoError(t, v3.Check(db))
		require.Equal(t, []string{"1", "2", "3"}, first)
		require.Empty(t, v3.builds)

		// A program of an older version cannot read what the newer one has written.
		_, err = old.Migrate(context.Background(), db)
		require.ErrorIs(t, err, schema.ErrNewer)
		require.ErrorIs(t, old.Check(db), schema.ErrNewer)
	})
}

func TestMigrateChangedWithoutAVersion(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		migrate(t, db, newProgram(1, "v1").Schema)

		// Another codec for the same version is a mistake.
		changed := newProgram(1, "v2")
		_, err := changed.Migrate(context.Background(), db)
		require.ErrorIs(t, err, schema.ErrChanged)
		require.ErrorContains(t, err, `data "pets/data"`)
		require.ErrorIs(t, changed.Check(db), schema.ErrChanged)

		// With a version and a migration it is what the unit is from then on.
		v2 := newProgram(2, "v2")
		v2.pets.AddMigration(1, func(context.Context, *schema.Migrator) error { return nil })
		require.Equal(t, []string{`migrated unit "pets" from version 1 to 2`}, migrate(t, db, v2.Schema))
		require.NoError(t, newProgram(2, "v2").Check(db))
		require.ErrorIs(t, newProgram(2, "v1").Check(db), schema.ErrChanged)
	})
}

func TestMigrateBuilds(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		migrate(t, db, v1.Schema)
		set(t, db, v1.data, "1", "rex", "2", "tom")

		// An index that is new to a store that is not is built from its records.
		v2 := newProgram(1, "v1")
		by := v2.index("by_name", "name")
		require.ErrorIs(t, v2.Check(db), schema.ErrNeedsMigration)
		require.Empty(t, v2.builds)
		require.Equal(t, []string{`built index "pets/ext/by_name"`}, migrate(t, db, v2.Schema))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))
		require.NoError(t, v2.Check(db))
		require.Equal(t, 1, v2.builds["by_name"])

		// An index whose definition has changed is built again, without a version.
		v3 := newProgram(1, "v1")
		v3.index("by_name", "name,age")
		v3.index("by_age", "age")
		require.ErrorIs(t, v3.Check(db), schema.ErrNeedsMigration)
		require.Equal(t, []string{
			`built index "pets/ext/by_name"`,
			`built index "pets/ext/by_age"`,
		}, migrate(t, db, v3.Schema))
		require.Equal(t, []string{"1=name,age:rex", "2=name,age:tom"}, dump(t, db, by))
		require.Equal(t, map[string]int{"by_name": 1, "by_age": 1}, v3.builds)
		require.Empty(t, migrate(t, db, v3.Schema))

		// A build that fails is done again, even by a program that defines the index as
		// it is recorded.
		failing := newProgram(1, "v1")
		failing.index("by_age", "age")
		failing.exts.Sub("by_name").Define(schema.Definition{
			Kind:       "index",
			Keys:       true,
			Attributes: func() map[string]string { return map[string]string{"fields": "name"} },
			Rebuild:    func(context.Context, kv.DB) error { return errors.New("no space") },
		})
		_, err := failing.Migrate(context.Background(), db)
		require.ErrorContains(t, err, "no space")
		require.ErrorContains(t, err, `index "pets/ext/by_name"`)

		v4 := newProgram(1, "v1")
		v4.index("by_name", "name")
		v4.index("by_age", "age")
		require.ErrorIs(t, v4.Check(db), schema.ErrNeedsMigration)
		require.Equal(t, []string{`built index "pets/ext/by_name"`}, migrate(t, db, v4.Schema))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))
	})
}

func TestMigrateDropped(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		by := v1.index("by_name", "name")
		owners := v1.Namespace("owners")
		owners.Define(schema.Definition{Kind: "store", Keys: true})
		migrate(t, db, v1.Schema)
		set(t, db, v1.data, "1", "rex")
		set(t, db, by, "1", "name:rex")
		set(t, db, owners, "9", "ann")

		// An index that is not defined any more is deleted: the program would not keep it up
		// to date, so it is of no use. Nothing needs to be said of it.
		v2 := func() *program {
			p := newProgram(1, "v1")
			p.Namespace("owners").Define(schema.Definition{Kind: "store", Keys: true})
			return p
		}
		require.Equal(t, []string{`dropped index "pets/ext/by_name"`}, migrate(t, db, v2().Schema))
		require.Empty(t, dump(t, db, by))
		require.Empty(t, migrate(t, db, v2().Schema))
		require.NoError(t, v2().Check(db))

		// When it is defined again, it is built from the records as they are then.
		set(t, db, v1.data, "2", "tom")
		back := func() *program {
			p := newProgram(1, "v1")
			p.index("by_name", "name")
			p.Namespace("owners").Define(schema.Definition{Kind: "store", Keys: true})
			return p
		}
		require.ErrorIs(t, back().Check(db), schema.ErrNeedsMigration)
		require.Equal(t, []string{`built index "pets/ext/by_name"`}, migrate(t, db, back().Schema))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))

		// A program that only checks leaves it where it is, marked, as it cannot delete it.
		// It is built again when it is defined again, and deleted by Migrate when it is not.
		require.NoError(t, v2().Check(db))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))
		require.ErrorIs(t, back().Check(db), schema.ErrNeedsMigration)
		require.NoError(t, v2().Check(db))
		require.Equal(t, []string{`dropped index "pets/ext/by_name"`}, migrate(t, db, v2().Schema))
		require.Empty(t, dump(t, db, by))
		migrate(t, db, back().Schema)

		// A store that is not defined any more is not left without a word, which says what
		// deletes it.
		gone := newProgram(1, "v1")
		gone.index("by_name", "name")
		_, err := gone.Migrate(context.Background(), db)
		require.ErrorIs(t, err, schema.ErrUnknown)
		require.ErrorContains(t, err, `store "owners" is in the database: Retire("owners") of the schema deletes it`)
		require.ErrorIs(t, gone.Check(db), schema.ErrUnknown)
		require.Equal(t, []string{"9=ann"}, dump(t, db, owners))

		// Retiring it by its name deletes its data and what is recorded of it.
		retiring := func() *program {
			p := newProgram(1, "v1")
			p.index("by_name", "name")
			p.Retire("owners")
			return p
		}
		require.ErrorIs(t, retiring().Check(db), schema.ErrNeedsMigration)
		require.Equal(t, []string{"9=ann"}, dump(t, db, owners))
		require.Equal(t, []string{`retired store "owners"`}, migrate(t, db, retiring().Schema))
		require.Empty(t, dump(t, db, owners))
		require.Equal(t, []string{"1=rex", "2=tom"}, dump(t, db, v1.data))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))

		// What is retired and gone is nothing to do, and the program can forget of it.
		require.Empty(t, migrate(t, db, retiring().Schema))
		require.NoError(t, retiring().Check(db))
		forgot := newProgram(1, "v1")
		forgot.index("by_name", "name")
		require.NoError(t, forgot.Check(db))

		// A unit that was retired starts again as a new one, of any version.
		again := newProgram(1, "v1")
		again.index("by_name", "name")
		again.Namespace("owners", schema.Version(7)).Define(schema.Definition{Kind: "store", Keys: true})
		require.Equal(t, []string{`recorded store "owners"`}, migrate(t, db, again.Schema))

		// Something cannot be retired and defined, and a name is not a path.
		for name, define := range map[string]func(p *program){
			"Unit":      func(p *program) { p.Retire("pets") },
			"Namespace": func(p *program) { p.pets.Retire("ext") },
			"Path":      func(p *program) { p.Retire("pets/ext/by_old") },
			"Empty":     func(p *program) { p.Retire("") },
		} {
			p := newProgram(1, "v1")
			p.index("by_name", "name")
			define(p)
			require.Error(t, p.Validate(), name)
			_, err = p.Migrate(context.Background(), db)
			require.Error(t, err, name)
			require.Error(t, p.Check(db), name)
		}
	})
}

func TestMigrateDependents(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// With a store that is new, anything can be added to it.
		v1 := newProgram(1, "v1")
		v1.exts.Sub("audit").Define(schema.Definition{Kind: "log", Keys: true})
		migrate(t, db, v1.Schema)

		define := func(startEmpty bool) *program {
			p := newProgram(1, "v1")
			p.exts.Sub("audit").Define(schema.Definition{Kind: "log", Keys: true})
			p.exts.Sub("counts").Define(schema.Definition{Kind: "counter", Keys: true, StartEmpty: startEmpty})
			return p
		}

		// What is added to a store that is there must say what it makes of its records.
		_, err := define(false).Migrate(context.Background(), db)
		require.ErrorIs(t, err, schema.ErrCannotBuild)
		require.ErrorContains(t, err, `counter "pets/ext/counts"`)
		require.ErrorIs(t, define(false).Check(db), schema.ErrCannotBuild)

		require.Equal(t, []string{`recorded counter "pets/ext/counts"`}, migrate(t, db, define(true).Schema))
		require.NoError(t, define(false).Check(db))

		// The same holds when the store is migrated in the same run.
		next := func(startEmpty bool) *program {
			q := newProgram(2, "v2")
			q.pets.AddMigration(1, func(context.Context, *schema.Migrator) error { return nil })
			q.exts.Sub("audit").Define(schema.Definition{Kind: "log", Keys: true})
			q.exts.Sub("counts").Define(schema.Definition{Kind: "counter", Keys: true})
			q.exts.Sub("more").Define(schema.Definition{Kind: "counter", Keys: true, StartEmpty: startEmpty})
			return q
		}
		_, err = next(false).Migrate(context.Background(), db)
		require.ErrorIs(t, err, schema.ErrCannotBuild)
		require.ErrorContains(t, err, `counter "pets/ext/more"`)
		require.Equal(t, []string{`migrated unit "pets" from version 1 to 2`}, migrate(t, db, next(true).Schema))
		require.NoError(t, next(false).Check(db))

		// What is not in a dependent namespace has no data to know of, and is simply new.
		wider := next(false)
		wider.pets.Sub("notes").Define(schema.Definition{Kind: "store", Keys: true})
		require.Equal(t, []string{`recorded store "pets/notes"`}, migrate(t, db, wider.Schema))
	})
}

// TestMigrateInterruptedBuilds checks that the indexes of a store that was migrated are all
// built from its new data, also when the program stops between the migration and the builds.
func TestMigrateInterruptedBuilds(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		byAge, byName := v1.index("by_age", "age"), v1.index("by_name", "name")
		migrate(t, db, v1.Schema)
		set(t, db, v1.data, "1", "rex")
		set(t, db, byAge, "1", "age:rex")
		set(t, db, byName, "1", "name:rex")

		var done []string
		define := func(fail bool) *program {
			v2 := newProgram(2, "v2")
			v2.pets.AddMigration(1, upper(v2, "", &done))
			if fail {
				v2.exts.Sub("by_age").Define(schema.Definition{
					Kind:       "index",
					Keys:       true,
					Attributes: func() map[string]string { return map[string]string{"fields": "age"} },
					Rebuild:    func(context.Context, kv.DB) error { return errors.New("stopped") },
				})
			} else {
				v2.index("by_age", "age")
			}
			v2.index("by_name", "name")
			return v2
		}

		// The data is migrated, and the first build stops everything.
		_, err := define(true).Migrate(context.Background(), db)
		require.ErrorContains(t, err, "stopped")
		require.Equal(t, []string{"1=REX"}, dump(t, db, v1.data))
		require.Equal(t, []string{"1=name:rex"}, dump(t, db, byName))

		// Neither index is what the new data would give, and both are built the next time.
		require.ErrorIs(t, define(false).Check(db), schema.ErrNeedsMigration)
		v2 := define(false)
		require.Equal(t, []string{
			`built index "pets/ext/by_age"`,
			`built index "pets/ext/by_name"`,
		}, migrate(t, db, v2.Schema))
		require.Equal(t, []string{"1=age:REX"}, dump(t, db, byAge))
		require.Equal(t, []string{"1=name:REX"}, dump(t, db, byName))
		require.Equal(t, []string{"1"}, done)
	})
}

// TestCheckThatFailsRecordsNothing checks that a check that finds work for Migrate, or a
// problem, leaves the database as it was.
func TestCheckThatFailsRecordsNothing(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		migrate(t, db, newProgram(1, "v1").Schema)

		// A unit that is new, next to one that needs Migrate and next to one that is wrong.
		for _, codec := range []string{"v1", "changed"} {
			p := newProgram(1, codec)
			p.index("by_name", "name")
			p.Namespace("owners").Define(schema.Definition{Kind: "store", Keys: true})
			require.Error(t, p.Check(db))
		}

		// The unit was not recorded, so a program without it has nothing that it does not know.
		require.NoError(t, newProgram(1, "v1").Check(db))
		require.Empty(t, migrate(t, db, newProgram(1, "v1").Schema))
	})
}
