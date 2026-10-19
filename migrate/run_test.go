package migrate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/stretchr/testify/require"
)

// program is the unit of a version of a program: a store of pets under a prefix of its own,
// with parts that depend on the pets. An index is derived: an entry for every pet, which has
// the value of the pet and the fields that the index is on, and the program counts how often
// it is built.
type program struct {
	unit   *migrate.Unit
	data   []byte
	builds map[string]int
}

const petsPrefix = "pets"

func newProgram(version uint64, codec string) *program {
	return newProgramAt(petsPrefix, version, codec)
}

func newProgramAt(prefix string, version uint64, codec string) *program {
	p := &program{
		unit: migrate.NewUnit([]byte(prefix), "store", func() map[string]string {
			return map[string]string{"codec": codec}
		}),
		data:   []byte(prefix + "d"),
		builds: map[string]int{},
	}
	p.unit.SetVersion(version)
	return p
}

// partPrefix returns the prefix of a part of the pets.
func partPrefix(name string) []byte {
	return lex.AppendTerminated([]byte(petsPrefix+"x"), []byte(name))
}

// index adds something that is derived from the pets, and returns its prefix.
func (p *program) index(name, fields string) []byte {
	pre := partPrefix(name)
	p.unit.AddPart(migrate.Part{
		Name:       name,
		Kind:       "index",
		Prefix:     pre,
		Attributes: func() map[string]string { return map[string]string{"fields": fields} },
		Rebuild: func(ctx context.Context, db kv.DB) error {
			p.builds[name]++
			if err := kv.DeleteRange(ctx, db, pre, kv.PrefixEnd(pre)); err != nil {
				return err
			}
			return db.Update(func(tx kv.Tx) error {
				it, err := prefix.New(tx, p.data).Iterator(nil, nil)
				if err != nil {
					return err
				}
				defer it.Close()

				for ; it.Valid(); it.Next() {
					value, err := it.Value()
					if err != nil {
						return err
					}
					if err := prefix.New(tx, pre).Set(it.Key(), append([]byte(fields+":"), value...)); err != nil {
						return err
					}
				}
				return nil
			})
		},
	})
	return pre
}

// part adds something that keeps data that cannot be built again, and returns its prefix.
func (p *program) part(name, kind string, startEmpty bool) []byte {
	pre := partPrefix(name)
	p.unit.AddPart(migrate.Part{Name: name, Kind: kind, Prefix: pre, StartEmpty: startEmpty})
	return pre
}

func set(t *testing.T, db kv.DB, pre []byte, pairs ...string) {
	t.Helper()

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		for i := 0; i < len(pairs); i += 2 {
			if err := prefix.New(tx, pre).Set([]byte(pairs[i]), []byte(pairs[i+1])); err != nil {
				return err
			}
		}
		return nil
	}))
}

// dump returns the keys and the values under a prefix.
func dump(t *testing.T, db kv.DB, pre []byte) []string {
	t.Helper()

	pairs := []string{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := prefix.New(tx, pre).Iterator(nil, nil)
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

// readRecord returns what is recorded of the unit under a prefix, if anything is.
func readRecord(t *testing.T, db kv.DB, pre string) (rec struct {
	Kind      string
	Version   uint64
	Migrating bool
	Cursor    []byte
	Parts     map[string]struct {
		Kind    string
		Prefix  []byte
		Derived bool
		State   string
	}
}, ok bool) {
	t.Helper()

	err := db.View(func(tx kv.Tx) error {
		bz, err := tx.Get(migrate.MetaKey([]byte(pre)))
		if errors.Is(err, kv.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		ok = true
		return json.Unmarshal(bz, &rec)
	})
	require.NoError(t, err)
	return rec, ok
}

func run(t *testing.T, db kv.DB, units ...migrate.Migratable) []string {
	t.Helper()

	report, err := migrate.Run(context.Background(), db, units...)
	require.NoError(t, err)
	return lines(report)
}

func lines(report *migrate.Report) []string {
	if report.String() == "" {
		return []string{}
	}
	return strings.Split(report.String(), "\n")
}

// upper is a migration that makes the values of the pets upper case, a pet in a step.
// It fails before the pet failAt, to be run again, and records the pets that it converts.
func upper(p *program, failAt string, done *[]string) migrate.Migration {
	return func(ctx context.Context, m *migrate.Migrator) error {
		for {
			var key []byte
			err := m.DB().View(func(tx kv.Tx) error {
				start := m.Cursor()
				if start != nil {
					start = append(slices.Clone(start), 0x00)
				}
				it, err := prefix.New(tx, p.data).Iterator(start, nil)
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
				value, err := prefix.New(tx, p.data).Get(key)
				if err != nil {
					return err
				}
				return prefix.New(tx, p.data).Set(key, []byte(strings.ToUpper(string(value))))
			})
			if err != nil {
				return err
			}
			*done = append(*done, string(key))
		}
	}
}

func TestUnitDefinition(t *testing.T) {
	raw := []byte("pets")
	u := migrate.NewUnit(raw, "store", nil)
	raw[0] = 'x'
	require.Equal(t, []byte("pets"), u.Prefix())
	require.Same(t, u, u.Unit())
	var _ migrate.Migratable = u

	require.Equal(t, "store", u.Kind())
	u.SetKind("record")
	require.Equal(t, "record", u.Kind())

	require.Equal(t, uint64(1), u.Version())
	u.SetVersion(3)
	require.Equal(t, uint64(3), u.Version())
	require.Panics(t, func() { u.SetVersion(0) })

	// A unit goes by its prefix as text, unless it is given a name.
	require.Equal(t, "pets", u.Name())
	u.SetName("animals")
	require.Equal(t, "animals", u.Name())
	require.Equal(t, "0x0001ff", migrate.NewUnit([]byte{0x00, 0x01, 0xff}, "store", nil).Name())

	require.Panics(t, func() { migrate.NewUnit(nil, "store", nil) })
	require.Panics(t, func() { migrate.NewUnit([]byte{}, "store", nil) })

	t.Run("Labels", func(t *testing.T) {
		require.Equal(t, "pets", migrate.Label([]byte("pets")))
		require.Equal(t, "a b~", migrate.Label([]byte("a b~")))
		require.Equal(t, "0x6100", migrate.Label([]byte("a\x00")))
		require.Equal(t, "0x7f", migrate.Label([]byte{0x7f}))
		require.Equal(t, "0x1f", migrate.Label([]byte{0x1f}))
		require.Equal(t, "", migrate.Label(nil))
		require.Equal(t, []byte("petsm"), migrate.MetaKey([]byte("pets")))
		require.Equal(t, []byte{0x01, 'm'}, migrate.MetaKey([]byte{0x01}))
	})

	t.Run("Migrations", func(t *testing.T) {
		u := migrate.NewUnit([]byte("pets"), "store", nil)
		noop := func(context.Context, *migrate.Migrator) error { return nil }
		u.AddMigration(1, noop)
		u.AddMigration(2, noop)
		require.Panics(t, func() { u.AddMigration(0, noop) })
		require.Panics(t, func() { u.AddMigration(1, noop) })
	})

	t.Run("Parts", func(t *testing.T) {
		u := migrate.NewUnit([]byte("pets"), "store", nil)
		a := migrate.Part{Name: "a", Kind: "index", Prefix: []byte("petsxa")}
		b := migrate.Part{Name: "b", Kind: "log", Prefix: []byte("petsxb")}
		u.AddPart(a)
		u.AddPart(b)
		parts := u.Parts()
		require.Len(t, parts, 2)
		require.Equal(t, "a", parts[0].Name)
		require.Equal(t, "b", parts[1].Name)

		// The parts that are returned are copies.
		parts[0].Name = "changed"
		require.Equal(t, "a", u.Parts()[0].Name)

		for name, part := range map[string]migrate.Part{
			"EmptyName":   {Prefix: []byte("petsxc")},
			"TakenName":   {Name: "a", Prefix: []byte("petsxc")},
			"NoPrefix":    {Name: "c"},
			"OtherPrefix": {Name: "c", Prefix: []byte("owners")},
			"ShorterThanTheUnit": {
				Name: "c", Prefix: []byte("pet"),
			},
			"TheUnitItself": {Name: "c", Prefix: []byte("pets")},
			"TheRecord":     {Name: "c", Prefix: []byte("petsm")},
		} {
			require.Panics(t, func() { u.AddPart(part) }, name)
		}
		require.Len(t, u.Parts(), 2)

		// Under the record of the unit is fine, as is anything else under the prefix.
		u.AddPart(migrate.Part{Name: "c", Kind: "log", Prefix: []byte("petsmore")})
		require.Len(t, u.Parts(), 3)
	})

	t.Run("Retire", func(t *testing.T) {
		u := migrate.NewUnit([]byte("pets"), "store", nil)
		u.Retire("old")
		u.Retire("old")
		require.Panics(t, func() { u.Retire("") })
	})
}

func TestUnitIsSafeForConcurrentUse(t *testing.T) {
	u := migrate.NewUnit([]byte("pets"), "store", nil)

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			name := fmt.Sprint("part", i)
			u.AddPart(migrate.Part{Name: name, Kind: "log", Prefix: []byte("petsx" + name)})
			u.SetKind("store")
			u.SetName("pets")
			u.Retire("gone" + name)
			u.AddMigration(uint64(i+1), func(context.Context, *migrate.Migrator) error { return nil })
			_ = u.Parts()
			_ = u.Name()
			_ = u.Version()
			_ = u.Kind()
		}()
	}
	wg.Wait()
	require.Len(t, u.Parts(), 20)
}

func TestRunNewUnit(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		p := newProgram(1, "v1")
		p.index("by_name", "name")
		p.part("audit", "log", false)
		require.Equal(t, []string{
			`recorded store "pets"`,
			`recorded log "pets/audit"`,
			`built index "pets/by_name"`,
		}, run(t, db, p.unit))
		require.Equal(t, 1, p.builds["by_name"])

		// What is recorded is under the prefix of the unit, at the key of its record.
		rec, ok := readRecord(t, db, petsPrefix)
		require.True(t, ok)
		require.Equal(t, "store", rec.Kind)
		require.Equal(t, uint64(1), rec.Version)
		require.False(t, rec.Migrating)
		require.Len(t, rec.Parts, 2)
		require.Equal(t, "index", rec.Parts["by_name"].Kind)
		require.True(t, rec.Parts["by_name"].Derived)
		require.Equal(t, "ready", rec.Parts["by_name"].State)
		require.Equal(t, partPrefix("by_name"), rec.Parts["by_name"].Prefix)
		require.False(t, rec.Parts["audit"].Derived)
		require.Equal(t, "ready", rec.Parts["audit"].State)

		// The same program has nothing to do the next time, whichever of the two it calls.
		p = newProgram(1, "v1")
		p.index("by_name", "name")
		p.part("audit", "log", false)
		require.Empty(t, run(t, db, p.unit))
		require.NoError(t, migrate.Check(db, p.unit))
		require.Empty(t, p.builds)
	})
}

func TestCheckNewUnits(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// A database that has nothing of a unit has nothing to build for it either.
		p := newProgram(1, "v1")
		p.index("by_name", "name")
		require.NoError(t, migrate.Check(db, p.unit))
		require.Empty(t, p.builds)
		rec, ok := readRecord(t, db, petsPrefix)
		require.True(t, ok)
		require.True(t, rec.Parts["by_name"].Derived)
		require.Equal(t, "ready", rec.Parts["by_name"].State)

		// A unit that is new next to the others is recorded too.
		p = newProgram(1, "v1")
		p.index("by_name", "name")
		owners := newProgramAt("owners", 4, "v1")
		require.NoError(t, migrate.Check(db, p.unit, owners.unit))
		rec, ok = readRecord(t, db, "owners")
		require.True(t, ok)
		require.Equal(t, uint64(4), rec.Version)
		require.Empty(t, run(t, db, p.unit, owners.unit))
		require.Empty(t, p.builds)
	})
}

func TestRunVersions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		by := v1.index("by_name", "name")
		run(t, db, v1.unit)
		set(t, db, v1.data, "1", "rex", "2", "tom", "3", "kit")
		set(t, db, by, "1", "name:rex", "2", "name:tom", "3", "name:kit")

		// A program of a newer version that does not say how to get there.
		v3 := newProgram(3, "v3")
		v3.index("by_name", "name")
		_, err := migrate.Run(context.Background(), db, v3.unit)
		require.ErrorIs(t, err, migrate.ErrNoMigration)
		require.ErrorContains(t, err, `"pets" from version 1 to 2`)
		require.ErrorIs(t, migrate.Check(db, v3.unit), migrate.ErrNoMigration)

		var first, second []string
		define := func(failAt string) *program {
			v3 := newProgram(3, "v3")
			v3.index("by_name", "name")
			v3.unit.AddMigration(1, upper(v3, failAt, &first))
			v3.unit.AddMigration(2, func(ctx context.Context, m *migrate.Migrator) error {
				require.Equal(t, uint64(2), m.From())
				require.Same(t, v3.unit, m.Unit())
				require.Nil(t, m.Cursor())
				second = append(second, "ran")
				return m.Step(nil, func(tx kv.Tx) error {
					return prefix.New(tx, v3.data).Set([]byte("4"), []byte("NEW"))
				})
			})
			return v3
		}

		// Check does not migrate.
		require.ErrorIs(t, migrate.Check(db, define("").unit), migrate.ErrNeedsMigration)
		require.Empty(t, first)

		// A migration that is interrupted has done its steps, and nothing else is done.
		v3 = define("3")
		_, err = migrate.Run(context.Background(), db, v3.unit)
		require.ErrorContains(t, err, "interrupted")
		require.ErrorContains(t, err, `"pets" from version 1 to 2`)
		require.Equal(t, []string{"1", "2"}, first)
		require.Empty(t, second)
		require.Equal(t, []string{"1=REX", "2=TOM", "3=kit"}, dump(t, db, v3.data))
		require.Empty(t, v3.builds)

		// The record of the unit says how far it has come, and keeps its parts.
		rec, _ := readRecord(t, db, petsPrefix)
		require.True(t, rec.Migrating)
		require.Equal(t, uint64(1), rec.Version)
		require.Equal(t, []byte("2"), rec.Cursor)
		require.Contains(t, rec.Parts, "by_name")

		// A unit that is in the middle of a migration is not for a program to use, not even
		// the one of the version that it comes from.
		require.ErrorIs(t, migrate.Check(db, define("").unit), migrate.ErrNeedsMigration)
		old := newProgram(1, "v1")
		old.index("by_name", "name")
		require.Error(t, migrate.Check(db, old.unit))
		_, err = migrate.Run(context.Background(), db, old.unit)
		require.Error(t, err)

		// It goes on from where it stopped, without doing a step again, and the next
		// migration follows it. The index is built from the new data.
		v3 = define("")
		require.Equal(t, []string{
			`migrated store "pets" from version 1 to 3`,
			`built index "pets/by_name"`,
		}, run(t, db, v3.unit))
		require.Equal(t, []string{"1", "2", "3"}, first)
		require.Equal(t, []string{"ran"}, second)
		require.Equal(t, []string{"1=REX", "2=TOM", "3=KIT", "4=NEW"}, dump(t, db, v3.data))
		require.Equal(t, []string{"1=name:REX", "2=name:TOM", "3=name:KIT", "4=name:NEW"}, dump(t, db, by))
		require.Equal(t, 1, v3.builds["by_name"])

		rec, _ = readRecord(t, db, petsPrefix)
		require.False(t, rec.Migrating)
		require.Nil(t, rec.Cursor)
		require.Equal(t, uint64(3), rec.Version)
		require.Equal(t, "ready", rec.Parts["by_name"].State)

		// The definitions of the new version are the ones that are recorded.
		v3 = define("")
		require.Empty(t, run(t, db, v3.unit))
		require.NoError(t, migrate.Check(db, v3.unit))
		require.Equal(t, []string{"1", "2", "3"}, first)
		require.Empty(t, v3.builds)

		// A program of an older version cannot read what the newer one has written.
		_, err = migrate.Run(context.Background(), db, old.unit)
		require.ErrorIs(t, err, migrate.ErrNewer)
		require.ErrorIs(t, migrate.Check(db, old.unit), migrate.ErrNewer)
	})
}

func TestRunNewUnitOfALaterVersion(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// A unit that the database has not seen is recorded with the version of the program,
		// and has nothing to migrate.
		fresh := newProgram(5, "v5")
		fresh.unit.AddMigration(1, func(context.Context, *migrate.Migrator) error {
			t.Error("a unit that is new is not migrated")
			return nil
		})
		require.Equal(t, []string{`recorded store "pets"`}, run(t, db, fresh.unit))
		rec, _ := readRecord(t, db, petsPrefix)
		require.Equal(t, uint64(5), rec.Version)
	})
}

func TestRunChangedWithoutAVersion(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		run(t, db, newProgram(1, "v1").unit)

		// Another codec for the same version is a mistake.
		changed := newProgram(1, "v2")
		_, err := migrate.Run(context.Background(), db, changed.unit)
		require.ErrorIs(t, err, migrate.ErrChanged)
		require.ErrorContains(t, err, `store "pets"`)
		require.ErrorIs(t, migrate.Check(db, changed.unit), migrate.ErrChanged)

		// Another kind is, too.
		kind := newProgram(1, "v1")
		kind.unit.SetKind("record")
		require.ErrorIs(t, migrate.Check(db, kind.unit), migrate.ErrChanged)

		// With a version and a migration it is what the unit is from then on.
		v2 := newProgram(2, "v2")
		v2.unit.AddMigration(1, func(context.Context, *migrate.Migrator) error { return nil })
		require.Equal(t, []string{`migrated store "pets" from version 1 to 2`}, run(t, db, v2.unit))
		require.NoError(t, migrate.Check(db, newProgram(2, "v2").unit))
		require.ErrorIs(t, migrate.Check(db, newProgram(2, "v1").unit), migrate.ErrChanged)

		// Also the kind: a version bump may relabel the unit.
		v3 := newProgram(3, "v2")
		v3.unit.SetKind("record")
		v3.unit.AddMigration(2, func(context.Context, *migrate.Migrator) error { return nil })
		require.Equal(t, []string{`migrated record "pets" from version 2 to 3`}, run(t, db, v3.unit))
		again := newProgram(3, "v2")
		again.unit.SetKind("record")
		require.NoError(t, migrate.Check(db, again.unit))
	})

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// The same goes for the attributes of a part that is data of its own.
		define := func(attrs string) *program {
			p := newProgram(1, "v1")
			p.unit.AddPart(migrate.Part{
				Name: "audit", Kind: "log", Prefix: partPrefix("audit"),
				Attributes: func() map[string]string { return map[string]string{"format": attrs} },
			})
			return p
		}
		run(t, db, define("json").unit)
		require.NoError(t, migrate.Check(db, define("json").unit))
		_, err := migrate.Run(context.Background(), db, define("text").unit)
		require.ErrorIs(t, err, migrate.ErrChanged)
		require.ErrorContains(t, err, `log "pets/audit"`)
	})
}

func TestRunBuilds(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		run(t, db, v1.unit)
		set(t, db, v1.data, "1", "rex", "2", "tom")

		// An index that is new to a store that is not is built from its records.
		v2 := newProgram(1, "v1")
		by := v2.index("by_name", "name")
		require.ErrorIs(t, migrate.Check(db, v2.unit), migrate.ErrNeedsMigration)
		require.Empty(t, v2.builds)
		require.Equal(t, []string{`built index "pets/by_name"`}, run(t, db, v2.unit))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))
		require.NoError(t, migrate.Check(db, v2.unit))
		require.Equal(t, 1, v2.builds["by_name"])

		// An index whose definition has changed is built again, without a version.
		v3 := newProgram(1, "v1")
		v3.index("by_name", "name,age")
		v3.index("by_age", "age")
		require.ErrorIs(t, migrate.Check(db, v3.unit), migrate.ErrNeedsMigration)
		require.Equal(t, []string{
			`built index "pets/by_name"`,
			`built index "pets/by_age"`,
		}, run(t, db, v3.unit))
		require.Equal(t, []string{"1=name,age:rex", "2=name,age:tom"}, dump(t, db, by))
		require.Equal(t, map[string]int{"by_name": 1, "by_age": 1}, v3.builds)
		require.Empty(t, run(t, db, v3.unit))

		// A build that fails is done again, even by a program that defines the index as
		// it is recorded.
		failing := newProgram(1, "v1")
		failing.index("by_age", "age")
		failing.unit.AddPart(migrate.Part{
			Name:       "by_name",
			Kind:       "index",
			Prefix:     partPrefix("by_name"),
			Attributes: func() map[string]string { return map[string]string{"fields": "name"} },
			Rebuild:    func(context.Context, kv.DB) error { return errors.New("no space") },
		})
		report, err := migrate.Run(context.Background(), db, failing.unit)
		require.ErrorContains(t, err, "no space")
		require.ErrorContains(t, err, `index "pets/by_name"`)
		require.Empty(t, report.Actions)
		rec, _ := readRecord(t, db, petsPrefix)
		require.Equal(t, "building", rec.Parts["by_name"].State)
		require.Equal(t, "ready", rec.Parts["by_age"].State)

		v4 := newProgram(1, "v1")
		v4.index("by_name", "name,age")
		v4.index("by_age", "age")
		require.ErrorIs(t, migrate.Check(db, v4.unit), migrate.ErrNeedsMigration)
		require.Equal(t, []string{`built index "pets/by_name"`}, run(t, db, v4.unit))
		require.Equal(t, []string{"1=name,age:rex", "2=name,age:tom"}, dump(t, db, by))
		rec, _ = readRecord(t, db, petsPrefix)
		require.Equal(t, "ready", rec.Parts["by_name"].State)
	})
}

func TestRunDropsWhatIsDerived(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		by := v1.index("by_name", "name")
		run(t, db, v1.unit)
		set(t, db, v1.data, "1", "rex")
		set(t, db, by, "1", "name:rex")

		// An index that is not defined any more is deleted: the program would not keep it up
		// to date, so it is of no use. Nothing needs to be said of it.
		v2 := func() *program { return newProgram(1, "v1") }
		require.Equal(t, []string{`dropped index "pets/by_name"`}, run(t, db, v2().unit))
		require.Empty(t, dump(t, db, by))
		require.Empty(t, run(t, db, v2().unit))
		require.NoError(t, migrate.Check(db, v2().unit))
		rec, _ := readRecord(t, db, petsPrefix)
		require.Empty(t, rec.Parts)

		// When it is defined again, it is built from the records as they are then.
		set(t, db, v1.data, "2", "tom")
		back := func() *program {
			p := newProgram(1, "v1")
			p.index("by_name", "name")
			return p
		}
		require.ErrorIs(t, migrate.Check(db, back().unit), migrate.ErrNeedsMigration)
		require.Equal(t, []string{`built index "pets/by_name"`}, run(t, db, back().unit))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))

		// A program that only checks leaves it where it is, marked, as it cannot delete it.
		// It is built again when it is defined again, and deleted by Run when it is not.
		require.NoError(t, migrate.Check(db, v2().unit))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))
		rec, _ = readRecord(t, db, petsPrefix)
		require.Equal(t, "stale", rec.Parts["by_name"].State)
		require.ErrorIs(t, migrate.Check(db, back().unit), migrate.ErrNeedsMigration)
		require.NoError(t, migrate.Check(db, v2().unit))
		require.Equal(t, []string{`dropped index "pets/by_name"`}, run(t, db, v2().unit))
		require.Empty(t, dump(t, db, by))
	})
}

func TestRunRetiresWhatCannotBeBuilt(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		by := v1.index("by_name", "name")
		audit := v1.part("audit", "log", false)
		run(t, db, v1.unit)
		set(t, db, v1.data, "1", "rex", "2", "tom")
		set(t, db, by, "1", "name:rex", "2", "name:tom")
		set(t, db, audit, "9", "ann")

		// A part that is not defined any more is not left without a word, which says what
		// deletes it.
		gone := func() *program {
			p := newProgram(1, "v1")
			p.index("by_name", "name")
			return p
		}
		_, err := migrate.Run(context.Background(), db, gone().unit)
		require.ErrorIs(t, err, migrate.ErrUnknown)
		require.ErrorContains(t, err, `log "pets/audit" is in the database and is not defined: retire it to delete it`)
		require.ErrorIs(t, migrate.Check(db, gone().unit), migrate.ErrUnknown)
		require.Equal(t, []string{"9=ann"}, dump(t, db, audit))

		// Retiring it by its name deletes its data and what is recorded of it.
		retiring := func() *program {
			p := gone()
			p.unit.Retire("audit")
			return p
		}
		require.ErrorIs(t, migrate.Check(db, retiring().unit), migrate.ErrNeedsMigration)
		require.Equal(t, []string{"9=ann"}, dump(t, db, audit))
		require.Equal(t, []string{`retired log "pets/audit"`}, run(t, db, retiring().unit))
		require.Empty(t, dump(t, db, audit))
		require.Equal(t, []string{"1=rex", "2=tom"}, dump(t, db, v1.data))
		require.Equal(t, []string{"1=name:rex", "2=name:tom"}, dump(t, db, by))
		rec, _ := readRecord(t, db, petsPrefix)
		require.Contains(t, rec.Parts, "by_name")
		require.NotContains(t, rec.Parts, "audit")

		// What is retired and gone is nothing to do, and the program can forget of it.
		require.Empty(t, run(t, db, retiring().unit))
		require.NoError(t, migrate.Check(db, retiring().unit))
		require.NoError(t, migrate.Check(db, gone().unit))

		// A part that was retired starts again as a new one, which must say what it makes of
		// the data that is there.
		again := func(startEmpty bool) *program {
			p := gone()
			p.part("audit", "log", startEmpty)
			return p
		}
		_, err = migrate.Run(context.Background(), db, again(false).unit)
		require.ErrorIs(t, err, migrate.ErrCannotBuild)
		require.Equal(t, []string{`recorded log "pets/audit"`}, run(t, db, again(true).unit))
		require.NoError(t, migrate.Check(db, again(false).unit))
	})
}

func TestRetiredAndDefined(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		audit := v1.part("audit", "log", false)
		run(t, db, v1.unit)
		set(t, db, audit, "9", "ann")

		// Something cannot be retired and defined, and nothing is done then, not even for
		// the units that are fine.
		both := newProgram(1, "v1")
		both.part("audit", "log", false)
		both.unit.Retire("audit")
		other := newProgramAt("owners", 1, "v1")

		_, err := migrate.Run(context.Background(), db, other.unit, both.unit)
		require.ErrorContains(t, err, `"pets/audit" is retired and defined`)
		require.ErrorContains(t, migrate.Check(db, other.unit, both.unit), "retired and defined")
		require.Equal(t, []string{"9=ann"}, dump(t, db, audit))
		_, ok := readRecord(t, db, "owners")
		require.False(t, ok)
	})
}

func TestRunPartsChangeTheirKind(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		derived := func() *program {
			p := newProgram(1, "v1")
			p.index("by_name", "name")
			return p
		}
		plain := func() *program {
			p := newProgram(1, "v1")
			p.unit.AddPart(migrate.Part{
				Name: "by_name", Kind: "index", Prefix: partPrefix("by_name"),
				Attributes: func() map[string]string { return map[string]string{"fields": "name"} },
			})
			return p
		}
		run(t, db, derived().unit)
		rec, _ := readRecord(t, db, petsPrefix)
		require.True(t, rec.Parts["by_name"].Derived)

		// Data that was derived and is data of its own now is recorded as that, silently.
		require.Empty(t, run(t, db, plain().unit))
		rec, _ = readRecord(t, db, petsPrefix)
		require.False(t, rec.Parts["by_name"].Derived)

		// And the other way around, with nothing to build as it is what it was.
		next := derived()
		require.Empty(t, run(t, db, next.unit))
		require.Empty(t, next.builds)
		rec, _ = readRecord(t, db, petsPrefix)
		require.True(t, rec.Parts["by_name"].Derived)
	})
}

func TestRunNewPartsOfAUnitThatHasData(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// With a unit that is new, anything can be added to it.
		v1 := newProgram(1, "v1")
		v1.part("audit", "log", false)
		run(t, db, v1.unit)

		define := func(startEmpty bool) *program {
			p := newProgram(1, "v1")
			p.part("audit", "log", false)
			p.part("counts", "counter", startEmpty)
			return p
		}

		// What is added to a unit that is there must say what it makes of its data.
		_, err := migrate.Run(context.Background(), db, define(false).unit)
		require.ErrorIs(t, err, migrate.ErrCannotBuild)
		require.ErrorContains(t, err, `counter "pets/counts"`)
		require.ErrorIs(t, migrate.Check(db, define(false).unit), migrate.ErrCannotBuild)
		rec, _ := readRecord(t, db, petsPrefix)
		require.NotContains(t, rec.Parts, "counts")

		require.Equal(t, []string{`recorded counter "pets/counts"`}, run(t, db, define(true).unit))
		require.NoError(t, migrate.Check(db, define(false).unit))

		// The same holds when the unit is migrated in the same run, and nothing is migrated then.
		var ran bool
		next := func(startEmpty bool) *program {
			q := newProgram(2, "v2")
			q.unit.AddMigration(1, func(context.Context, *migrate.Migrator) error { ran = true; return nil })
			q.part("audit", "log", false)
			q.part("counts", "counter", false)
			q.part("more", "counter", startEmpty)
			return q
		}
		_, err = migrate.Run(context.Background(), db, next(false).unit)
		require.ErrorIs(t, err, migrate.ErrCannotBuild)
		require.ErrorContains(t, err, `counter "pets/more"`)
		require.False(t, ran)
		require.Equal(t, []string{`migrated store "pets" from version 1 to 2`}, run(t, db, next(true).unit))
		require.True(t, ran)
		require.NoError(t, migrate.Check(db, next(false).unit))
	})
}

// TestRunInterruptedBuilds checks that the indexes of a unit that was migrated are all built
// from its new data, also when the program stops between the migration and the builds.
func TestRunInterruptedBuilds(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		v1 := newProgram(1, "v1")
		byAge, byName := v1.index("by_age", "age"), v1.index("by_name", "name")
		run(t, db, v1.unit)
		set(t, db, v1.data, "1", "rex")
		set(t, db, byAge, "1", "age:rex")
		set(t, db, byName, "1", "name:rex")

		var done []string
		define := func(fail bool) *program {
			v2 := newProgram(2, "v2")
			v2.unit.AddMigration(1, upper(v2, "", &done))
			if fail {
				v2.unit.AddPart(migrate.Part{
					Name:       "by_age",
					Kind:       "index",
					Prefix:     partPrefix("by_age"),
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
		report, err := migrate.Run(context.Background(), db, define(true).unit)
		require.ErrorContains(t, err, "stopped")
		require.Equal(t, []string{`migrated store "pets" from version 1 to 2`}, lines(report))
		require.Equal(t, []string{"1=REX"}, dump(t, db, v1.data))
		require.Equal(t, []string{"1=name:rex"}, dump(t, db, byName))
		rec, _ := readRecord(t, db, petsPrefix)
		require.Equal(t, uint64(2), rec.Version)
		require.Equal(t, "building", rec.Parts["by_age"].State)
		require.Equal(t, "stale", rec.Parts["by_name"].State)

		// Neither index is what the new data would give, and both are built the next time.
		require.ErrorIs(t, migrate.Check(db, define(false).unit), migrate.ErrNeedsMigration)
		v2 := define(false)
		require.Equal(t, []string{
			`built index "pets/by_age"`,
			`built index "pets/by_name"`,
		}, run(t, db, v2.unit))
		require.Equal(t, []string{"1=age:REX"}, dump(t, db, byAge))
		require.Equal(t, []string{"1=name:REX"}, dump(t, db, byName))
		require.Equal(t, []string{"1"}, done)
	})
}

// TestCheckThatFailsRecordsNothing checks that a check that finds work for Run, or a
// problem, leaves the database as it was.
func TestCheckThatFailsRecordsNothing(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		run(t, db, newProgram(1, "v1").unit)

		// A unit that is new, next to one that needs Run and next to one that is wrong.
		for _, codec := range []string{"v1", "changed"} {
			p := newProgram(1, codec)
			p.index("by_name", "name")
			owners := newProgramAt("owners", 1, "v1")
			require.Error(t, migrate.Check(db, p.unit, owners.unit), codec)

			_, ok := readRecord(t, db, "owners")
			require.False(t, ok, codec)
			rec, _ := readRecord(t, db, petsPrefix)
			require.Empty(t, rec.Parts, codec)
		}

		// A stale mark of something that is not defined is not written either.
		v1 := newProgram(1, "v1")
		v1.index("by_name", "name")
		run(t, db, v1.unit)
		withNew := newProgram(1, "v1")
		withNew.unit.AddPart(migrate.Part{Name: "x", Kind: "log", Prefix: partPrefix("x")})
		require.ErrorIs(t, migrate.Check(db, withNew.unit), migrate.ErrCannotBuild)
		rec, _ := readRecord(t, db, petsPrefix)
		require.Equal(t, "ready", rec.Parts["by_name"].State)

		// The unit was not recorded, so a program without it has nothing that it does not know.
		_, ok := readRecord(t, db, "owners")
		require.False(t, ok)
		require.NoError(t, migrate.Check(db, newProgram(1, "v1").unit, newProgramAt("owners", 1, "v1").unit))
	})
}

func TestUnitsMustNotOverlap(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		pets := newProgramAt("pets", 1, "v1")
		petsToo := newProgramAt("pets2", 1, "v1")
		same := newProgramAt("pets", 1, "v1")
		owners := newProgramAt("owners", 1, "v1")

		// A prefix that the other one begins with, both ways round, and the same prefix.
		for name, units := range map[string][]migrate.Migratable{
			"Beginning":    {pets.unit, petsToo.unit},
			"Reversed":     {petsToo.unit, owners.unit, pets.unit},
			"Equal":        {pets.unit, same.unit},
			"WithBeginner": {owners.unit, petsToo.unit, pets.unit},
		} {
			_, err := migrate.Run(context.Background(), db, units...)
			require.Error(t, err, name)
			require.ErrorContains(t, err, "overlaps", name)
			require.Error(t, migrate.Check(db, units...), name)
		}

		// The error names both.
		_, err := migrate.Run(context.Background(), db, pets.unit, petsToo.unit)
		require.ErrorContains(t, err, `"pets2"`)
		require.ErrorContains(t, err, `"pets"`)
		_, ok := readRecord(t, db, "pets")
		require.False(t, ok)

		// Units that do not overlap are fine, and so is a unit that is given more than once.
		require.Equal(t, []string{`recorded store "pets"`, `recorded store "owners"`},
			run(t, db, pets.unit, owners.unit, pets.unit, owners.unit))
		require.NoError(t, migrate.Check(db, pets.unit, owners.unit))

		// A unit that is nil is not a unit.
		_, err = migrate.Run(context.Background(), db, nil)
		require.Error(t, err)
	})
}

func TestRunSeveralUnits(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		pets := newProgram(1, "v1")
		owners := newProgramAt("owners", 1, "v1")
		run(t, db, pets.unit, owners.unit)
		set(t, db, pets.data, "1", "rex")
		set(t, db, owners.data, "1", "ann")

		var done []string
		migratePets := func(codec string) *program {
			p := newProgram(2, codec)
			p.unit.AddMigration(1, upper(p, "", &done))
			return p
		}
		migrateOwners := func(fail bool) *program {
			p := newProgramAt("owners", 2, "v2")
			p.unit.AddMigration(1, func(ctx context.Context, m *migrate.Migrator) error {
				if fail {
					return errors.New("no way")
				}
				return m.Step(nil, func(tx kv.Tx) error {
					return prefix.New(tx, p.data).Set([]byte("1"), []byte("ANN"))
				})
			})
			return p
		}

		// Units are compared before any is changed: one that is wrong stops them all.
		wrong := newProgramAt("owners", 1, "changed")
		_, err := migrate.Run(context.Background(), db, migratePets("v2").unit, wrong.unit)
		require.ErrorIs(t, err, migrate.ErrChanged)
		require.Empty(t, done)
		require.Equal(t, []string{"1=rex"}, dump(t, db, pets.data))

		// Check reports what every unit needs.
		err = migrate.Check(db, migratePets("v2").unit, migrateOwners(false).unit)
		require.ErrorIs(t, err, migrate.ErrNeedsMigration)
		require.ErrorContains(t, err, `"pets"`)
		require.ErrorContains(t, err, `"owners"`)

		// They are brought up to date one after the other, in the order that they are given,
		// and when one fails the report has what the ones before it did.
		report, err := migrate.Run(context.Background(), db, migratePets("v2").unit, migrateOwners(true).unit)
		require.ErrorContains(t, err, "no way")
		require.ErrorContains(t, err, `"owners" from version 1 to 2`)
		require.Equal(t, []string{`migrated store "pets" from version 1 to 2`}, lines(report))
		require.Equal(t, []string{"1=REX"}, dump(t, db, pets.data))
		require.Equal(t, []string{"1=ann"}, dump(t, db, owners.data))

		// And what is done is not done again.
		report, err = migrate.Run(context.Background(), db, migratePets("v2").unit, migrateOwners(false).unit)
		require.NoError(t, err)
		require.Equal(t, []string{`migrated store "owners" from version 1 to 2`}, lines(report))
		require.Equal(t, []string{"1"}, done)
		require.Equal(t, []string{"1=ANN"}, dump(t, db, owners.data))
	})
}

func TestRunStopsWhenTheContextIsDone(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		run(t, db, newProgram(1, "v1").unit)

		var done []string
		p := newProgram(2, "v2")
		p.unit.AddMigration(1, upper(p, "", &done))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := migrate.Run(ctx, db, p.unit)
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, done)

		rec, _ := readRecord(t, db, petsPrefix)
		require.Equal(t, uint64(1), rec.Version)
		require.Equal(t, []string{`migrated store "pets" from version 1 to 2`}, run(t, db, p.unit))
	})
}
