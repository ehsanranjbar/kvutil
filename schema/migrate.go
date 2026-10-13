package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil/kv"
)

var (
	// ErrChanged is returned for a definition that is not the one that the data was written
	// with, while its version is: something was changed without a new version.
	ErrChanged = errors.New("schema: definition has changed")
	// ErrNewer is returned for data of a version that is newer than the one of the program.
	ErrNewer = errors.New("schema: the database is newer than the program")
	// ErrNoMigration is returned for data of an older version that the program has no
	// migration from.
	ErrNoMigration = errors.New("schema: no migration")
	// ErrNeedsMigration is returned by Check for a database that Migrate has work to do on.
	ErrNeedsMigration = errors.New("schema: the database needs to be migrated")
	// ErrUnknown is returned for data in the database that the program does not define.
	// Retire says that it is to be deleted.
	ErrUnknown = errors.New("schema: not defined")
	// ErrCannotBuild is returned for a definition that is added to data that is already
	// there, and that says neither how it is built from the data nor that it starts empty.
	ErrCannotBuild = errors.New("schema: cannot be built from the data that is there")
)

// Migration converts the data of a unit, which is a namespace at the root of a schema with
// everything under it, from a version to the next one. It may be interrupted and run again:
// see Migrator.
type Migration func(ctx context.Context, m *Migrator) error

// Migrator is what a migration works with.
//
// A migration of more data than a transaction holds is done in steps. Every step is a
// transaction that also saves a cursor, which says how far the migration has come, so that a
// migration that is interrupted goes on from its last step when the program starts again,
// and never does a step twice.
type Migrator struct {
	schema *Schema
	db     kv.DB
	unit   *Namespace
	from   uint64
	cursor []byte
}

// DB returns the database.
func (m *Migrator) DB() kv.DB {
	return m.db
}

// Namespace returns the namespace at the root of the schema that is migrated.
func (m *Migrator) Namespace() *Namespace {
	return m.unit
}

// From returns the version that the data is migrated from, to the next one.
func (m *Migrator) From() uint64 {
	return m.from
}

// Cursor returns the cursor that the last step saved, or nil if the migration starts.
func (m *Migrator) Cursor() []byte {
	return m.cursor
}

// Step runs a function in a transaction that also saves the cursor. The function may run
// more than once if the transaction conflicts with another one.
func (m *Migrator) Step(cursor []byte, fn func(tx kv.Tx) error) error {
	cursor = bytes.Clone(cursor)
	if cursor == nil {
		// A cursor that is saved is not the nil one of a migration that starts.
		cursor = []byte{}
	}

	err := m.db.Update(func(tx kv.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return m.schema.putUnit(tx, m.unit.name, unitState{
			Version:   m.from,
			Migrating: true,
			Cursor:    cursor,
		})
	})
	if err != nil {
		return err
	}
	m.cursor = cursor
	return nil
}

// Retire says that something that the program does not define any more is to be deleted from
// the database: Migrate deletes its data and what is recorded of it, with everything under
// it. The path is the one of its namespace, like "pets" or "pets/ext/by_owner".
// Without it, an index or an aggregation that is not defined any more is left where it is,
// and data that cannot be built again is an error.
func (s *Schema) Retire(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.retired = append(s.retired, path)
}

// What Migrate has done with a definition.
const (
	// Recorded is for a definition that the database had not seen.
	Recorded = "recorded"
	// Migrated is for a unit whose data was converted from a version to another one.
	Migrated = "migrated"
	// Built is for something that was built from the data that it is derived from.
	Built = "built"
	// Stale is for something that the program does not define any more and that is left in
	// the database, to be built again if it is defined again.
	Stale = "stale"
	// Retired is for something whose data was deleted.
	Retired = "retired"
)

// Action is something that Migrate has done.
type Action struct {
	// What is one of Recorded, Migrated, Built, Stale and Retired.
	What string
	// Kind and Path are the kind and the path of the namespace of the definition.
	Kind string
	Path string
	// From and To are the versions of a migration.
	From, To uint64
}

// String returns the action as text.
func (a Action) String() string {
	text := fmt.Sprintf("%s %s %q", a.What, a.Kind, a.Path)
	if a.What == Migrated {
		text += fmt.Sprintf(" from version %d to %d", a.From, a.To)
	}
	return text
}

// Report is what Migrate has done, in the order that it was done in.
type Report struct {
	Actions []Action
}

// String returns the actions as lines of text.
func (r *Report) String() string {
	lines := make([]string, 0, len(r.Actions))
	for _, a := range r.Actions {
		lines = append(lines, a.String())
	}
	return strings.Join(lines, "\n")
}

// Check validates the schema and compares it with the database, without changing the data.
// A unit that the database has not seen is recorded as it is defined. Anything that Migrate
// would have work to do for, like data of an older version or an index that is new to a
// store that is not, is reported with ErrNeedsMigration, and what Migrate would fail for is
// reported as it would be.
//
// It is for a program that must not change a database, and for the databases that are
// always new, like the ones of tests. A program that owns its database calls Migrate.
func (s *Schema) Check(db kv.DB) error {
	_, err := s.sync(context.Background(), db, false)
	return err
}

// Migrate brings the database to what the schema defines, and reports what it has done.
// It is meant to be called once when a program starts, after everything is defined and
// before the database is used, as it must be alone with the data.
//
// For every unit, which is a namespace at the root of the schema with everything under it:
//
//   - A unit that the database has not seen is recorded with its version.
//   - A unit of an older version is migrated, a version at a time, by the migrations that
//     were added for it. A version without a migration is ErrNoMigration, and a unit of a
//     newer version than the one of the program is ErrNewer.
//   - A definition that has changed while its version has not is ErrChanged.
//
// What is derived from other data, which is what has a Rebuild in its definition, like an
// index or an aggregation, needs no migrations: it is built when it is new, when its
// definition has changed, and when the data of its unit was migrated.
//
// For what the database has and the program does not define: something that is derived is
// left there and marked, so that it is built again if it is defined again, as it was not kept
// up to date in the meantime. Anything else is ErrUnknown. Retire deletes either.
//
// A migration or a build that is interrupted goes on, or starts again, the next time.
// When Migrate fails, nothing of what it did before is taken back.
func (s *Schema) Migrate(ctx context.Context, db kv.DB) (*Report, error) {
	return s.sync(ctx, db, true)
}

// work is what is to be done to a database, as it is found by comparing it with the schema.
type work struct {
	// The records that are written right away, in one transaction.
	units map[string]unitState
	defs  map[string]defState
	// What takes longer, which only Migrate does.
	retire   []retirement
	migrate  []*Namespace
	settle   map[*Namespace][]*Definition
	build    []*Definition
	report   Report
	errs     []error
	needsRun []string
}

type retirement struct {
	path   string
	kind   string
	prefix []byte
}

func (w *work) fail(err error, format string, args ...any) {
	w.errs = append(w.errs, fmt.Errorf("%w: "+format, append([]any{err}, args...)...))
}

func (w *work) note(what string, def *Definition) {
	w.report.Actions = append(w.report.Actions, Action{What: what, Kind: def.Kind, Path: def.ns.Path()})
}

func (s *Schema) sync(ctx context.Context, db kv.DB, run bool) (*Report, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	retired := slices.Clone(s.retired)
	s.mu.Unlock()

	// What is defined, by the units.
	var units []*Namespace
	byUnit := map[*Namespace][]*Definition{}
	defined := map[string]*Definition{}
	for _, def := range s.Definitions() {
		unit := def.ns.Unit()
		if _, ok := byUnit[unit]; !ok {
			units = append(units, unit)
		}
		byUnit[unit] = append(byUnit[unit], def)
		defined[def.ns.Path()] = def
	}
	for _, path := range retired {
		for defPath := range defined {
			if under(defPath, path) {
				return nil, fmt.Errorf("schema: %q is retired and defined", defPath)
			}
		}
	}

	var w *work
	err := db.Update(func(tx kv.Tx) error {
		w = &work{units: map[string]unitState{}, defs: map[string]defState{}, settle: map[*Namespace][]*Definition{}}
		stored, err := s.load(tx)
		if err != nil {
			return err
		}

		for _, unit := range units {
			s.compare(w, stored, unit, byUnit[unit], run)
		}
		s.compareUnknown(w, stored, defined, retired)

		if len(w.errs) > 0 {
			return nil
		}
		if !run && len(w.needsRun) > 0 {
			return nil
		}
		// What is new is recorded, and what is gone is marked.
		for name, state := range w.units {
			if err := s.putUnit(tx, name, state); err != nil {
				return err
			}
		}
		for path, state := range w.defs {
			if err := s.putDef(tx, path, state); err != nil {
				return err
			}
		}
		return s.dropLegacy(tx, stored)
	})
	if err != nil {
		return nil, err
	}
	if len(w.errs) > 0 {
		return nil, errors.Join(w.errs...)
	}
	if !run {
		if len(w.needsRun) > 0 {
			return nil, fmt.Errorf("%w: %s", ErrNeedsMigration, strings.Join(w.needsRun, "; "))
		}
		return &w.report, nil
	}

	for _, r := range w.retire {
		if err := s.retire(ctx, db, r); err != nil {
			return &w.report, err
		}
		w.report.Actions = append(w.report.Actions, Action{What: Retired, Kind: r.kind, Path: r.path})
	}
	// A unit that is retired as a whole is forgotten, with its version.
	err = db.Update(func(tx kv.Tx) error {
		for _, path := range retired {
			if strings.Contains(path, "/") {
				continue
			}
			if err := s.meta.Open(tx).Delete(unitKey(path)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return &w.report, err
	}
	for _, unit := range w.migrate {
		if err := s.migrate(ctx, db, w, unit); err != nil {
			return &w.report, err
		}
	}
	for _, def := range w.build {
		if err := s.build(ctx, db, def); err != nil {
			return &w.report, err
		}
		w.note(Built, def)
	}
	return &w.report, nil
}

// under reports whether a path is another one or is under it.
func under(path, parent string) bool {
	return path == parent || strings.HasPrefix(path, parent+"/")
}

// compare finds what is to be done for a unit.
func (s *Schema) compare(w *work, stored *storedState, unit *Namespace, defs []*Definition, run bool) {
	state, known := stored.units[unit.name]
	if !known {
		// A unit that was recorded before the units had versions is of the first one.
		for _, def := range defs {
			if _, ok := stored.defs[def.ns.Path()]; ok {
				state, known = unitState{Version: 1}, true
				w.units[unit.name] = state
				break
			}
		}
	}

	switch {
	case !known:
		// Nothing of the unit is in the database, so there is nothing to convert. What is
		// derived is built all the same by Migrate, in case that there is data that was
		// never recorded.
		w.units[unit.name] = unitState{Version: unit.version}
		for _, def := range defs {
			if def.Rebuild != nil && run {
				w.build = append(w.build, def)
				continue
			}
			w.defs[def.ns.Path()] = s.readyState(def)
			w.note(Recorded, def)
		}
		return

	case state.Version > unit.version:
		w.fail(ErrNewer, "%q is of version %d, and the program is of version %d",
			unit.Path(), state.Version, unit.version)
		return

	case state.Version < unit.version || state.Migrating:
		// Every version on the way needs its migration.
		to := max(unit.version, state.Version+1)
		for v := state.Version; v < to; v++ {
			if unit.migrations[v] == nil {
				w.fail(ErrNoMigration, "%q from version %d to %d", unit.Path(), v, v+1)
				return
			}
		}
		w.needsRun = append(w.needsRun,
			fmt.Sprintf("%q is of version %d, and the program is of version %d", unit.Path(), state.Version, unit.version))
		w.migrate = append(w.migrate, unit)
		// The definitions are looked at again once the data is the one of the program.
		for _, def := range defs {
			if _, ok := stored.defs[def.ns.Path()]; !ok && s.cannotBuild(w, def) {
				continue
			}
			w.settle[unit] = append(w.settle[unit], def)
		}
		return
	}

	for _, def := range defs {
		path := def.ns.Path()
		was, ok := stored.defs[path]
		want := def.fingerprint()

		if def.Rebuild != nil {
			// What is derived is built whenever it is not what the data would give.
			if !ok || was.State != stateReady || !bytes.Equal(was.Fingerprint, want) {
				w.needsRun = append(w.needsRun, fmt.Sprintf("%s %q is to be built", def.Kind, path))
				w.build = append(w.build, def)
			} else if was.legacy || !was.Derived {
				// The record is completed with what was not recorded of it.
				w.defs[path] = s.readyState(def)
			}
			continue
		}

		switch {
		case !ok:
			if s.cannotBuild(w, def) {
				continue
			}
			w.defs[path] = s.readyState(def)
			w.note(Recorded, def)
		case !bytes.Equal(was.Fingerprint, want):
			w.fail(ErrChanged, "%s %q is not what its data of version %d was written with",
				def.Kind, path, unit.version)
		case was.Derived || was.State != stateReady || was.legacy:
			// It was derived, or was left behind, and is defined as data of its own now,
			// or its record is completed with what was not recorded of it.
			w.defs[path] = s.readyState(def)
		}
	}
}

// cannotBuild reports whether a definition that is new to a unit that is not depends on the
// data of the unit without saying how it gets to know of it.
func (s *Schema) cannotBuild(w *work, def *Definition) bool {
	if def.Rebuild != nil || def.StartEmpty || !def.ns.dependent {
		return false
	}
	w.fail(ErrCannotBuild, "%s %q is new and the data that it depends on is not: "+
		"its definition needs a Rebuild, or StartEmpty", def.Kind, def.ns.Path())
	return true
}

// compareUnknown finds what is to be done for what the database has and the program does
// not define.
func (s *Schema) compareUnknown(w *work, stored *storedState, defined map[string]*Definition, retired []string) {
	paths := make([]string, 0, len(stored.defs))
	for path := range stored.defs {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	for _, path := range paths {
		if _, ok := defined[path]; ok {
			continue
		}
		was := stored.defs[path]

		if slices.ContainsFunc(retired, func(r string) bool { return under(path, r) }) {
			w.needsRun = append(w.needsRun, fmt.Sprintf("%s %q is to be retired", was.Kind, path))
			w.retire = append(w.retire, retirement{path: path, kind: was.Kind, prefix: was.Prefix})
			continue
		}
		switch {
		case !was.Derived:
			w.fail(ErrUnknown, "%s %q is in the database: Retire deletes it", was.Kind, path)
		case was.State != stateStale:
			// Its data is not kept up to date from now on.
			was.State = stateStale
			w.defs[path] = was
			w.report.Actions = append(w.report.Actions, Action{What: Stale, Kind: was.Kind, Path: path})
		}
	}
}

// retire deletes the data of something that is not defined any more, and what is recorded
// of it. The record goes last, so that a deletion that is interrupted is done again.
func (s *Schema) retire(ctx context.Context, db kv.DB, r retirement) error {
	if len(r.prefix) > 0 {
		if err := kv.DeleteRange(ctx, db, r.prefix, kv.PrefixEnd(r.prefix)); err != nil {
			return fmt.Errorf("schema: failed to delete the data of %q: %w", r.path, err)
		}
	}
	return db.Update(func(tx kv.Tx) error {
		return s.meta.Open(tx).Delete(defKey(r.path))
	})
}

// migrate runs the migrations of a unit up to the version of the program, and then settles
// its definitions.
func (s *Schema) migrate(ctx context.Context, db kv.DB, w *work, unit *Namespace) error {
	var state unitState
	err := db.View(func(tx kv.Tx) error {
		stored, err := s.load(tx)
		state = stored.units[unit.name]
		if _, ok := stored.units[unit.name]; !ok {
			state = unitState{Version: 1}
		}
		return err
	})
	if err != nil {
		return err
	}

	from := state.Version
	for state.Version < unit.version || state.Migrating {
		if err := ctx.Err(); err != nil {
			return err
		}

		m := &Migrator{schema: s, db: db, unit: unit, from: state.Version}
		if state.Migrating {
			m.cursor = state.Cursor
		}
		if err := unit.migrations[state.Version](ctx, m); err != nil {
			return fmt.Errorf("schema: failed to migrate %q from version %d to %d: %w",
				unit.Path(), state.Version, state.Version+1, err)
		}

		// The version is raised together with marking what is derived from the data, which
		// is not what the new data would give any more.
		state = unitState{Version: state.Version + 1}
		err := db.Update(func(tx kv.Tx) error {
			stored, err := s.load(tx)
			if err != nil {
				return err
			}
			for path, def := range stored.defs {
				if def.Derived && under(path, unit.name) && def.State == stateReady {
					def.State = stateStale
					if err := s.putDef(tx, path, def); err != nil {
						return err
					}
				}
			}
			return s.putUnit(tx, unit.name, state)
		})
		if err != nil {
			return err
		}
	}
	w.report.Actions = append(w.report.Actions,
		Action{What: Migrated, Kind: "unit", Path: unit.Path(), From: from, To: state.Version})

	// The data is the one of the program now, so its definitions are recorded as they are.
	err = db.Update(func(tx kv.Tx) error {
		for _, def := range w.settle[unit] {
			if def.Rebuild != nil {
				continue
			}
			if err := s.putDef(tx, def.ns.Path(), s.readyState(def)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, def := range w.settle[unit] {
		if def.Rebuild != nil {
			w.build = append(w.build, def)
		}
	}
	return nil
}

// build builds something that is derived from other data. It is marked while it is built, so
// that a build that is interrupted starts again.
func (s *Schema) build(ctx context.Context, db kv.DB, def *Definition) error {
	state := s.readyState(def)
	state.State = stateBuilding
	err := db.Update(func(tx kv.Tx) error {
		return s.putDef(tx, def.ns.Path(), state)
	})
	if err != nil {
		return err
	}

	if err := def.Rebuild(ctx, db); err != nil {
		return fmt.Errorf("schema: failed to build %s %q: %w", def.Kind, def.ns.Path(), err)
	}

	state.State = stateReady
	return db.Update(func(tx kv.Tx) error {
		return s.putDef(tx, def.ns.Path(), state)
	})
}

// The states of what is recorded of a definition.
const (
	stateReady    = "ready"
	stateBuilding = "building"
	stateStale    = "stale"
)

// unitState is what the database records of a unit.
type unitState struct {
	// Version is the version of the data of the unit.
	Version uint64 `json:"version"`
	// Migrating is set while the data is migrated from the version to the next one, and
	// Cursor is what the last step of the migration saved.
	Migrating bool   `json:"migrating,omitempty"`
	Cursor    []byte `json:"cursor,omitempty"`
}

// defState is what the database records of a definition.
type defState struct {
	Kind        string `json:"kind"`
	Prefix      []byte `json:"prefix"`
	Fingerprint []byte `json:"fingerprint"`
	// Derived is set for what is built from other data.
	Derived bool   `json:"derived,omitempty"`
	State   string `json:"state"`

	// legacy is set for a record that was read from a fingerprint that was recorded alone.
	legacy bool
}

func (s *Schema) readyState(def *Definition) defState {
	return defState{
		Kind:        def.Kind,
		Prefix:      def.ns.prefix,
		Fingerprint: def.fingerprint(),
		Derived:     def.Rebuild != nil,
		State:       stateReady,
	}
}

var (
	unitPrefix = []byte("u/")
	defPrefix  = []byte("d/")
	// legacyPrefix is the prefix of the fingerprints that were recorded alone, before there
	// were versions.
	legacyPrefix = []byte("f/")
)

func unitKey(name string) []byte { return append(bytes.Clone(unitPrefix), name...) }
func defKey(path string) []byte  { return append(bytes.Clone(defPrefix), path...) }

func (s *Schema) putUnit(tx kv.Tx, name string, state unitState) error {
	bz, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.meta.Open(tx).Set(unitKey(name), bz)
}

func (s *Schema) putDef(tx kv.Tx, path string, state defState) error {
	bz, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.meta.Open(tx).Set(defKey(path), bz)
}

// storedState is what the database records of the units and the definitions.
type storedState struct {
	units  map[string]unitState
	defs   map[string]defState
	legacy []string
}

func (s *Schema) load(tx kv.Tx) (*storedState, error) {
	stored := &storedState{units: map[string]unitState{}, defs: map[string]defState{}}
	meta := s.meta.Open(tx)

	scan := func(prefix []byte, each func(name string, value []byte) error) error {
		it, err := meta.Iterator(prefix, kv.PrefixEnd(prefix))
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			if err != nil {
				return err
			}
			if err := each(string(it.Key()[len(prefix):]), value); err != nil {
				return err
			}
		}
		return nil
	}

	err := scan(unitPrefix, func(name string, value []byte) error {
		var state unitState
		if err := json.Unmarshal(value, &state); err != nil {
			return fmt.Errorf("schema: the record of the unit %q cannot be read: %w", name, err)
		}
		stored.units[name] = state
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = scan(defPrefix, func(path string, value []byte) error {
		var state defState
		if err := json.Unmarshal(value, &state); err != nil {
			return fmt.Errorf("schema: the record of %q cannot be read: %w", path, err)
		}
		stored.defs[path] = state
		return nil
	})
	if err != nil {
		return nil, err
	}
	// A fingerprint that was recorded alone is the record of something that is ready.
	err = scan(legacyPrefix, func(path string, value []byte) error {
		stored.legacy = append(stored.legacy, path)
		if _, ok := stored.defs[path]; !ok {
			stored.defs[path] = defState{Fingerprint: bytes.Clone(value), State: stateReady, Kind: "definition", legacy: true}
		}
		return nil
	})
	return stored, err
}

// dropLegacy replaces the fingerprints that were recorded alone by records of today.
func (s *Schema) dropLegacy(tx kv.Tx, stored *storedState) error {
	meta := s.meta.Open(tx)
	for _, path := range stored.legacy {
		key := defKey(path)
		if ok, err := meta.Has(key); err != nil {
			return err
		} else if !ok {
			if err := s.putDef(tx, path, stored.defs[path]); err != nil {
				return err
			}
		}
		if err := meta.Delete(append(bytes.Clone(legacyPrefix), path...)); err != nil {
			return err
		}
	}
	return nil
}
