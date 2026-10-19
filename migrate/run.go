package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil/kv"
)

var (
	// ErrChanged is returned for a unit or a part that is not what its data was written with,
	// while its version is: something was changed without a new version.
	ErrChanged = errors.New("migrate: definition has changed")
	// ErrNewer is returned for data of a version that is newer than the one of the program.
	ErrNewer = errors.New("migrate: the database is newer than the program")
	// ErrNoMigration is returned for data of an older version that the program has no
	// migration from.
	ErrNoMigration = errors.New("migrate: no migration")
	// ErrNeedsMigration is returned by Check for a database that Run has work to do on.
	ErrNeedsMigration = errors.New("migrate: the database needs to be migrated")
	// ErrUnknown is returned for data in the database that the program does not define.
	// Retire says that it is to be deleted.
	ErrUnknown = errors.New("migrate: not defined")
	// ErrCannotBuild is returned for a part that is added to data that is already there, and
	// that says neither how it is built from the data nor that it starts empty.
	ErrCannotBuild = errors.New("migrate: cannot be built from the data that is there")
)

// Migration converts the data of a unit from a version to the next one. It may be interrupted
// and run again: see Migrator.
type Migration func(ctx context.Context, m *Migrator) error

// Migrator is what a migration works with.
//
// A migration of more data than a transaction holds is done in steps. Every step is a
// transaction that also saves a cursor, which says how far the migration has come, so that a
// migration that is interrupted goes on from its last step when the program starts again,
// and never does a step twice.
type Migrator struct {
	db     kv.DB
	snap   *snapshot
	from   uint64
	cursor []byte
}

// DB returns the database.
func (m *Migrator) DB() kv.DB {
	return m.db
}

// Unit returns the unit that is migrated.
func (m *Migrator) Unit() *Unit {
	return m.snap.unit
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
		return modify(tx, m.snap, func(st *unitState) {
			st.Version = m.from
			st.Migrating = true
			st.Cursor = cursor
		})
	})
	if err != nil {
		return err
	}
	m.cursor = cursor
	return nil
}

// What Run has done with a unit or a part.
const (
	// Recorded is for a unit or a part that the database had not seen.
	Recorded = "recorded"
	// Migrated is for a unit whose data was converted from a version to another one.
	Migrated = "migrated"
	// Built is for a part that was built from the data that it is derived from.
	Built = "built"
	// Dropped is for a part that was derived from other data and is not defined any more,
	// whose data was deleted. It is built again if it is defined again.
	Dropped = "dropped"
	// Retired is for something that was retired, whose data was deleted.
	Retired = "retired"
)

// Action is something that Run has done.
type Action struct {
	// What is one of Recorded, Migrated, Built, Dropped and Retired.
	What string
	// Kind and Name are the kind and the name of the unit, or of the part, which is the name
	// of its unit, a slash, and its own name.
	Kind string
	Name string
	// From and To are the versions of a migration.
	From, To uint64
}

// String returns the action as text.
func (a Action) String() string {
	text := fmt.Sprintf("%s %s %q", a.What, a.Kind, a.Name)
	if a.What == Migrated {
		text += fmt.Sprintf(" from version %d to %d", a.From, a.To)
	}
	return text
}

// Report is what Run has done, in the order that it was done in.
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

// The states of what is recorded of a part.
const (
	stateReady    = "ready"
	stateBuilding = "building"
	stateStale    = "stale"
)

// unitState is what the database records of a unit, under its prefix.
type unitState struct {
	// Kind and Fingerprint are the ones the data was written with.
	Kind        string `json:"kind"`
	Version     uint64 `json:"version"`
	Fingerprint []byte `json:"fingerprint"`
	// Migrating is set while the data is migrated from the version to the next one, and
	// Cursor is what the last step of the migration saved.
	Migrating bool                 `json:"migrating,omitempty"`
	Cursor    []byte               `json:"cursor,omitempty"`
	Parts     map[string]partState `json:"parts,omitempty"`
}

// partState is what the database records of a part.
type partState struct {
	Kind        string `json:"kind"`
	Prefix      []byte `json:"prefix"`
	Fingerprint []byte `json:"fingerprint"`
	// Derived is set for what is built from other data.
	Derived bool   `json:"derived,omitempty"`
	State   string `json:"state"`
}

func readyPart(p Part) partState {
	return partState{
		Kind:        p.Kind,
		Prefix:      p.Prefix,
		Fingerprint: fingerprint(p.Kind, p.Prefix, p.Attributes),
		Derived:     p.Rebuild != nil,
		State:       stateReady,
	}
}

// readState reads the record of a unit, or returns nil if it has none.
func readState(tx kv.Tx, s *snapshot) (*unitState, error) {
	bz, err := tx.Get(s.metaKey)
	if errors.Is(err, kv.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	st := &unitState{}
	if err := json.Unmarshal(bz, st); err != nil {
		return nil, fmt.Errorf("migrate: the record of %q cannot be read: %w", s.name, err)
	}
	if st.Parts == nil {
		st.Parts = map[string]partState{}
	}
	return st, nil
}

func writeState(tx kv.Tx, s *snapshot, st *unitState) error {
	bz, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return tx.Set(s.metaKey, bz)
}

// modify reads the record of a unit, changes it and writes it, so that what the function
// does not touch is kept.
func modify(tx kv.Tx, s *snapshot, fn func(st *unitState)) error {
	st, err := readState(tx, s)
	if err != nil {
		return err
	}
	if st == nil {
		return fmt.Errorf("migrate: the record of %q is missing", s.name)
	}
	fn(st)
	return writeState(tx, s, st)
}

func modifyDB(db kv.DB, s *snapshot, fn func(st *unitState)) error {
	return db.Update(func(tx kv.Tx) error {
		return modify(tx, s, fn)
	})
}

// prepare takes the units that are given: a unit that is given more than once is one, and
// units that overlap are a mistake, as is a part that is retired and defined.
func prepare(units []Migratable) ([]*snapshot, error) {
	var (
		snaps []*snapshot
		errs  []error
		seen  = map[*Unit]bool{}
	)
	for _, m := range units {
		var u *Unit
		if m != nil {
			u = m.Unit()
		}
		if u == nil {
			errs = append(errs, errors.New("migrate: a unit is nil"))
			continue
		}
		if !seen[u] {
			seen[u] = true
			snaps = append(snaps, u.snapshot())
		}
	}

	for i, a := range snaps {
		for _, b := range snaps[:i] {
			if bytes.HasPrefix(a.prefix, b.prefix) || bytes.HasPrefix(b.prefix, a.prefix) {
				errs = append(errs, fmt.Errorf("migrate: the prefix of %q (%x) overlaps with the prefix of %q (%x)",
					a.name, a.prefix, b.name, b.prefix))
			}
		}
	}

	for _, s := range snaps {
		for _, name := range s.retired {
			if slices.ContainsFunc(s.parts, func(p Part) bool { return p.Name == name }) {
				errs = append(errs, fmt.Errorf("migrate: %q is retired and defined", s.partName(name)))
			}
		}
	}
	return snaps, errors.Join(errs...)
}

// retirement is something whose data is to be deleted: a part that was retired, or a derived
// one that is not defined any more.
type retirement struct {
	what   string
	name   string
	kind   string
	prefix []byte
}

// plan is what is to be done for a unit, as it is found by comparing it with its record.
type plan struct {
	snap *snapshot
	// state is the record, which is written right away if write is set.
	state *unitState
	write bool
	// What takes longer, which only Run does.
	retire  []retirement
	migrate bool
	settle  []Part
	build   []Part
}

// work is what is to be done to a database, as it is found by comparing it with the units.
type work struct {
	plans    []*plan
	report   Report
	errs     []error
	needsRun []string
}

func (w *work) fail(err error, format string, args ...any) {
	w.errs = append(w.errs, fmt.Errorf("%w: "+format, append([]any{err}, args...)...))
}

func (w *work) note(what, kind, name string) {
	w.report.Actions = append(w.report.Actions, Action{What: what, Kind: kind, Name: name})
}

// Check compares the units with the database, without changing any data. A unit that the
// database has not seen is recorded as it is defined. Anything that Run would have work to
// do for, like data of an older version or an index that is new to a store that is not, is
// reported with ErrNeedsMigration, and nothing is written then. What Run would fail for is
// reported as it would be.
//
// It is for a program that must not change a database, and for the databases that are
// always new, like the ones of tests. A program that owns its database calls Run.
func Check(db kv.DB, units ...Migratable) error {
	snaps, err := prepare(units)
	if err != nil {
		return err
	}

	w, err := compareAll(db, snaps, false)
	if err != nil {
		return err
	}
	if len(w.errs) > 0 {
		return errors.Join(w.errs...)
	}
	if len(w.needsRun) > 0 {
		return fmt.Errorf("%w: %s", ErrNeedsMigration, strings.Join(w.needsRun, "; "))
	}
	return nil
}

// Run brings the database to what the units define, and reports what it has done. It is
// meant to be called once when a program starts, after everything is defined and before the
// database is used, as it must be alone with the data.
//
// For every unit:
//
//   - A unit that the database has not seen is recorded with its version. What is derived
//     from its data is built all the same, in case that there is data that was never recorded.
//   - A unit of an older version is migrated, a version at a time, by the migrations that
//     were added for it. A version without a migration is ErrNoMigration, and a unit of a
//     newer version than the one of the program is ErrNewer.
//   - A unit that has changed while its version has not is ErrChanged.
//
// What is derived from the data of a unit, which is a part with a Rebuild, like an index or an
// aggregation, needs no migrations: it is built when it is new, when it has changed, and when
// the data of its unit was migrated.
//
// For the parts that the database has and the program does not define: a part that is derived
// is deleted, and is built again if it is defined again. Any other part is ErrUnknown, unless
// it is retired, which deletes it.
//
// Units are compared all together before anything is changed, and then the units are brought
// up to date one after the other, in the order that they are given. A migration or a build that
// is interrupted goes on, or starts again, the next time. When Run fails, nothing of what it
// did before is taken back, and the report has what it did.
func Run(ctx context.Context, db kv.DB, units ...Migratable) (*Report, error) {
	snaps, err := prepare(units)
	if err != nil {
		return &Report{}, err
	}

	w, err := compareAll(db, snaps, true)
	if err != nil {
		return &Report{}, err
	}
	if len(w.errs) > 0 {
		return &Report{}, errors.Join(w.errs...)
	}

	for _, p := range w.plans {
		if err := w.execute(ctx, db, p); err != nil {
			return &w.report, err
		}
	}
	return &w.report, nil
}

// compareAll compares the units with their records in one transaction, and writes what is to
// be written right away if nothing is wrong and, for a check, nothing needs to be run.
func compareAll(db kv.DB, snaps []*snapshot, run bool) (*work, error) {
	var w *work
	err := db.Update(func(tx kv.Tx) error {
		w = &work{}
		for _, s := range snaps {
			if err := w.compare(tx, s, run); err != nil {
				return err
			}
		}

		if len(w.errs) > 0 || (!run && len(w.needsRun) > 0) {
			return nil
		}
		for _, p := range w.plans {
			if p.write {
				if err := writeState(tx, p.snap, p.state); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

// compare finds what is to be done for a unit.
func (w *work) compare(tx kv.Tx, s *snapshot, run bool) error {
	p := &plan{snap: s}
	w.plans = append(w.plans, p)

	rec, err := readState(tx, s)
	if err != nil {
		return err
	}

	if rec == nil {
		// Nothing of the unit is in the database, so there is nothing to convert. What is
		// derived is built all the same by Run, in case that there is data that was never
		// recorded.
		rec = &unitState{
			Kind:        s.kind,
			Version:     s.version,
			Fingerprint: s.fingerprint(),
			Parts:       map[string]partState{},
		}
		p.state, p.write = rec, true
		w.note(Recorded, s.kind, s.name)

		for _, part := range s.parts {
			if part.Rebuild != nil && run {
				p.build = append(p.build, part)
				continue
			}
			rec.Parts[part.Name] = readyPart(part)
			w.note(Recorded, part.Kind, s.partName(part.Name))
		}
		return nil
	}
	p.state = rec

	switch {
	case rec.Version > s.version:
		w.fail(ErrNewer, "%q is of version %d, and the program is of version %d", s.name, rec.Version, s.version)
		return nil

	case rec.Version < s.version || rec.Migrating:
		// Every version on the way needs its migration.
		to := max(s.version, rec.Version+1)
		for v := rec.Version; v < to; v++ {
			if s.migrations[v] == nil {
				w.fail(ErrNoMigration, "%q from version %d to %d", s.name, v, v+1)
				return nil
			}
		}
		w.needsRun = append(w.needsRun,
			fmt.Sprintf("%q is of version %d, and the program is of version %d", s.name, rec.Version, s.version))
		p.migrate = true

		// The parts are looked at again once the data is the one of the program.
		for _, part := range s.parts {
			if _, ok := rec.Parts[part.Name]; !ok && w.cannotBuild(s, part) {
				continue
			}
			p.settle = append(p.settle, part)
		}

	default:
		if !bytes.Equal(rec.Fingerprint, s.fingerprint()) {
			w.fail(ErrChanged, "%s %q is not what its data of version %d was written with", s.kind, s.name, s.version)
			return nil
		}
		w.compareParts(p, rec)
	}

	w.compareUnknown(p, rec, run)
	return nil
}

// cannotBuild reports whether a part that is new to a unit that is not depends on the data of
// the unit without saying how it gets to know of it.
func (w *work) cannotBuild(s *snapshot, part Part) bool {
	if part.Rebuild != nil || part.StartEmpty {
		return false
	}
	w.fail(ErrCannotBuild, "%s %q is new and the data that it depends on is not: "+
		"it needs a Rebuild, or StartEmpty", part.Kind, s.partName(part.Name))
	return true
}

// compareParts finds what is to be done for the parts of a unit that is of the version of the
// program, and of the fingerprint.
func (w *work) compareParts(p *plan, rec *unitState) {
	s := p.snap
	for _, part := range s.parts {
		was, ok := rec.Parts[part.Name]
		want := fingerprint(part.Kind, part.Prefix, part.Attributes)

		if part.Rebuild != nil {
			// What is derived is built whenever it is not what the data would give.
			switch {
			case !ok || was.State != stateReady || !bytes.Equal(was.Fingerprint, want):
				w.needsRun = append(w.needsRun, fmt.Sprintf("%s %q is to be built", part.Kind, s.partName(part.Name)))
				p.build = append(p.build, part)
			case !was.Derived:
				// It was data of its own, and is derived now.
				rec.Parts[part.Name] = readyPart(part)
				p.write = true
			}
			continue
		}

		switch {
		case !ok:
			if w.cannotBuild(s, part) {
				continue
			}
			rec.Parts[part.Name] = readyPart(part)
			p.write = true
			w.note(Recorded, part.Kind, s.partName(part.Name))
		case !bytes.Equal(was.Fingerprint, want):
			w.fail(ErrChanged, "%s %q is not what its data of version %d was written with",
				part.Kind, s.partName(part.Name), s.version)
		case was.Derived || was.State != stateReady:
			// It was derived, or was left behind, and is data of its own now.
			rec.Parts[part.Name] = readyPart(part)
			p.write = true
		}
	}
}

// compareUnknown finds what is to be done for the parts that the database has and the program
// does not define.
func (w *work) compareUnknown(p *plan, rec *unitState, run bool) {
	s := p.snap
	for _, name := range slices.Sorted(maps.Keys(rec.Parts)) {
		if slices.ContainsFunc(s.parts, func(part Part) bool { return part.Name == name }) {
			continue
		}
		was := rec.Parts[name]

		switch {
		case slices.Contains(s.retired, name):
			w.needsRun = append(w.needsRun, fmt.Sprintf("%s %q is to be retired", was.Kind, s.partName(name)))
			p.retire = append(p.retire, retirement{what: Retired, name: name, kind: was.Kind, prefix: was.Prefix})

		case !was.Derived:
			w.fail(ErrUnknown, "%s %q is in the database and is not defined: retire it to delete it",
				was.Kind, s.partName(name))

		case run:
			// What is derived is not kept up to date from now on, so it is of no use. Run
			// deletes it.
			p.retire = append(p.retire, retirement{what: Dropped, name: name, kind: was.Kind, prefix: was.Prefix})

		case was.State != stateStale:
			// Check leaves it, marked, so that it is built again if it is defined again; a
			// program that does not define it does not need it to be gone.
			was.State = stateStale
			rec.Parts[name] = was
			p.write = true
		}
	}
}

// execute does the long work of a unit.
func (w *work) execute(ctx context.Context, db kv.DB, p *plan) error {
	s := p.snap
	for _, r := range p.retire {
		if err := w.retire(ctx, db, s, r); err != nil {
			return err
		}
	}
	if p.migrate {
		if err := w.migrate(ctx, db, p); err != nil {
			return err
		}
	}
	for _, part := range p.build {
		if err := w.build(ctx, db, s, part); err != nil {
			return err
		}
	}
	return nil
}

// retire deletes the data of a part that is not defined any more, and what is recorded of it.
// The record goes last, so that a deletion that is interrupted is done again.
func (w *work) retire(ctx context.Context, db kv.DB, s *snapshot, r retirement) error {
	// The record is what the database says, and the data that it points to must be the one
	// of the unit.
	if len(r.prefix) <= len(s.prefix) || !bytes.HasPrefix(r.prefix, s.prefix) || bytes.Equal(r.prefix, s.metaKey) {
		return fmt.Errorf("migrate: the record of %q has the prefix %x, which is not under the prefix of its unit",
			s.partName(r.name), r.prefix)
	}

	if err := kv.DeleteRange(ctx, db, r.prefix, kv.PrefixEnd(r.prefix)); err != nil {
		return fmt.Errorf("migrate: failed to delete the data of %q: %w", s.partName(r.name), err)
	}
	err := modifyDB(db, s, func(st *unitState) {
		delete(st.Parts, r.name)
	})
	if err != nil {
		return err
	}

	w.note(r.what, r.kind, s.partName(r.name))
	return nil
}

// migrate runs the migrations of a unit up to the version of the program, and then settles
// its parts.
func (w *work) migrate(ctx context.Context, db kv.DB, p *plan) error {
	s := p.snap

	var rec *unitState
	err := db.View(func(tx kv.Tx) error {
		var err error
		rec, err = readState(tx, s)
		if err == nil && rec == nil {
			err = fmt.Errorf("migrate: the record of %q is missing", s.name)
		}
		return err
	})
	if err != nil {
		return err
	}

	from, version := rec.Version, rec.Version
	for version < s.version || rec.Migrating {
		if err := ctx.Err(); err != nil {
			return err
		}

		migration := s.migrations[version]
		if migration == nil {
			return fmt.Errorf("%w: %q from version %d to %d", ErrNoMigration, s.name, version, version+1)
		}
		m := &Migrator{db: db, snap: s, from: version}
		if rec.Migrating {
			m.cursor = rec.Cursor
		}
		if err := migration(ctx, m); err != nil {
			return fmt.Errorf("migrate: failed to migrate %q from version %d to %d: %w",
				s.name, version, version+1, err)
		}

		// The version is raised together with marking what is derived from the data, which
		// is not what the new data would give any more.
		version++
		rec.Migrating = false
		err := modifyDB(db, s, func(st *unitState) {
			st.Version = version
			st.Migrating = false
			st.Cursor = nil
			for name, part := range st.Parts {
				if part.Derived {
					part.State = stateStale
					st.Parts[name] = part
				}
			}
		})
		if err != nil {
			return err
		}
	}
	w.report.Actions = append(w.report.Actions,
		Action{What: Migrated, Kind: s.kind, Name: s.name, From: from, To: version})

	// The data is the one of the program now, so the unit and its parts are recorded as they
	// are. A version may change the codecs of the data.
	err = modifyDB(db, s, func(st *unitState) {
		st.Kind = s.kind
		st.Fingerprint = s.fingerprint()
		for _, part := range p.settle {
			if part.Rebuild == nil {
				st.Parts[part.Name] = readyPart(part)
			}
		}
	})
	if err != nil {
		return err
	}
	for _, part := range p.settle {
		if part.Rebuild != nil {
			p.build = append(p.build, part)
		}
	}
	return nil
}

// build builds a part that is derived from the data of its unit. It is marked while it is
// built, so that a build that is interrupted starts again.
func (w *work) build(ctx context.Context, db kv.DB, s *snapshot, part Part) error {
	state := readyPart(part)
	state.State = stateBuilding
	err := modifyDB(db, s, func(st *unitState) {
		st.Parts[part.Name] = state
	})
	if err != nil {
		return err
	}

	if err := part.Rebuild(ctx, db); err != nil {
		return fmt.Errorf("migrate: failed to build %s %q: %w", part.Kind, s.partName(part.Name), err)
	}

	state.State = stateReady
	err = modifyDB(db, s, func(st *unitState) {
		st.Parts[part.Name] = state
	})
	if err != nil {
		return err
	}

	w.note(Built, part.Kind, s.partName(part.Name))
	return nil
}
