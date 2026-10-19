package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/ehsanranjbar/kvutil/kv"
)

// Unit is everything that a store, or anything else that keeps data under a prefix of its
// own, records of itself, and that is migrated as a whole: a version, a fingerprint of what
// the data depends on, and the parts that depend on the data, like the indexes of a store.
//
// The record of a unit is kept under its prefix, at MetaKey. A unit is safe for concurrent use.
type Unit struct {
	mu         sync.Mutex
	prefix     []byte
	kind       string
	name       string
	named      bool
	version    uint64
	attributes func() map[string]string
	migrations map[uint64]Migration
	parts      []Part
	retired    []string
}

// Part is something kept under the prefix of a unit and dependent on its data, like an index
// of a store.
type Part struct {
	// Name is the name of the part in its unit, like the name of an index.
	Name string
	// Kind is the kind of the part, like "index".
	Kind string
	// Prefix is the prefix of the keys of the part. It must be under the prefix of the unit.
	Prefix []byte
	// Attributes returns what the stored data of the part depends on, like the fields of an
	// index. A part whose attributes are not the ones that its data was written with is built
	// again if it is derived, and is ErrChanged if it is not. It may be nil.
	Attributes func() map[string]string
	// Rebuild builds the stored data of the part again from the data of its unit. It is for
	// derived data, like an index or an aggregation, which needs no migrations: it is built
	// when it is new, when its attributes have changed, and when the data of its unit was
	// migrated. It is nil for what cannot be built again.
	Rebuild func(ctx context.Context, db kv.DB) error
	// StartEmpty says that the part has nothing to know of the data that was there before it
	// was defined. It is for a part without Rebuild: when it is added to a unit that has
	// data already, it must say how it gets to know of it, or that it starts empty.
	StartEmpty bool
}

// Migratable is anything that has a unit, like a store.
type Migratable interface {
	Unit() *Unit
}

// NewUnit creates a unit of the data under a prefix. The kind says what the unit is, like
// "record", and the attributes return what the stored data of the unit depends on, like the
// names of the codecs; they may be nil. It panics if the prefix is empty.
func NewUnit(prefix []byte, kind string, attributes func() map[string]string) *Unit {
	if len(prefix) == 0 {
		// A mistake in the definition, which is made in code when the program starts.
		panic("migrate: the prefix of a unit must not be empty")
	}
	return &Unit{
		prefix:     bytes.Clone(prefix),
		kind:       kind,
		version:    1,
		attributes: attributes,
		migrations: map[uint64]Migration{},
	}
}

// Unit returns the unit itself, so that a unit is Migratable.
func (u *Unit) Unit() *Unit {
	return u
}

// Prefix returns the prefix of the data of the unit. The slice must not be modified.
func (u *Unit) Prefix() []byte {
	return u.prefix
}

// Kind returns the kind of the unit.
func (u *Unit) Kind() string {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.kind
}

// SetKind changes the kind of the unit. A store that is made of another one relabels its unit.
func (u *Unit) SetKind(kind string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.kind = kind
}

// Name returns the name of the unit, which is the prefix as text unless a name was set.
func (u *Unit) Name() string {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.nameLocked()
}

func (u *Unit) nameLocked() string {
	if u.named {
		return u.name
	}
	return Label(u.prefix)
}

// SetName sets the name that the unit goes by in reports and errors.
func (u *Unit) SetName(name string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.name, u.named = name, name != ""
}

// Version returns the version of the data of the unit, which is 1 by default.
func (u *Unit) Version() uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.version
}

// SetVersion sets the version of the data of the unit. The version is raised when the data
// must be converted for the program to read it, and a migration from every version before it
// says how. It panics on 0.
func (u *Unit) SetVersion(v uint64) {
	if v == 0 {
		panic(fmt.Sprintf("migrate: %s: the version must not be 0", u.Name()))
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	u.version = v
}

// AddMigration adds the migration that converts the data of the unit from a version to the
// next one. It panics on version 0, which there is nothing to migrate from, and on a second
// migration from the same version.
func (u *Unit) AddMigration(from uint64, m Migration) {
	if from == 0 {
		panic(fmt.Sprintf("migrate: %s: there is no version 0 to migrate from", u.Name()))
	}
	if m == nil {
		panic(fmt.Sprintf("migrate: %s: the migration from version %d is nil", u.Name(), from))
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.migrations[from] != nil {
		panic(fmt.Sprintf("migrate: %s has more than one migration from version %d", u.nameLocked(), from))
	}
	u.migrations[from] = m
}

// AddPart adds a part to the unit. It panics if the name of the part is empty or taken, or if
// its prefix is not under the prefix of the unit or is the key of the record of the unit.
func (u *Unit) AddPart(p Part) {
	u.mu.Lock()
	defer u.mu.Unlock()

	name := u.nameLocked()
	switch {
	case p.Name == "":
		panic(fmt.Sprintf("migrate: %s: the name of a part must not be empty", name))
	case len(p.Prefix) <= len(u.prefix) || !bytes.HasPrefix(p.Prefix, u.prefix):
		panic(fmt.Sprintf("migrate: %s: the prefix of the part %q is empty or not under the prefix of the unit",
			name, p.Name))
	case bytes.Equal(p.Prefix, MetaKey(u.prefix)):
		panic(fmt.Sprintf("migrate: %s: the prefix of the part %q is where the record of the unit is kept",
			name, p.Name))
	}
	for _, other := range u.parts {
		if other.Name == p.Name {
			panic(fmt.Sprintf("migrate: %s: the part %q is defined more than once", name, p.Name))
		}
	}

	p.Prefix = bytes.Clone(p.Prefix)
	u.parts = append(u.parts, p)
}

// Parts returns the parts of the unit, in the order that they were added.
func (u *Unit) Parts() []Part {
	u.mu.Lock()
	defer u.mu.Unlock()

	return slices.Clone(u.parts)
}

// Retire says that a part that is no longer defined is to be deleted from the database, by
// its name: Run deletes its data and forgets it. It is for data that cannot be built again,
// like the one of an extension of a store; what is derived is deleted when it is not defined
// any more without it. Without it, a part that the database has and the program does not
// define is ErrUnknown. A name that the database does not have is nothing to do, and a name
// that is defined is an error. It panics on an empty name.
func (u *Unit) Retire(name string) {
	if name == "" {
		panic(fmt.Sprintf("migrate: %s: the name of a part to retire must not be empty", u.Name()))
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	if !slices.Contains(u.retired, name) {
		u.retired = append(u.retired, name)
	}
}

// snapshot is what a unit is at a time, which the work of Run and Check is based on.
type snapshot struct {
	unit       *Unit
	prefix     []byte
	metaKey    []byte
	kind       string
	name       string
	version    uint64
	attributes func() map[string]string
	migrations map[uint64]Migration
	parts      []Part
	retired    []string
}

func (u *Unit) snapshot() *snapshot {
	u.mu.Lock()
	defer u.mu.Unlock()

	return &snapshot{
		unit:       u,
		prefix:     u.prefix,
		metaKey:    MetaKey(u.prefix),
		kind:       u.kind,
		name:       u.nameLocked(),
		version:    u.version,
		attributes: u.attributes,
		migrations: maps.Clone(u.migrations),
		parts:      slices.Clone(u.parts),
		retired:    slices.Clone(u.retired),
	}
}

// fingerprint returns a hash of what the stored data of the unit depends on.
func (s *snapshot) fingerprint() []byte {
	return fingerprint(s.kind, nil, s.attributes)
}

// partName returns the name of a part as reports and errors have it.
func (s *snapshot) partName(name string) string {
	return s.name + "/" + name
}

// fingerprint returns a hash of a kind, a prefix if there is one, and sorted attributes.
func fingerprint(kind string, prefix []byte, attributes func() map[string]string) []byte {
	h := sha256.New()
	if prefix == nil {
		fmt.Fprintf(h, "%s\n", kind)
	} else {
		fmt.Fprintf(h, "%s\n%x\n", kind, prefix)
	}

	if attributes != nil {
		attrs := attributes()
		for _, name := range slices.Sorted(maps.Keys(attrs)) {
			fmt.Fprintf(h, "%q=%q\n", name, attrs[name])
		}
	}
	return h.Sum(nil)
}

// Label returns a prefix as text: the prefix itself if every byte of it is printable ASCII,
// and "0x" followed by the bytes in hexadecimal otherwise.
func Label(prefix []byte) string {
	for _, b := range prefix {
		if b < 0x20 || b > 0x7e {
			return "0x" + hex.EncodeToString(prefix)
		}
	}
	return string(prefix)
}

// MetaKey returns the key that the record of the unit under a prefix is kept at, which is the
// prefix followed by 'm'.
func MetaKey(prefix []byte) []byte {
	key := make([]byte, 0, len(prefix)+1)
	key = append(key, prefix...)
	return append(key, 'm')
}
