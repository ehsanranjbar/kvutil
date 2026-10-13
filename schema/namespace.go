package schema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
)

// Namespace is a part of the keys of a database that belongs to one thing: all of its keys
// have the prefix of the namespace, and no other namespace has keys with that prefix,
// except for the namespaces under it.
type Namespace struct {
	schema   *Schema
	parent   *Namespace
	name     string
	prefix   []byte
	children []*Namespace
	def      *Definition
	cascades []*Namespace

	// version is the version of a namespace at the root, and of everything under it.
	version uint64
	// migrations are the migrations of a namespace at the root, by the versions that they
	// are from.
	migrations map[uint64]Migration
	// dependent is set for a namespace whose definitions depend on the data of another one.
	dependent bool
}

type namespaceOptions struct {
	prefix    []byte
	version   uint64
	dependent bool
}

// NamespaceOption is an option of a namespace.
type NamespaceOption func(*namespaceOptions)

// WithPrefix sets the bytes that a namespace adds to the prefix of its parent.
// By default they are the name of the namespace, in a form that marks where it ends.
func WithPrefix(prefix []byte) NamespaceOption {
	return func(o *namespaceOptions) {
		o.prefix = bytes.Clone(prefix)
	}
}

// Version sets the version of a namespace at the root of a schema, which is the version of the
// data of everything that is defined in it and under it: a store with its indexes. It is 1 by
// default. The version is raised when the data must be converted for the program to read it,
// and a migration from every version before it says how; see Migrate.
func Version(version uint64) NamespaceOption {
	return func(o *namespaceOptions) {
		o.version = version
	}
}

// Dependent marks a namespace as the place of things that depend on data that is kept
// somewhere else, like the extensions of a store. A definition that is added to it when the
// data is already there must say how it is built from the data, with the Rebuild of its
// definition, or that it starts empty.
func Dependent() NamespaceOption {
	return func(o *namespaceOptions) {
		o.dependent = true
	}
}

// Sub defines a namespace under this one. Any problem with it, such as a prefix that overlaps
// with the prefix of another namespace, is reported by the Validate and Check of the schema.
func (ns *Namespace) Sub(name string, opts ...NamespaceOption) *Namespace {
	var o namespaceOptions
	for _, opt := range opts {
		opt(&o)
	}

	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	child := &Namespace{schema: s, parent: ns, name: name, dependent: ns.dependent || o.dependent}
	path := child.Path()

	switch {
	case o.version == 0:
		if ns.parent == nil {
			child.version = 1
		}
	case ns.parent != nil:
		s.fail("namespace %q: only a namespace at the root of the schema has a version", path)
	default:
		child.version = o.version
	}

	own := o.prefix
	switch {
	case own != nil:
	case ns.parent == nil && s.allocator != nil:
		allocated, err := s.allocator.Name(name)
		if err != nil {
			s.fail("namespace %q: failed to allocate a prefix: %w", path, err)
		}
		own = allocated
	default:
		own = lex.AppendTerminated(nil, []byte(name))
	}
	child.prefix = append(bytes.Clone(ns.prefix), own...)

	switch {
	case name == "" || strings.Contains(name, "/"):
		s.fail("namespace %q: a name must not be empty or have a slash in it", path)
	case len(own) == 0:
		s.fail("namespace %q: the prefix is empty", path)
	case ns.def != nil && ns.def.Keys:
		s.fail("namespace %q: %s %q keeps its keys right under its prefix, so it cannot have a namespace under it",
			path, ns.def.Kind, ns.Path())
	}
	for _, sibling := range ns.children {
		if sibling.name == name {
			s.fail("namespace %q is defined more than once", path)
		} else if bytes.HasPrefix(sibling.prefix, child.prefix) || bytes.HasPrefix(child.prefix, sibling.prefix) {
			s.fail("namespace %q: the prefix overlaps with the prefix of namespace %q", path, sibling.Path())
		}
	}

	ns.children = append(ns.children, child)
	return child
}

// Schema returns the schema of the namespace.
func (ns *Namespace) Schema() *Schema {
	return ns.schema
}

// Name returns the name of the namespace in its parent.
func (ns *Namespace) Name() string {
	return ns.name
}

// Unit returns the namespace at the root of the schema that this one is in or is itself.
// It is what has a version and what is migrated as a whole.
func (ns *Namespace) Unit() *Namespace {
	for ns.parent != nil && ns.parent.parent != nil {
		ns = ns.parent
	}
	return ns
}

// Version returns the version of the data of the namespace, which is the one of its unit.
func (ns *Namespace) Version() uint64 {
	return ns.Unit().version
}

// AddMigration adds the migration that converts the data of the unit of the namespace from a
// version to the next one. A problem with it, like a second migration from the same version,
// is reported by the Validate of the schema.
func (ns *Namespace) AddMigration(from uint64, m Migration) {
	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	unit := ns.Unit()
	switch {
	case from == 0:
		s.fail("namespace %q: there is no version 0 to migrate from", unit.Path())
	case unit.migrations[from] != nil:
		s.fail("namespace %q has more than one migration from version %d", unit.Path(), from)
	}
	if unit.migrations == nil {
		unit.migrations = map[uint64]Migration{}
	}
	unit.migrations[from] = m
}

// Path returns the names of the namespace and of the namespaces that it is under, joined by slashes.
func (ns *Namespace) Path() string {
	if ns.parent == nil || ns.parent.parent == nil {
		return ns.name
	}
	return ns.parent.Path() + "/" + ns.name
}

// Prefix returns the prefix of all the keys of the namespace. The slice must not be modified.
func (ns *Namespace) Prefix() []byte {
	return ns.prefix
}

// Range returns the range of all the keys of the namespace.
func (ns *Namespace) Range() kv.KeyRange {
	return kv.KeyRange{Start: ns.prefix, End: kv.PrefixEnd(ns.prefix)}
}

// Open returns the keyspace of the namespace in a transaction.
func (ns *Namespace) Open(tx kv.Tx) *prefix.Tx {
	return prefix.New(tx, ns.prefix)
}

// Define records what the namespace is used for.
func (ns *Namespace) Define(def Definition) *Definition {
	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case ns.def != nil:
		s.fail("namespace %q is used by both %s and %s", ns.Path(), ns.def.Kind, def.Kind)
	case def.Keys && len(ns.children) > 0:
		s.fail("namespace %q: %s keeps its keys right under its prefix, so it cannot have a namespace under it",
			ns.Path(), def.Kind)
	}

	def.ns = ns
	ns.def = &def
	return ns.def
}

// Definition returns what the namespace is used for, or nil if nothing is defined in it.
func (ns *Namespace) Definition() *Definition {
	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	return ns.def
}

// Definitions returns what is defined in the namespace and in the namespaces under it,
// parents before their children: a store before its indexes.
func (ns *Namespace) Definitions() []*Definition {
	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	var defs []*Definition
	var walk func(n *Namespace)
	walk = func(n *Namespace) {
		if n.def != nil {
			defs = append(defs, n.def)
		}
		for _, child := range n.children {
			walk(child)
		}
	}
	walk(ns)
	return defs
}

// Cascade records that writing to this namespace may write to another one too,
// like deleting a record that deletes the records that belong to it.
func (ns *Namespace) Cascade(to *Namespace) {
	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	if !slices.Contains(ns.cascades, to) {
		ns.cascades = append(ns.cascades, to)
	}
}

// Footprint returns the ranges of the keys that an operation on the namespace may touch:
// its own range, which has everything that is defined under it, and the ranges of the
// namespaces that it cascades to.
func (ns *Namespace) Footprint() []kv.KeyRange {
	s := ns.schema
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		ranges  []kv.KeyRange
		visited = map[*Namespace]bool{}
		visit   func(n *Namespace)
	)
	visit = func(n *Namespace) {
		if visited[n] {
			return
		}
		visited[n] = true
		ranges = append(ranges, n.Range())

		// What is defined under a namespace may cascade somewhere else too.
		var walk func(n *Namespace)
		walk = func(n *Namespace) {
			for _, to := range n.cascades {
				visit(to)
			}
			for _, child := range n.children {
				walk(child)
			}
		}
		walk(n)
	}
	visit(ns)
	return ranges
}

// Definition describes something that is defined in a namespace.
type Definition struct {
	// Kind is the kind of the thing, like a store or an index.
	Kind string
	// Keys is set if the keys are right under the prefix of the namespace, which means that
	// there cannot be a namespace under it.
	Keys bool
	// KeyType and ValueType are the Go types of the keys and the values. They are informative:
	// renaming a type does not change the definition.
	KeyType   string
	ValueType string
	// Attributes returns what the stored data depends on, like the names of the codecs.
	// A definition whose attributes are not the ones that the data was written with is
	// reported by the Check of the schema.
	Attributes func() map[string]string
	// Validate reports whether the definition is complete.
	Validate func() error
	// Descriptor describes the thing to whoever plans queries on it. Its type depends on the kind.
	Descriptor any
	// Object is the thing itself, for whoever needs more of it than its description,
	// like what runs the background work of the things of a kind.
	Object any
	// Rebuild builds the stored data of the thing again from the data that it is derived
	// from, for something that keeps nothing of its own, like an index. Migrate calls it for
	// a definition that is new, that has changed, or whose data was migrated, so such a
	// thing needs no migrations. It is nil for what cannot be built again, like a store.
	Rebuild func(ctx context.Context, db kv.DB) error
	// StartEmpty says that the thing has nothing to know of the data that was there before
	// it was defined. It is for a definition without Rebuild in a Dependent namespace.
	StartEmpty bool

	ns *Namespace
}

// Namespace returns the namespace of the definition.
func (d *Definition) Namespace() *Namespace {
	return d.ns
}

// fingerprint returns a hash of what the stored data of the definition depends on.
func (d *Definition) fingerprint() []byte {
	h := sha256.New()
	fmt.Fprintf(h, "%s\n%x\n", d.Kind, d.ns.prefix)

	if d.Attributes != nil {
		attrs := d.Attributes()
		for _, name := range slices.Sorted(maps.Keys(attrs)) {
			fmt.Fprintf(h, "%q=%q\n", name, attrs[name])
		}
	}
	return h.Sum(nil)
}
