// Package schema describes what is defined in a database: the namespaces that divide its
// keys and the stores, indexes and the like that live in them. It holds no data.
package schema

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
)

// ErrChanged is returned by Check for a definition that is not the one the data was written with.
var ErrChanged = errors.New("schema: definition has changed")

// reservedName is the name of the namespace that the schema keeps its own records in.
const reservedName = "_schema"

// PrefixAllocator gives the namespaces at the root of a schema their prefixes.
// It must give a name the same prefix every time.
type PrefixAllocator interface {
	Name(name string) ([]byte, error)
}

// Schema is the registry of the definitions of a database.
// It is built from code when a program starts and is safe for concurrent use.
type Schema struct {
	mu        sync.Mutex
	format    codec.Format
	allocator PrefixAllocator
	root      *Namespace
	meta      *Namespace
	errs      []error
}

// Option is an option of a Schema.
type Option func(*Schema)

// WithFormat sets the format of the values of the stores that are not given a value codec.
// It is Msgpack by default.
func WithFormat(format codec.Format) Option {
	return func(s *Schema) {
		s.format = format
	}
}

// WithPrefixAllocator makes the namespaces at the root of the schema take their prefixes from
// an allocator, such as a NameRegistry, instead of using their names as their prefixes.
func WithPrefixAllocator(allocator PrefixAllocator) Option {
	return func(s *Schema) {
		s.allocator = allocator
	}
}

// New creates a new Schema.
func New(opts ...Option) *Schema {
	s := &Schema{}
	for _, opt := range opts {
		opt(s)
	}

	s.root = &Namespace{schema: s}
	s.meta = s.root.Sub(reservedName)
	s.meta.Define(Definition{Kind: "schema", Keys: true})
	return s
}

// Format returns the default format of values, which is nil if none was set.
func (s *Schema) Format() codec.Format {
	return s.format
}

// Namespace defines a namespace at the root of the schema.
// Any problem with it is reported by Validate and Check.
func (s *Schema) Namespace(name string, opts ...NamespaceOption) *Namespace {
	return s.root.Sub(name, opts...)
}

// Namespaces returns all the namespaces of the schema, parents before their children.
func (s *Schema) Namespaces() []*Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()

	var all []*Namespace
	var walk func(ns *Namespace)
	walk = func(ns *Namespace) {
		for _, child := range ns.children {
			all = append(all, child)
			walk(child)
		}
	}
	walk(s.root)
	return all
}

// Definitions returns everything that is defined in the schema.
func (s *Schema) Definitions() []*Definition {
	var defs []*Definition
	for _, ns := range s.Namespaces() {
		if def := ns.Definition(); def != nil {
			defs = append(defs, def)
		}
	}
	return defs
}

// fail records a problem with a definition. The schema must be locked.
func (s *Schema) fail(format string, args ...any) {
	s.errs = append(s.errs, fmt.Errorf("schema: "+format, args...))
}

// Validate reports the problems of the schema that can be found without a database:
// namespaces that collide and definitions that say that they are not complete.
func (s *Schema) Validate() error {
	s.mu.Lock()
	errs := append([]error(nil), s.errs...)
	s.mu.Unlock()

	for _, def := range s.Definitions() {
		if def.Validate == nil {
			continue
		}
		if err := def.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Check validates the schema and compares it with the database. A definition that the database
// has not seen is recorded. A definition that is not the one that the database has recorded,
// for example because the codec of a store was replaced, is reported with ErrChanged, as the
// data that is already there was written by the other one.
// It is meant to be called once when a program starts, after everything is defined.
func (s *Schema) Check(db kv.DB) error {
	if err := s.Validate(); err != nil {
		return err
	}

	var errs []error
	err := db.Update(func(tx kv.Tx) error {
		errs = nil
		meta := s.meta.Open(tx)
		for _, def := range s.Definitions() {
			key := append([]byte("f/"), def.ns.Path()...)
			want := def.fingerprint()

			got, err := meta.Get(key)
			switch {
			case errors.Is(err, kv.ErrNotFound):
				if err := meta.Set(key, want); err != nil {
					return err
				}
			case err != nil:
				return err
			case !bytes.Equal(got, want):
				errs = append(errs, fmt.Errorf("%w: %s %q", ErrChanged, def.Kind, def.ns.Path()))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return errors.Join(errs...)
}
