// Package schema describes what is defined in a database: the namespaces that divide its
// keys and the stores, indexes and the like that live in them. It holds no data.
package schema

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ehsanranjbar/kvutil/codec"
)

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
	retired   []string
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
	retired := append([]string(nil), s.retired...)
	s.mu.Unlock()

	for _, def := range s.Definitions() {
		// What is retired is not what is defined.
		path := def.ns.Path()
		for _, r := range retired {
			if path == r || strings.HasPrefix(path, r+"/") {
				errs = append(errs, fmt.Errorf("schema: %q is retired and defined", path))
			}
		}

		if def.Validate == nil {
			continue
		}
		if err := def.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
