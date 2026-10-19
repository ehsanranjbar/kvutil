// Package ql turns the text of a condition, like the WHERE of a query, into a condition on the
// records of a store.
//
//	Author = "Herbert" AND Year >= 1970 AND contains(Title, "Dune")
//
// What compares a field with values becomes a comparison of package cond, which an index can
// look records up by: =, ==, !=, <>, <, <=, >, >=, IN and BETWEEN, joined by AND, OR and NOT.
// Everything else, like a function, arithmetic or a comparison of two fields, is evaluated on
// each record. Which of the two a part of the text becomes only depends on how it is written,
// never on the indexes that there are.
//
// The fields are named as they are in Go, with dots for the fields of fields: Address.City.
// Text goes in quotes, a field that needs them goes in backquotes.
//
// The text is parsed and evaluated by qlbridge, whose functions can be used. This is the only
// package of the module that needs it. Its limits are the limits of this package:
//
//   - Its lexer never returns for a text that begins with a minus or a slash, or that has a
//     semicolon or a comment between /* and */ outside quotes. Nothing guards against these,
//     so a text that comes from someone who is not trusted must be checked for them first.
//   - A negative number is only read as one on the right of a comparison: Year > -5.
//   - What it cannot evaluate on a record, like a division by zero or the comparison of a
//     number with a text of letters, is not an error: the record does not satisfy the condition.
//   - A function of an empty text has no result, so it is not true either.
package ql

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

var (
	// ErrSyntax is returned for a text that is not a condition as it is written.
	ErrSyntax = errors.New("ql: syntax error")
	// ErrUnknownField is returned for a text that names a field that the records do not have.
	ErrUnknownField = errors.New("ql: unknown field")
	// ErrUnknownFunction is returned for a text that calls a function that there is not.
	ErrUnknownFunction = errors.New("ql: unknown function")
	// ErrType is returned for a text that has something of a type that its place cannot have:
	// a value that the field that it is compared with cannot have, or something that is not
	// true or false where a condition is expected.
	ErrType = errors.New("ql: type mismatch")
	// ErrEvaluation is returned by the Match of a condition for a part of the text that is
	// evaluated and that is not true or false for a record, like a function that returns a
	// number where a condition is expected.
	ErrEvaluation = errors.New("ql: evaluation failed")
)

// Store is a store whose records conditions can be parsed for: one that indexes can be defined
// on. It is implemented by the stores of the packages ext and record.
type Store[K, V any] interface {
	extstore.Extensible[K, V]
	// Extensions returns the extensions of the store, which the indexes are among.
	Extensions() []any
}

// Parser parses the text of conditions on the records of the type V.
// It can be used from several goroutines.
type Parser[V any] struct {
	// store is where the indexed fields are found, when a text is first parsed.
	store interface{ Extensions() []any }

	mu      sync.Mutex
	loaded  bool
	columns map[string]*column[V]
}

// Option is an option of a Parser.
type Option[V any] func(*Parser[V])

// WithFields gives the parser fields that it cannot find itself, which are the ones that are
// computed by a function and that no index is defined on. A field that is given is used
// instead of the one of the same path that the parser would find.
func WithFields[V any](fields ...field.Untyped[V]) Option[V] {
	return func(p *Parser[V]) {
		for _, f := range fields {
			p.columns[f.Path()] = fieldColumn(f)
		}
	}
}

// New returns a parser of conditions on the records of the type V, which is not of a store.
// It finds the fields that a text names in the struct V by reflection, as field.Reflect does.
func New[V any](opts ...Option[V]) *Parser[V] {
	// The functions of qlbridge are registered once for the whole program.
	builtins.LoadAllBuiltins()

	p := &Parser[V]{columns: map[string]*column[V]{}}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// For returns a parser of conditions on the records of a store.
//
// A field that a text names is one of the fields that the indexes of the store are defined on,
// which a comparison must be written with for the index to serve it, and which may be computed
// by a function. If no index has the field, it is found in the struct V by reflection, as
// field.Reflect does. The indexes are the ones that the store has when a text is first parsed.
func For[K, V any](store Store[K, V], opts ...Option[V]) *Parser[V] {
	p := New(opts...)
	p.store = store
	return p
}

// Parse parses the text of a condition. It returns an error for a text that cannot be right
// for any record: one that is not written as a condition, that names a field or a function
// that there is not, or that compares a field with a value that it cannot have.
func (p *Parser[V]) Parse(text string) (cond.Cond[V], error) {
	lexer := lex.NewLexer(text, lex.LogicalExpressionDialect)
	pager := expr.NewLexTokenPager(lexer)
	node, err := expr.ParsePager(pager)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSyntax, err)
	}
	// The parser stops at what it cannot continue with, like a parenthesis that closes
	// nothing, and takes what it has for the whole.
	if rest := pager.Cur(); rest.T != lex.TokenEOF {
		return nil, fmt.Errorf("%w: unexpected %q after %s", ErrSyntax, rest.V, node)
	}
	lexer.SkipWhiteSpaces()
	if !lexer.IsEnd() {
		return nil, fmt.Errorf("%w: unexpected %q after %s", ErrSyntax, lexer.PeekX(20), node)
	}
	return p.translate(node)
}

// column is a field of the records as a text names it.
type column[V any] struct {
	name string
	// field is the field as conditions are written with it. It is nil for a field of a type
	// that has no key codec, which can be evaluated but not compared: reason says why.
	field  field.Untyped[V]
	reason error
	// value returns the value of the field in a record for the evaluator, in a form that
	// qlbridge knows. It reports whether the record has one.
	value func(v *V) (any, bool)
	// has reports whether a record has a value for the field: one at least for a field of
	// several values.
	has func(v *V) bool
}

func fieldColumn[V any](f field.Untyped[V]) *column[V] {
	return &column[V]{
		name:  f.Path(),
		field: f,
		value: func(v *V) (any, bool) {
			values := f.AnyValues(v)
			if f.Multi() {
				return plainSlice(reflect.ValueOf(values)), true
			}
			if len(values) == 0 {
				return nil, false
			}
			return plain(reflect.ValueOf(values[0]))
		},
		has: func(v *V) bool { return len(f.AnyValues(v)) > 0 },
	}
}

// column returns the field of a name: the one that was given to the parser, or the one of an
// index of the store, or the one that the struct has.
func (p *Parser[V]) column(name string) (*column[V], error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.loaded {
		p.loaded = true
		p.loadIndexed()
	}
	if c, ok := p.columns[name]; ok {
		return c, nil
	}

	get, _, err := field.Accessor[V](name)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrUnknownField, name, err)
	}

	var c *column[V]
	if f, err := field.Reflect[V](name); err == nil {
		c = fieldColumn[V](f)
	} else {
		c = &column[V]{
			name:   name,
			reason: err,
			value: func(v *V) (any, bool) {
				rv, ok := get(v)
				if !ok {
					return nil, false
				}
				return plain(rv)
			},
			has: func(v *V) bool {
				rv, ok := get(v)
				if !ok {
					return false
				}
				switch rv.Kind() {
				case reflect.Pointer, reflect.Interface:
					return !rv.IsNil()
				case reflect.Slice, reflect.Map:
					return rv.Len() > 0
				}
				return true
			},
		}
	}
	p.columns[name] = c
	return c, nil
}

// loadIndexed adds the fields that the indexes of the store are defined on, which are found
// through the extensions of the store. The first index that has a field gives it.
func (p *Parser[V]) loadIndexed() {
	if p.store == nil {
		return
	}
	for _, ext := range p.store.Extensions() {
		x, ok := ext.(interface{ Fields() []field.Component[V] })
		if !ok {
			continue
		}
		for _, part := range x.Fields() {
			f, ok := part.(field.Untyped[V])
			if !ok {
				continue
			}
			if _, ok := p.columns[f.Path()]; !ok {
				p.columns[f.Path()] = fieldColumn(f)
			}
		}
	}
}
