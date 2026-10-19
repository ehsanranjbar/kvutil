// Package sql turns the WHERE clause of a query, in the SQL of SQLite, into a condition on the
// records of a store, and runs it.
//
//	Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%Dune%'
//
// What compares a field with values becomes a comparison of package cond, which an index can
// look records up by: =, ==, !=, <>, <, <=, >, >=, IN and BETWEEN with values, joined by AND, OR
// and NOT. A field that is true or false is a condition by itself, and IS NULL and IS NOT NULL
// tell whether a record has a value for a field. Everything else that is true or false, like
// LIKE and GLOB, a function, arithmetic or a comparison of two fields, is evaluated on each
// record by an evaluator of this package. Which of the two a part of the text becomes only
// depends on how it is written, never on the indexes that there are.
//
// # Quotes
//
// In the SQL of SQLite single quotes make a string and double quotes make a name:
//
//	Author = 'Herbert'    // the field Author is equal to the text Herbert
//	Author = "Herbert"    // the field Author is equal to the field Herbert
//
// so the second is an unknown field when the records have no field Herbert. Backquotes and
// brackets make a name too. The text is parsed by github.com/rqlite/sql, which is the only
// package of the module that needs it.
//
// # Fields
//
// The fields are named as they are in Go, with the case that they have, and with dots for the
// fields of fields: Publisher.City, "Publisher"."City" and "Publisher.City" are the same. The
// parser reads at most three names joined by dots, but a name in quotes may have any number of
// dots in it: "A.B.C.D". A name that SQL reserves, like "Order" or "Index", goes in quotes.
// Where a name is looked for, the first of these has it:
//
//  1. the fields given with WithFields;
//  2. the fields of the indexes of the store, if the parser is of a store, which may be
//     computed by a function; the first index that has a field gives it;
//  3. the fields of the struct, found by reflection as field.Reflect does.
//
// The fields of the indexes are the ones that the store has when the first text is parsed.
// A field of a type that has no key codec, like a map or a struct, can be used in what is
// evaluated, but it cannot be compared with a value of the text.
//
// # Values
//
// A value of the text is a string in single quotes, a number (an integer is read from its
// digits, so it keeps its value up to 18446744073709551615), TRUE, FALSE or, for a field that
// is compared with it, a blob like x'ab12'. It is converted to the type of the field it is
// compared with as field.Convert does, so Year = '1965' and Year = 1965.0 are fine for an int,
// and Year = 'abc', Year = 1965.5 and a Rating of 300 for a uint8 are errors of the text.
// Nothing is equal to NULL, so Author = NULL is an error that says to use IS NULL.
//
// # Two values
//
// Conditions are true or false, like the ones of package cond, and not true, false or unknown
// like in SQL: a comparison with a value that a record does not have is false, and NOT makes it
// true. So Subtitle != 'x' is satisfied by a record that has no Subtitle, as NOT (Subtitle = 'x')
// is; the same goes for NOT IN, NOT BETWEEN, NOT LIKE and NOT GLOB. A field of several values
// satisfies a comparison, LIKE, GLOB, IN or BETWEEN if one of its values does, and a function or
// an operator of numbers or text that is given one is applied to each of its values.
//
// # Evaluated
//
// The evaluator works with numbers (integers stay integers, and a division by zero has no
// value), text, true and false, bytes, times, and no value. It has the operators + - * / % and
// || (text), the comparisons, IS and IS NOT, IN with a list or with a field of several values,
// BETWEEN, LIKE with % and _ (ASCII letters in either case, with ESCAPE), GLOB with * and ?,
// CASE, and the functions lower, upper, length, abs, substr, instr, trim, ltrim, rtrim, round,
// coalesce and ifnull, with the meanings that they have in SQLite, but for lower and upper,
// which change all letters and not only those of ASCII. The length of a field of several values
// is the number of its values. A function that is given the name of a function there is not is
// ErrUnknownFunction, and one that is given the wrong number or kinds of arguments is ErrType. What it does not have, like subqueries, CAST,
// COLLATE, MATCH, REGEXP, bit operators and parameters, is a syntax error.
//
// The types of what is evaluated are checked when the text is parsed: the ones that a text can
// be known to have, which are bool, number, text, bytes and time, and any for a field of
// an interface type. A text that has something that is not true or false where a condition is
// expected (Author, 5, Year + 1, lower(Author)), or that does arithmetic on text, is an
// error then. What can only be known on a record, like a field of an interface type that is not
// true or false where it is expected to be, is not true for it, or is ErrEvaluation from Match
// if it is the whole of a condition.
package sql

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	rsql "github.com/rqlite/sql"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

var (
	// ErrSyntax is returned for a text that is not a condition as it is written, or that uses
	// what this package does not have.
	ErrSyntax = errors.New("sql: syntax error")
	// ErrUnknownField is returned for a text that names a field that the records do not have.
	ErrUnknownField = errors.New("sql: unknown field")
	// ErrUnknownFunction is returned for a text that calls a function that there is not.
	ErrUnknownFunction = errors.New("sql: unknown function")
	// ErrType is returned for a text that has something of a type that its place cannot have:
	// a value that the field that it is compared with cannot have, a function that is given
	// the wrong number or types of arguments, or something that is not true or false where a
	// condition is expected.
	ErrType = errors.New("sql: type mismatch")
	// ErrEvaluation is returned by the Match of a condition for a part of the text that is
	// evaluated and that is not true or false for a record, which can only be the case for a
	// field of an interface type.
	ErrEvaluation = errors.New("sql: evaluation failed")
)

// Store is a store whose records can be queried: one that indexes can be defined on. It is
// implemented by the stores of the packages ext and record.
type Store[K, V any] interface {
	extstore.Extensible[K, V]
	// Extensions returns the extensions of the store, which the indexes are among.
	Extensions() []any
}

// Option is an option of a parser.
type Option[V any] func(*core[V])

// WithFields gives the parser fields that it cannot find itself, which are the ones that are
// computed by a function and that no index is defined on. A field that is given is used
// instead of the one of the same path that the parser would find.
func WithFields[V any](fields ...field.Untyped[V]) Option[V] {
	return func(c *core[V]) {
		for _, f := range fields {
			c.columns[f.Path()] = fieldColumn(f)
		}
	}
}

// ParseWhere parses a WHERE clause, without the word WHERE, for records that are not in a
// store. The fields are found in the struct V by reflection, as field.Reflect does, and in the
// ones that WithFields gives.
func ParseWhere[V any](where string, opts ...Option[V]) (cond.Cond[V], error) {
	return newCore(nil, opts).parse(where)
}

// core is what finds the fields of the records and translates a text, for a store or without one.
type core[V any] struct {
	// store is where the indexed fields are found, when a text is first parsed.
	store interface{ Extensions() []any }

	mu      sync.Mutex
	loaded  bool
	columns map[string]*column[V]
}

func newCore[V any](store interface{ Extensions() []any }, opts []Option[V]) *core[V] {
	c := &core[V]{store: store, columns: map[string]*column[V]{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// parse parses a text into a condition.
func (c *core[V]) parse(where string) (cond.Cond[V], error) {
	expr, err := parseExpr(where)
	if err != nil {
		return nil, err
	}
	return c.translate(expr)
}

// parseExpr parses a text as one expression, and fails if there is anything after it.
func parseExpr(where string) (expr rsql.Expr, err error) {
	defer func() {
		// The parser asserts what it relies on. A text must not be able to take the program down.
		if r := recover(); r != nil {
			expr, err = nil, fmt.Errorf("%w: %v", ErrSyntax, r)
		}
	}()

	p := rsql.NewParser(strings.NewReader(where))
	expr, err = p.ParseExpr()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSyntax, err)
	}

	// The parser stops at what it cannot continue the expression with, like a parenthesis that
	// closes nothing, and gives what it has. It has read that token, so the statement that it
	// is asked for next starts with it: there is nothing left of the text only if there is
	// none. Whatever else it finds is something that does not belong to the condition.
	_, err = p.ParseStatement()
	if errors.Is(err, io.EOF) {
		return expr, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: unexpected text after %s: %v", ErrSyntax, expr, err)
	}
	return nil, fmt.Errorf("%w: unexpected text after %s", ErrSyntax, expr)
}

// column is a field of the records as a text names it.
type column[V any] struct {
	path string
	// field is the field as conditions are written with it. It is nil for a field of a type
	// that has no key codec, which can be evaluated but not compared: reason says why.
	field  field.Untyped[V]
	reason error
	// kind is the type of the values of the field, as far as it is known.
	kind kind
	// multi is set if a record may have several values for the field.
	multi bool
	// value returns the value of the field in a record for the evaluator: no value (nil), a
	// value, or a list of values for a field of several.
	value func(v *V) any
}

func fieldColumn[V any](f field.Untyped[V]) *column[V] {
	col := &column[V]{path: f.Path(), field: f, kind: kindOfType(f.Type()), multi: f.Multi()}
	if f.Multi() {
		col.value = func(v *V) any {
			values := f.AnyValues(v)
			l := make(list, 0, len(values))
			for _, value := range values {
				if x := plain(reflect.ValueOf(value)); x != nil {
					l = append(l, x)
				}
			}
			return l
		}
	} else {
		col.value = func(v *V) any {
			values := f.AnyValues(v)
			if len(values) == 0 {
				return nil
			}
			return plain(reflect.ValueOf(values[0]))
		}
	}
	return col
}

// column returns the field of a path: the one that was given to the parser, or the one of an
// index of the store, or the one that the struct has.
func (c *core[V]) column(path string) (*column[V], error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.loaded {
		c.loaded = true
		c.loadIndexed()
	}
	if col, ok := c.columns[path]; ok {
		return col, nil
	}

	get, t, err := field.Accessor[V](path)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %v", ErrUnknownField, path, err)
	}

	var col *column[V]
	if f, err := field.Reflect[V](path); err == nil {
		col = fieldColumn[V](f)
	} else {
		col = &column[V]{
			path:   path,
			reason: err,
			kind:   kindOfType(t),
			multi:  t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8,
			value: func(v *V) any {
				rv, ok := get(v)
				if !ok {
					return nil
				}
				return plain(rv)
			},
		}
	}
	c.columns[path] = col
	return col, nil
}

// loadIndexed adds the fields that the indexes of the store are defined on, which are found
// through the extensions of the store. The first index that has a field gives it.
func (c *core[V]) loadIndexed() {
	if c.store == nil {
		return
	}
	for _, ext := range c.store.Extensions() {
		x, ok := ext.(interface{ Fields() []field.Component[V] })
		if !ok {
			continue
		}
		for _, part := range x.Fields() {
			f, ok := part.(field.Untyped[V])
			if !ok {
				continue
			}
			if _, ok := c.columns[f.Path()]; !ok {
				c.columns[f.Path()] = fieldColumn(f)
			}
		}
	}
}
