package sql

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	rsql "github.com/rqlite/sql"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
)

// Expr defines a field whose value is an SQL expression of a record, like lower(Author), to
// index records by. The path of the field is the expression in a canonical text: the one that
// the parser prints for it, with the names of the functions in lower case, so lower(Author),
// LOWER("Author") and lower( Author ) are all lower("Author"). A query that compares an
// expression of that text with values is served by an index on the field (see the package
// documentation), and the parsers of the stores that the index is defined on find the field.
//
//	byAuthor := sql.Expr[Book, string](`lower(Author)`)
//	index.On(books, "by_author", byAuthor)
//	sql.For(books).Explain(`LOWER(Author) = 'herbert'`) // IndexLookup by_author: 1 range
//
// The values of the field are those of the expression: no value if it has none for a record
// (a field it names that is a nil pointer, NULL, a function that has no value for its
// arguments), one value, or several if it names a field of several values (then the field is
// Multi). They are converted to F as field.Convert does; the kinds of the expression and of F
// must agree (a number for a number, a text for a string, and so on). The field is not Always.
//
// A record whose expression cannot be evaluated or converted is not left out of the index: setting
// it fails, with the error (see field.Computed). The functions that the expression calls must
// be deterministic; give the field functions with WithFunctions, and see Function for versions.
// An index on the field is identified by the canonical text, and by the versions of the
// functions that the expression calls, so it is built again when either changes.
//
// It panics on a text that does not parse, that is not an expression of a type that F converts
// from, or that names a field or a function that there is not, as these are mistakes of the
// definition of the program.
func Expr[V, F any](text string, opts ...Option[V]) field.Field[V, F] {
	f, err := newExpression[V, F](text, opts)
	if err != nil {
		panic(fmt.Sprintf("sql.Expr(%q): %v", text, err))
	}
	return f
}

func newExpression[V, F any](text string, opts []Option[V]) (field.Field[V, F], error) {
	var none field.Field[V, F]

	c := newCore(nil, opts)
	e, err := parseExpr(text)
	if err != nil {
		return none, err
	}
	n, err := c.compile(e)
	if err != nil {
		return none, err
	}

	to := reflect.TypeFor[F]()
	if n.kind == kindNull || (n.kind != kindAny && n.kind != kindOfType(to)) {
		return none, fmt.Errorf("%w: the expression is %s, which cannot be converted to a %s", ErrType, n.kind, to)
	}

	path := show(unparenthesised(e))
	scan := c.scan(e)

	eval := n.eval
	get := func(v *V) (values []F, err error) {
		defer recoverFailure(&err)

		values, err = convertValues[F](eval(v), to)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrEvaluation, path, err)
		}
		return values, nil
	}

	var f field.Field[V, F]
	if scan.multi {
		f = field.ComputedEach(path, get)
	} else {
		f = field.Computed(path, get)
	}
	if version := scan.version; version != "" {
		f = f.WithVersion(version)
	}
	return f, nil
}

// convertValues converts the value of an expression, which is none, a value or a list of them,
// to the values of a field.
func convertValues[F any](x any, to reflect.Type) ([]F, error) {
	one := func(x any) (F, error) {
		var converted F
		c, err := field.Convert(x, to)
		if err != nil {
			return converted, err
		}
		return c.(F), nil
	}

	switch x := x.(type) {
	case nil:
		return nil, nil
	case list:
		values := make([]F, 0, len(x))
		for _, item := range x {
			if item == nil {
				continue
			}
			value, err := one(item)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}
	value, err := one(x)
	if err != nil {
		return nil, err
	}
	return []F{value}, nil
}

// Where is the index option that keeps only the records that satisfy an SQL condition, as
// index.Where does for a condition written in Go:
//
//	index.On(books, "in_print_by_year", year, sql.Where[Book](`InPrint AND Year >= 1900`))
//
// The text is translated as the text of a query is, to comparisons that queries can be matched
// against and to conditions that are evaluated. A query reads the index only if its conditions
// imply the condition of the index (see the package documentation of query): the comparisons
// by implication, anything else if the query has the identical text, which is the same
// canonical text that Expr has. The index is identified by that text, and by the versions of
// the functions that the text calls, so it is built again when either changes. The functions
// must be deterministic. An error while checking the condition for a record fails the
// setting of the record.
//
// The fields of the text are the ones of the struct, found by reflection, and the ones that
// WithFields gives; functions come from WithFunctions. It panics on a text that does not
// parse, as that is a mistake of the definition of the program.
func Where[V any](text string, opts ...Option[V]) index.Option[V] {
	c := newCore(nil, opts)
	e, err := parseExpr(text)
	var translated cond.Cond[V]
	if err == nil {
		translated, err = c.translate(e)
	}
	if err != nil {
		panic(fmt.Sprintf("sql.Where(%q): %v", text, err))
	}

	where := index.Where(translated)
	version := c.scan(e).version
	if version == "" {
		return where
	}
	versioned := index.Versioned[V](version)
	return func(indexer index.Indexer[V]) index.Indexer[V] {
		return versioned(where(indexer))
	}
}

// scanned is what is found in an expression.
type scanned struct {
	// multi is set if it names a field of several values.
	multi bool
	// version is the versions of the functions that it calls that have one: name@version,
	// sorted, with commas between them.
	version string
}

// scan finds what matters in a text that has been compiled: the fields that it names and the
// functions that it calls.
func (c *core[V]) scan(e rsql.Expr) scanned {
	s := &scanner{skip: map[*rsql.Ident]struct{}{}}
	if _, err := rsql.Walk(s, e); err != nil {
		// The text was compiled, so it is made of what is known.
		panic(err)
	}

	var found scanned
	for _, path := range s.refs {
		if col, err := c.column(path); err == nil && col.multi {
			found.multi = true
		}
	}
	var versions []string
	for _, name := range s.calls {
		if f, ok := c.functions.byName[name]; ok && f.version != "" {
			versions = append(versions, f.name+"@"+f.version)
		}
	}
	slices.Sort(versions)
	found.version = strings.Join(slices.Compact(versions), ",")
	return found
}

// scanner is a visitor that collects the paths of the fields of an expression and the names of
// the functions that it calls.
type scanner struct {
	refs, calls []string
	// skip are the names that are not of fields, which are parts of other nodes.
	skip map[*rsql.Ident]struct{}
}

func (s *scanner) Visit(n rsql.Node) (rsql.Visitor, rsql.Node, error) {
	switch n := n.(type) {
	case *rsql.Call:
		s.calls = append(s.calls, strings.ToLower(n.Name.Name))
		s.skip[n.Name] = struct{}{}
	case *rsql.QualifiedRef:
		if path, ok := referencePath(n); ok {
			s.refs = append(s.refs, path)
		}
		// The names in it are the parts of the path.
		return nil, n, nil
	case *rsql.Ident:
		if _, ok := s.skip[n]; !ok {
			s.refs = append(s.refs, n.Name)
		}
	}
	return s, n, nil
}

func (s *scanner) VisitEnd(n rsql.Node) (rsql.Node, error) { return n, nil }
