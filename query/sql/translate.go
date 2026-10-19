package sql

import (
	"encoding/hex"
	"fmt"

	rsql "github.com/rqlite/sql"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

// translate turns an expression that stands where a condition is expected into a condition.
func (c *core[V]) translate(e rsql.Expr) (cond.Cond[V], error) {
	switch e := e.(type) {
	case *rsql.ParenExpr:
		return c.translate(e.X)

	case *rsql.BoolLit:
		return constant[V](e.Value), nil

	case *rsql.UnaryExpr:
		if e.Op != rsql.NOT {
			return c.evaluate(e)
		}
		inner, err := c.translate(e.X)
		if err != nil {
			return nil, err
		}
		return cond.Not(inner), nil

	case *rsql.BinaryExpr:
		return c.translateBinary(e)

	case *rsql.Null:
		return c.translateNull(e)

	case *rsql.Ident, *rsql.QualifiedRef:
		// A field that is true or false is a condition by itself.
		if path, ok := reference(e); ok {
			col, err := c.column(path)
			if err != nil {
				return nil, err
			}
			if col.field != nil && col.kind == kindBool {
				return comparison(col.field.EqAny(true))
			}
		}
	}
	return c.evaluate(e)
}

// constant returns the condition that every record satisfies, or that none does.
func constant[V any](b bool) cond.Cond[V] {
	if b {
		return cond.And[V]()
	}
	return cond.Or[V]()
}

// comparison returns a comparison as a condition, with what was wrong with its values as an
// error of the text.
func comparison[V any](c *cond.Comparison[V], err error) (cond.Cond[V], error) {
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrType, err)
	}
	return c, nil
}

// join returns the conditions joined by AND or by OR. The conditions that are joined by the same
// themselves are taken apart, so that a AND b AND c is one condition of three.
func join[V any](and bool, conds ...cond.Cond[V]) cond.Cond[V] {
	var flat []cond.Cond[V]
	for _, c := range conds {
		if and {
			if conj, ok := c.(*cond.Conjunction[V]); ok && len(conj.Conds) > 0 {
				flat = append(flat, conj.Conds...)
				continue
			}
		} else if disj, ok := c.(*cond.Disjunction[V]); ok && len(disj.Conds) > 0 {
			flat = append(flat, disj.Conds...)
			continue
		}
		flat = append(flat, c)
	}
	if and {
		return cond.And(flat...)
	}
	return cond.Or(flat...)
}

func (c *core[V]) translateBinary(e *rsql.BinaryExpr) (cond.Cond[V], error) {
	switch e.Op {
	case rsql.AND, rsql.OR:
		a, err := c.translate(e.X)
		if err != nil {
			return nil, err
		}
		b, err := c.translate(e.Y)
		if err != nil {
			return nil, err
		}
		return join(e.Op == rsql.AND, a, b), nil

	case rsql.EQ, rsql.NE, rsql.LT, rsql.LE, rsql.GT, rsql.GE, rsql.IS, rsql.ISNOT:
		return c.translateComparison(e)

	case rsql.IN, rsql.NOTIN:
		return c.translateIn(e)

	case rsql.BETWEEN, rsql.NOTBETWEEN:
		return c.translateBetween(e)
	}
	return c.evaluate(e)
}

// reference returns the path of a field, if an expression names one.
func reference(e rsql.Expr) (string, bool) {
	switch e := e.(type) {
	case *rsql.Ident:
		return e.Name, true
	case *rsql.QualifiedRef:
		return referencePath(e)
	case *rsql.ParenExpr:
		return reference(e.X)
	}
	return "", false
}

// operandPath returns the path of the field that an expression stands for in a comparison: the
// field that it names, or else the field of a path that is the expression itself, in the text
// that the parser prints for it, like lower("Author"). Such a field is one that the parser was
// given, or that an index of the store is defined on (see Expr), and an index can look the
// records up by it. An expression that no field is for is not one, and is evaluated.
func (c *core[V]) operandPath(e rsql.Expr) (string, bool) {
	if path, ok := reference(e); ok {
		return path, true
	}
	e = unparenthesised(e)
	switch e.(type) {
	case *rsql.Call, *rsql.BinaryExpr, *rsql.UnaryExpr, *rsql.CaseExpr:
		text := show(e)
		if col, ok := c.known(text); ok && col.field != nil {
			return text, true
		}
	}
	return "", false
}

// unparenthesised returns an expression without the parentheses around the whole of it.
func unparenthesised(e rsql.Expr) rsql.Expr {
	for {
		p, ok := e.(*rsql.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}

// literal returns the value of an expression that is a value as it is written: a string, a
// number, TRUE or FALSE, or a blob.
func literal(e rsql.Expr) (any, bool) {
	switch e := e.(type) {
	case *rsql.StringLit:
		return e.Value, true

	case *rsql.BoolLit:
		return e.Value, true

	case *rsql.BlobLit:
		bz, err := hex.DecodeString(e.Value)
		return bz, err == nil

	case *rsql.ParenExpr:
		return literal(e.X)

	case *rsql.NumberLit, *rsql.UnaryExpr:
		value, ok := numberLiteral(e)
		return value, ok
	}
	return nil, false
}

// translateComparison translates a comparison of two things. The comparison of a field with a
// value is a comparison that an index can serve, on whichever side the field is, and the
// comparison of anything else is evaluated.
func (c *core[V]) translateComparison(e *rsql.BinaryExpr) (cond.Cond[V], error) {
	op := e.Op
	path, isField := c.operandPath(e.X)
	value, isValue := literal(e.Y)
	if !isField || !isValue {
		// The value may be on the left, which turns the comparison around.
		if path, isField = c.operandPath(e.Y); isField {
			if value, isValue = literal(e.X); isValue {
				op = turned(op)
			}
		}
	}
	if !isField || !isValue {
		_, nullX := e.X.(*rsql.NullLit)
		_, nullY := e.Y.(*rsql.NullLit)
		if (nullX || nullY) && op != rsql.IS && op != rsql.ISNOT {
			return nil, fmt.Errorf("%w: %s: nothing is equal to NULL; IS NULL tells whether a field has a value",
				ErrType, show(e))
		}
		return c.evaluate(e)
	}

	f, err := c.comparableField(path)
	if err != nil {
		return nil, err
	}
	switch op {
	case rsql.EQ, rsql.IS:
		return comparison(f.EqAny(value))
	case rsql.NE, rsql.ISNOT:
		eq, err := comparison(f.EqAny(value))
		if err != nil {
			return nil, err
		}
		return cond.Not(eq), nil
	case rsql.GT:
		return comparison(f.RangeAny(kvutil.After(value)))
	case rsql.GE:
		return comparison(f.RangeAny(kvutil.From(value)))
	case rsql.LT:
		return comparison(f.RangeAny(kvutil.Until(value)))
	}
	return comparison(f.RangeAny(kvutil.Through(value)))
}

// turned returns the operator of a comparison with its sides exchanged.
func turned(op rsql.Token) rsql.Token {
	switch op {
	case rsql.GT:
		return rsql.LT
	case rsql.GE:
		return rsql.LE
	case rsql.LT:
		return rsql.GT
	case rsql.LE:
		return rsql.GE
	}
	return op
}

// translateIn translates a field being one of several values, and a value being one of the
// values of a field.
func (c *core[V]) translateIn(e *rsql.BinaryExpr) (cond.Cond[V], error) {
	negate := e.Op == rsql.NOTIN

	if path, ok := c.operandPath(e.X); ok {
		if list, ok := e.Y.(*rsql.ExprList); ok {
			if values, ok := literals(list); ok {
				f, err := c.comparableField(path)
				if err != nil {
					return nil, err
				}
				if len(values) == 0 {
					// Nothing is one of none.
					return constant[V](negate), nil
				}
				in, err := comparison(f.InAny(values...))
				if err != nil {
					return nil, err
				}
				return negated(in, negate), nil
			}
		}
	}

	// A value that is one of the several values of a field.
	if value, ok := literal(e.X); ok {
		if path, ok := reference(e.Y); ok {
			col, err := c.column(path)
			if err != nil {
				return nil, err
			}
			if col.field != nil && col.multi {
				eq, err := comparison(col.field.EqAny(value))
				if err != nil {
					return nil, err
				}
				return negated(eq, negate), nil
			}
		}
	}
	return c.evaluate(e)
}

func negated[V any](c cond.Cond[V], negate bool) cond.Cond[V] {
	if negate {
		return cond.Not(c)
	}
	return c
}

// literals returns the values of a list of values as they are written.
func literals(list *rsql.ExprList) ([]any, bool) {
	values := make([]any, 0, len(list.Exprs))
	for _, e := range list.Exprs {
		value, ok := literal(e)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

// translateBetween translates a BETWEEN low AND high of values, which has both of its bounds.
func (c *core[V]) translateBetween(e *rsql.BinaryExpr) (cond.Cond[V], error) {
	if path, ok := c.operandPath(e.X); ok {
		if r, ok := e.Y.(*rsql.Range); ok {
			low, isLow := literal(r.X)
			high, isHigh := literal(r.Y)
			if isLow && isHigh {
				f, err := c.comparableField(path)
				if err != nil {
					return nil, err
				}
				between, err := comparison(f.RangeAny(kvutil.Between(low, high)))
				if err != nil {
					return nil, err
				}
				return negated(between, e.Op == rsql.NOTBETWEEN), nil
			}
		}
	}
	return c.evaluate(e)
}

// translateNull translates whether a field has a value.
func (c *core[V]) translateNull(e *rsql.Null) (cond.Cond[V], error) {
	if path, ok := reference(e.X); ok {
		col, err := c.column(path)
		if err != nil {
			return nil, err
		}
		missing := e.Op == rsql.ISNULL
		text := show(e.X) + " IS NOT NULL"
		if missing {
			text = show(e.X) + " IS NULL"
		}
		return &exists[V]{col: col, text: text, missing: missing}, nil
	}
	return c.evaluate(e)
}

// comparableField returns the field that a path names, for a comparison with values.
func (c *core[V]) comparableField(path string) (field.Untyped[V], error) {
	col, err := c.column(path)
	if err != nil {
		return nil, err
	}
	if col.field == nil {
		return nil, fmt.Errorf("%w: %s cannot be compared with a value: %v", ErrType, path, col.reason)
	}
	return col.field, nil
}

// exists is the condition of a record having a value for a field, or not having one: a pointer
// that is not nil, and one value at least for a field of several values. A string or a number
// always is a value, whatever it is.
type exists[V any] struct {
	col     *column[V]
	text    string
	missing bool
}

// Match implements the cond.Cond interface.
func (e *exists[V]) Match(v *V) (bool, error) {
	return absent(e.col.value(v)) == e.missing, nil
}

// String implements the cond.Cond interface.
func (e *exists[V]) String() string {
	return e.text
}
