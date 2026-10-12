package ql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/lex"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
)

// translate turns a node that stands where a condition is expected into a condition.
func (p *Parser[V]) translate(node expr.Node) (cond.Cond[V], error) {
	switch n := node.(type) {
	case *expr.BinaryNode:
		return p.translateBinary(n)

	case *expr.BooleanNode:
		conds := make([]cond.Cond[V], 0, len(n.Args))
		for _, arg := range n.Args {
			c, err := p.translate(arg)
			if err != nil {
				return nil, err
			}
			conds = append(conds, c)
		}
		c, err := join(n.Operator.T, conds)
		if err != nil {
			return nil, err
		}
		return negated(c, n.Negated()), nil

	case *expr.TriNode:
		if n.Operator.T != lex.TokenBetween || len(n.Args) != 3 {
			return nil, fmt.Errorf("%w: %s is not supported", ErrSyntax, n.Operator.V)
		}
		c, err := p.translateBetween(n.Args[0], n.Args[1], n.Args[2])
		if err != nil {
			return nil, err
		}
		return negated(c, n.Negated()), nil

	case *expr.UnaryNode:
		if n.Operator.T == lex.TokenNegate {
			c, err := p.translate(n.Arg)
			if err != nil {
				return nil, err
			}
			return cond.Not(c), nil
		}
		if n.Operator.T == lex.TokenExists {
			// Whether a record has a value for a field is not left to the evaluator, which
			// takes an empty text for no value.
			if id, ok := identity(n.Arg); ok {
				col, err := p.column(id.Text)
				if err != nil {
					return nil, err
				}
				return &exists[V]{col: col}, nil
			}
			return p.evaluate(n)
		}
		return nil, notACondition(node)

	case *expr.IdentityNode:
		if b, ok := boolean(n); ok {
			return constant[V](b), nil
		}
		// A field that is true or false is a condition by itself.
		col, err := p.column(n.Text)
		if err != nil {
			return nil, err
		}
		if col.field == nil || col.field.Type().Kind() != reflect.Bool {
			return nil, notACondition(node)
		}
		return comparison(col.field.EqAny(true))

	case *expr.FuncNode:
		return p.evaluate(n)
	}
	return nil, notACondition(node)
}

func notACondition(node expr.Node) error {
	return fmt.Errorf("%w: %s is not a condition", ErrType, node)
}

// constant returns the condition that every record satisfies, or that none does.
func constant[V any](b bool) cond.Cond[V] {
	if b {
		return cond.And[V]()
	}
	return cond.Or[V]()
}

func negated[V any](c cond.Cond[V], negated bool) cond.Cond[V] {
	if negated {
		return cond.Not(c)
	}
	return c
}

// comparison returns a comparison as a condition, with what was wrong with its values as an
// error of the text.
func comparison[V any](c *cond.Comparison[V], err error) (cond.Cond[V], error) {
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrType, err)
	}
	return c, nil
}

// join returns the conditions joined by AND or by OR. The conditions that are joined by the
// same themselves are taken apart, so that a AND b AND c is one condition of three.
func join[V any](op lex.TokenType, conds []cond.Cond[V]) (cond.Cond[V], error) {
	var flat []cond.Cond[V]
	switch op {
	case lex.TokenLogicAnd, lex.TokenAnd:
		for _, c := range conds {
			if and, ok := c.(*cond.Conjunction[V]); ok && len(and.Conds) > 0 {
				flat = append(flat, and.Conds...)
				continue
			}
			flat = append(flat, c)
		}
		return cond.And(flat...), nil

	case lex.TokenLogicOr, lex.TokenOr:
		for _, c := range conds {
			if or, ok := c.(*cond.Disjunction[V]); ok && len(or.Conds) > 0 {
				flat = append(flat, or.Conds...)
				continue
			}
			flat = append(flat, c)
		}
		return cond.Or(flat...), nil
	}
	return nil, fmt.Errorf("%w: conditions cannot be joined by %s", ErrSyntax, op)
}

func (p *Parser[V]) translateBinary(n *expr.BinaryNode) (cond.Cond[V], error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("%w: %s", ErrSyntax, n)
	}
	left, right := n.Args[0], n.Args[1]

	switch n.Operator.T {
	case lex.TokenLogicAnd, lex.TokenAnd, lex.TokenLogicOr, lex.TokenOr:
		a, err := p.translate(left)
		if err != nil {
			return nil, err
		}
		b, err := p.translate(right)
		if err != nil {
			return nil, err
		}
		return join(n.Operator.T, []cond.Cond[V]{a, b})

	case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE,
		lex.TokenGT, lex.TokenGE, lex.TokenLT, lex.TokenLE:
		return p.translateComparison(n, left, right)

	case lex.TokenIN:
		// A field that is one of several values.
		if id, ok := identity(left); ok {
			if values, ok := literals(right); ok {
				col, err := p.comparable(id)
				if err != nil {
					return nil, err
				}
				return comparison(col.field.InAny(values...))
			}
		}
		// A value that is one of the several values of a field.
		if value, ok := literal(left); ok {
			if id, ok := identity(right); ok {
				col, err := p.column(id.Text)
				if err != nil {
					return nil, err
				}
				if col.field != nil && col.field.Multi() {
					return comparison(col.field.EqAny(value))
				}
			}
		}
		return p.evaluate(n)

	case lex.TokenPlus, lex.TokenMinus, lex.TokenStar, lex.TokenMultiply, lex.TokenDivide, lex.TokenModulus:
		return nil, notACondition(n)
	}
	// LIKE, CONTAINS, INTERSECTS and whatever else the evaluator knows.
	return p.evaluate(n)
}

// translateComparison translates a comparison of two things. The comparison of a field with a
// value is a comparison that an index can serve, on whichever side the field is, and the
// comparison of anything else is evaluated.
func (p *Parser[V]) translateComparison(n *expr.BinaryNode, left, right expr.Node) (cond.Cond[V], error) {
	op := n.Operator.T
	id, isField := identity(left)
	value, isValue := literal(right)
	if !isField || !isValue {
		// The value may be on the left, which turns the comparison around.
		if id, isField = identity(right); isField {
			if value, isValue = literal(left); isValue {
				op = turned(op)
			}
		}
	}
	if !isField || !isValue {
		if _, ok := left.(*expr.NullNode); ok {
			return nil, comparedWithNull(n)
		}
		if _, ok := right.(*expr.NullNode); ok {
			return nil, comparedWithNull(n)
		}
		return p.evaluate(n)
	}

	col, err := p.comparable(id)
	if err != nil {
		return nil, err
	}
	f := col.field
	switch op {
	case lex.TokenEqual, lex.TokenEqualEqual:
		return comparison(f.EqAny(value))
	case lex.TokenNE:
		c, err := comparison(f.EqAny(value))
		if err != nil {
			return nil, err
		}
		return cond.Not(c), nil
	case lex.TokenGT:
		return comparison(f.RangeAny(kvutil.After(value)))
	case lex.TokenGE:
		return comparison(f.RangeAny(kvutil.From(value)))
	case lex.TokenLT:
		return comparison(f.RangeAny(kvutil.Until(value)))
	default:
		return comparison(f.RangeAny(kvutil.Through(value)))
	}
}

func comparedWithNull(node expr.Node) error {
	return fmt.Errorf("%w: %s: nothing is equal to NULL; EXISTS tells whether a field has a value",
		ErrType, node)
}

// turned returns the operator of a comparison with its sides exchanged.
func turned(op lex.TokenType) lex.TokenType {
	switch op {
	case lex.TokenGT:
		return lex.TokenLT
	case lex.TokenGE:
		return lex.TokenLE
	case lex.TokenLT:
		return lex.TokenGT
	case lex.TokenLE:
		return lex.TokenGE
	}
	return op
}

// translateBetween translates a BETWEEN low AND high, which has both of its bounds: it is
// a >= low AND a <= high, however it is written.
func (p *Parser[V]) translateBetween(arg, low, high expr.Node) (cond.Cond[V], error) {
	if id, ok := identity(arg); ok {
		lowValue, isLow := literal(low)
		highValue, isHigh := literal(high)
		if isLow && isHigh {
			col, err := p.comparable(id)
			if err != nil {
				return nil, err
			}
			return comparison(col.field.RangeAny(kvutil.Between(lowValue, highValue)))
		}
	}

	// The evaluator leaves the bounds out of a BETWEEN, so it is given the two comparisons.
	from, err := p.translate(expr.NewBinaryNode(lex.Token{T: lex.TokenGE, V: ">="}, arg, low))
	if err != nil {
		return nil, err
	}
	to, err := p.translate(expr.NewBinaryNode(lex.Token{T: lex.TokenLE, V: "<="}, arg, high))
	if err != nil {
		return nil, err
	}
	return cond.And(from, to), nil
}

// comparable returns the field that an identity names, for a comparison with values.
func (p *Parser[V]) comparable(id *expr.IdentityNode) (*column[V], error) {
	col, err := p.column(id.Text)
	if err != nil {
		return nil, err
	}
	if col.field == nil {
		return nil, fmt.Errorf("%w: %s cannot be compared with a value: %v", ErrType, id.Text, col.reason)
	}
	return col, nil
}

// identity returns a node as the name of a field, if it is one.
func identity(node expr.Node) (*expr.IdentityNode, bool) {
	id, ok := node.(*expr.IdentityNode)
	if !ok {
		return nil, false
	}
	if _, isBool := boolean(id); isBool {
		return nil, false
	}
	return id, true
}

// boolean returns a node as true or false, if it is one of the two written without quotes.
func boolean(id *expr.IdentityNode) (value, ok bool) {
	if id.Quote != 0 {
		return false, false
	}
	switch strings.ToLower(id.Text) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	return false, false
}

// literal returns the value of a node that is a value as it is written: a text, a number,
// true or false.
func literal(node expr.Node) (any, bool) {
	switch n := node.(type) {
	case *expr.StringNode:
		return n.Text, true

	case *expr.NumberNode:
		return number(n), true

	case *expr.UnaryNode:
		// A negative number is a minus and a number.
		num, ok := n.Arg.(*expr.NumberNode)
		if n.Operator.T != lex.TokenMinus || !ok {
			return nil, false
		}
		switch value := number(num).(type) {
		case int64:
			return -value, true
		case float64:
			return -value, true
		}
		// A number that only an unsigned integer has cannot be negative.
		return -num.Float64, true

	case *expr.IdentityNode:
		if b, ok := boolean(n); ok {
			return b, true
		}
	}
	return nil, false
}

// number returns the value of a number as it is written: an integer for digits alone, which a
// float may not have exactly, and a float for anything else. The integer that the parser makes
// of a number is not used, as it is not the number for one that an int64 does not have.
func number(n *expr.NumberNode) any {
	if i, err := strconv.ParseInt(n.Text, 10, 64); err == nil {
		return i
	}
	if u, err := strconv.ParseUint(n.Text, 10, 64); err == nil {
		return u
	}
	if n.IsFloat {
		return n.Float64
	}
	return n.Int64
}

// literals returns the values of a node that is a list of values.
func literals(node expr.Node) ([]any, bool) {
	array, ok := node.(*expr.ArrayNode)
	if !ok {
		return nil, false
	}
	values := make([]any, 0, len(array.Args))
	for _, arg := range array.Args {
		value, ok := literal(arg)
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

// exists is the condition of a record having a value for a field: a pointer that is not nil,
// and one value at least for a field of several values. A text or a number always is a value,
// whatever it is.
type exists[V any] struct {
	col *column[V]
}

// Match implements the cond.Cond interface.
func (e *exists[V]) Match(v *V) (bool, error) {
	return e.col.has(v), nil
}

// String implements the cond.Cond interface.
func (e *exists[V]) String() string {
	return "EXISTS " + e.col.name
}
