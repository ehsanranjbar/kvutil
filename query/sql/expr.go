package sql

import (
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	rsql "github.com/rqlite/sql"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

// node is a part of a text that is evaluated on a record, with the kind of value that it is
// known to have before there is one.
type node[V any] struct {
	kind kind
	eval func(v *V) any
	text string
	// lit is the value of a node that is a value as it is written, which is not nil if lit is set.
	lit   any
	isLit bool
}

func literalNode[V any](k kind, value any, text string) *node[V] {
	return &node[V]{kind: k, eval: func(*V) any { return value }, text: text, lit: value, isLit: true}
}

// evaluated is a condition that is evaluated on every record: the part of a text that is not
// a comparison of a field with values. An index cannot serve it.
type evaluated[V any] struct {
	text string
	eval func(v *V) any
}

// evaluate returns the condition of an expression that is evaluated, after checking what can
// be checked without a record: the fields and the functions that it names, and the types.
func (c *core[V]) evaluate(e rsql.Expr) (cond.Cond[V], error) {
	n, err := c.compile(e)
	if err != nil {
		return nil, err
	}
	if n.kind != kindBool && n.kind != kindAny {
		return nil, notACondition(e, n.kind)
	}
	return &evaluated[V]{text: show(e), eval: n.eval}, nil
}

func notACondition(e rsql.Expr, k kind) error {
	return fmt.Errorf("%w: %s is %s, not true or false", ErrType, show(e), k)
}

// Match implements the cond.Cond interface.
func (e *evaluated[V]) Match(v *V) (ok bool, err error) {
	// A function that fails ends the evaluation with its error.
	defer recoverFailure(&err)

	switch result := e.eval(v).(type) {
	case bool:
		return result, nil
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("%w: %s is %s, not true or false", ErrEvaluation, e.text, describe(result))
	}
}

// String implements the cond.Cond interface.
func (e *evaluated[V]) String() string {
	return e.text
}

// describe returns the kind of a value for an error.
func describe(v any) string {
	switch v.(type) {
	case int64, float64:
		return kindNumber.String()
	case string:
		return kindString.String()
	case []byte:
		return kindBytes.String()
	case time.Time:
		return kindTime.String()
	case list:
		return "a list"
	}
	return "a value that is not true or false"
}

// show returns an expression as the parser prints it.
func show(e rsql.Expr) (s string) {
	defer func() {
		if r := recover(); r != nil {
			s = fmt.Sprintf("%T", e)
		}
	}()
	return e.String()
}

func unsupported(e rsql.Expr, what string) error {
	return fmt.Errorf("%w: %s is not supported: %s", ErrSyntax, what, show(e))
}

// compile turns an expression into a node that evaluates it.
func (c *core[V]) compile(e rsql.Expr) (*node[V], error) {
	switch e := e.(type) {
	case *rsql.Ident:
		return c.reference(e.Name)

	case *rsql.QualifiedRef:
		path, ok := referencePath(e)
		if !ok {
			return nil, unsupported(e, "a star")
		}
		return c.reference(path)

	case *rsql.StringLit:
		return literalNode[V](kindString, e.Value, show(e)), nil

	case *rsql.NumberLit:
		value, err := numberValue(e.Value)
		if err != nil {
			return nil, err
		}
		if u, ok := value.(uint64); ok {
			value = float64(u)
		}
		return literalNode[V](kindNumber, value, show(e)), nil

	case *rsql.BoolLit:
		return literalNode[V](kindBool, e.Value, show(e)), nil

	case *rsql.BlobLit:
		bz, err := hex.DecodeString(e.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrSyntax, show(e), err)
		}
		return literalNode[V](kindBytes, bz, show(e)), nil

	case *rsql.NullLit:
		return &node[V]{kind: kindNull, eval: func(*V) any { return nil }, text: "NULL"}, nil

	case *rsql.ParenExpr:
		return c.compile(e.X)

	case *rsql.UnaryExpr:
		return c.compileUnary(e)

	case *rsql.BinaryExpr:
		return c.compileBinary(e)

	case *rsql.Null:
		x, err := c.compile(e.X)
		if err != nil {
			return nil, err
		}
		want := e.Op == rsql.ISNULL
		return boolNode(show(e), func(v *V) bool { return absent(x.eval(v)) == want }), nil

	case *rsql.Call:
		return c.compileCall(e)

	case *rsql.CaseExpr:
		return c.compileCase(e)

	case *rsql.TimestampLit:
		return nil, unsupported(e, "a time of the clock")
	case *rsql.BindExpr:
		return nil, unsupported(e, "a parameter")
	case *rsql.CastExpr:
		return nil, unsupported(e, "CAST")
	case *rsql.CollateExpr:
		return nil, unsupported(e, "COLLATE")
	case *rsql.ExprList:
		return nil, unsupported(e, "a list of values here")
	}
	return nil, unsupported(e, "this")
}

func boolNode[V any](text string, f func(v *V) bool) *node[V] {
	return &node[V]{kind: kindBool, eval: func(v *V) any { return f(v) }, text: text}
}

// reference returns the node of a field.
func (c *core[V]) reference(path string) (*node[V], error) {
	col, err := c.column(path)
	if err != nil {
		return nil, err
	}
	return &node[V]{kind: col.kind, eval: col.value, text: path}, nil
}

// referencePath returns the path of a field that is written with dots between its names.
func referencePath(r *rsql.QualifiedRef) (string, bool) {
	if r.Star.IsValid() || r.Table == nil || r.Column == nil {
		return "", false
	}
	var parts []string
	if r.Schema != nil {
		parts = append(parts, r.Schema.Name)
	}
	parts = append(parts, r.Table.Name, r.Column.Name)
	return strings.Join(parts, "."), true
}

// numberValue returns the value of a number as it is written: an integer for digits alone, an
// unsigned one if an int64 does not have it, and a float for anything else.
func numberValue(s string) (any, error) {
	if rest, ok := strings.CutPrefix(strings.ToLower(s), "0x"); ok {
		u, err := strconv.ParseUint(rest, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: the number %s is too great", ErrSyntax, s)
		}
		if u > math.MaxInt64 {
			return u, nil
		}
		return int64(u), nil
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, nil
	}
	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		return u, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil && !isRangeError(err) {
		return nil, fmt.Errorf("%w: %s is not a number", ErrSyntax, s)
	}
	return f, nil
}

func isRangeError(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

// negateNumber returns the negative of a number as numberValue returns it.
func negateNumber(value any) any {
	switch x := value.(type) {
	case int64:
		return -x
	case uint64:
		// A number that only an unsigned integer has is not one when it is negative.
		if x == 1<<63 {
			return int64(math.MinInt64)
		}
		f := -float64(x)
		if x > 1<<63 && f >= math.MinInt64 {
			// The float that is nearest rounds to the least integer, which this is not.
			f = math.Nextafter(f, math.Inf(-1))
		}
		return f
	case float64:
		return -x
	}
	return nil
}

func (c *core[V]) compileUnary(e *rsql.UnaryExpr) (*node[V], error) {
	switch e.Op {
	case rsql.MINUS, rsql.PLUS:
		if value, ok := numberLiteral(e); ok {
			if u, ok := value.(uint64); ok {
				value = float64(u)
			}
			return literalNode[V](kindNumber, value, show(e)), nil
		}
		x, err := c.compile(e.X)
		if err != nil {
			return nil, err
		}
		if !numberOnly.has(x.kind) {
			return nil, operandError(e, x, kindNumber)
		}
		minus := e.Op == rsql.MINUS
		return &node[V]{kind: kindNumber, text: show(e), eval: func(v *V) any {
			return lift([]any{x.eval(v)}, func(args []any) any {
				if !minus {
					return args[0]
				}
				switch a := args[0].(type) {
				case int64:
					if a == math.MinInt64 {
						return -float64(a)
					}
					return -a
				case float64:
					return -a
				}
				return nil
			})
		}}, nil

	case rsql.NOT:
		x, err := c.compile(e.X)
		if err != nil {
			return nil, err
		}
		if !conditional(x.kind) {
			return nil, notACondition(e.X, x.kind)
		}
		return boolNode(show(e), func(v *V) bool { return !truth(x.eval(v)) }), nil
	}
	return nil, unsupported(e, "this operator")
}

// numberLiteral returns the value of a number with signs in front of it.
func numberLiteral(e rsql.Expr) (any, bool) {
	switch e := e.(type) {
	case *rsql.NumberLit:
		value, err := numberValue(e.Value)
		return value, err == nil
	case *rsql.ParenExpr:
		return numberLiteral(e.X)
	case *rsql.UnaryExpr:
		value, ok := numberLiteral(e.X)
		if !ok {
			return nil, false
		}
		switch e.Op {
		case rsql.PLUS:
			return value, true
		case rsql.MINUS:
			return negateNumber(value), true
		}
	}
	return nil, false
}

func operandError[V any](e rsql.Expr, operand *node[V], want kind) error {
	return fmt.Errorf("%w: %s is %s, not %s in %s", ErrType, operand.text, operand.kind, want, show(e))
}

// conditional reports whether something of a kind can be true or false.
func conditional(k kind) bool {
	return k == kindBool || k == kindAny
}

func (c *core[V]) compileBinary(e *rsql.BinaryExpr) (*node[V], error) {
	switch e.Op {
	case rsql.AND, rsql.OR:
		x, y, err := c.operands(e.X, e.Y)
		if err != nil {
			return nil, err
		}
		for _, operand := range []struct {
			e rsql.Expr
			n *node[V]
		}{{e.X, x}, {e.Y, y}} {
			if !conditional(operand.n.kind) {
				return nil, notACondition(operand.e, operand.n.kind)
			}
		}
		if e.Op == rsql.AND {
			return boolNode(show(e), func(v *V) bool { return truth(x.eval(v)) && truth(y.eval(v)) }), nil
		}
		return boolNode(show(e), func(v *V) bool { return truth(x.eval(v)) || truth(y.eval(v)) }), nil

	case rsql.PLUS, rsql.MINUS, rsql.STAR, rsql.SLASH, rsql.REM:
		x, y, err := c.operands(e.X, e.Y)
		if err != nil {
			return nil, err
		}
		for _, operand := range []*node[V]{x, y} {
			if !numberOnly.has(operand.kind) {
				return nil, operandError(e, operand, kindNumber)
			}
		}
		op := map[rsql.Token]byte{rsql.PLUS: '+', rsql.MINUS: '-', rsql.STAR: '*', rsql.SLASH: '/', rsql.REM: '%'}[e.Op]
		return &node[V]{kind: kindNumber, text: show(e), eval: func(v *V) any {
			return lift([]any{x.eval(v), y.eval(v)}, func(args []any) any {
				return arithmetic(op, args[0], args[1])
			})
		}}, nil

	case rsql.CONCAT:
		x, y, err := c.operands(e.X, e.Y)
		if err != nil {
			return nil, err
		}
		for _, operand := range []*node[V]{x, y} {
			if !setOf(kindString, kindNumber).has(operand.kind) {
				return nil, operandError(e, operand, kindString)
			}
		}
		return &node[V]{kind: kindString, text: show(e), eval: func(v *V) any {
			return lift([]any{x.eval(v), y.eval(v)}, func(args []any) any {
				return concat(args[0], args[1])
			})
		}}, nil

	case rsql.EQ, rsql.NE, rsql.LT, rsql.LE, rsql.GT, rsql.GE,
		rsql.IS, rsql.ISNOT, rsql.ISDISTINCT, rsql.ISNOTDISTINCT:
		x, y, err := c.operands(e.X, e.Y)
		if err != nil {
			return nil, err
		}
		if x, y, err = coerce(e, x, y); err != nil {
			return nil, err
		}
		test, negate, nullSafe := comparisonOf(e.Op)
		return boolNode(show(e), func(v *V) bool {
			a, b := x.eval(v), y.eval(v)
			if nullSafe && (absent(a) || absent(b)) {
				return (absent(a) && absent(b)) != negate
			}
			return holds(a, b, test) != negate
		}), nil

	case rsql.IN, rsql.NOTIN:
		return c.compileIn(e)

	case rsql.BETWEEN, rsql.NOTBETWEEN:
		r, ok := e.Y.(*rsql.Range)
		if !ok {
			return nil, unsupported(e, "this")
		}
		x, err := c.compile(e.X)
		if err != nil {
			return nil, err
		}
		low, high, err := c.operands(r.X, r.Y)
		if err != nil {
			return nil, err
		}
		if _, low, err = coerce(e, x, low); err != nil {
			return nil, err
		}
		if _, high, err = coerce(e, x, high); err != nil {
			return nil, err
		}
		negate := e.Op == rsql.NOTBETWEEN
		atLeast := func(c int) bool { return c >= 0 }
		atMost := func(c int) bool { return c <= 0 }
		return boolNode(show(e), func(v *V) bool {
			l, h := low.eval(v), high.eval(v)
			return anyOf(x.eval(v), func(a any) bool {
				return holds(a, l, atLeast) && holds(a, h, atMost)
			}) != negate
		}), nil

	case rsql.LIKE, rsql.NOTLIKE, rsql.GLOB, rsql.NOTGLOB:
		return c.compileLike(e)
	}
	return nil, unsupported(e, "this operator")
}

// comparisonOf returns what an operator of comparison is. A comparison is true if the test is
// true of the comparison of one value of each side, and the opposite of that if negate is set.
// The ones that are not true for a value that is not there are the ones of the nullSafe kind.
func comparisonOf(op rsql.Token) (test func(int) bool, negate, nullSafe bool) {
	equal := func(c int) bool { return c == 0 }
	switch op {
	case rsql.EQ:
		return equal, false, false
	case rsql.NE:
		return equal, true, false
	case rsql.LT:
		return func(c int) bool { return c < 0 }, false, false
	case rsql.LE:
		return func(c int) bool { return c <= 0 }, false, false
	case rsql.GT:
		return func(c int) bool { return c > 0 }, false, false
	case rsql.GE:
		return func(c int) bool { return c >= 0 }, false, false
	case rsql.IS, rsql.ISNOTDISTINCT:
		return equal, false, true
	}
	return equal, true, true
}

// operands compiles the two sides of an operator.
func (c *core[V]) operands(x, y rsql.Expr) (*node[V], *node[V], error) {
	a, err := c.compile(x)
	if err != nil {
		return nil, nil, err
	}
	b, err := c.compile(y)
	if err != nil {
		return nil, nil, err
	}
	return a, b, nil
}

// coerce checks that two things can be compared, and reads a text that is compared with a time as
// the time that it is.
func coerce[V any](e rsql.Expr, x, y *node[V]) (*node[V], *node[V], error) {
	var err error
	if x.kind == kindTime && y.isLit && y.kind == kindString {
		y, err = timeNode[V](e, y)
	} else if y.kind == kindTime && x.isLit && x.kind == kindString {
		x, err = timeNode[V](e, x)
	}
	if err != nil {
		return nil, nil, err
	}

	switch {
	case x.kind.open(), y.kind.open(), x.kind == y.kind:
	case x.kind == kindString && y.kind == kindBytes, x.kind == kindBytes && y.kind == kindString:
	default:
		return nil, nil, fmt.Errorf("%w: %s is %s and %s is %s, which cannot be compared in %s",
			ErrType, x.text, x.kind, y.text, y.kind, show(e))
	}
	return x, y, nil
}

func timeNode[V any](e rsql.Expr, n *node[V]) (*node[V], error) {
	t, err := field.Convert(n.lit, reflect.TypeFor[time.Time]())
	if err != nil {
		return nil, fmt.Errorf("%w: %v, in %s", ErrType, err, show(e))
	}
	return literalNode[V](kindTime, t, n.text), nil
}

func (c *core[V]) compileIn(e *rsql.BinaryExpr) (*node[V], error) {
	x, err := c.compile(e.X)
	if err != nil {
		return nil, err
	}
	negate := e.Op == rsql.NOTIN

	var members []*node[V]
	switch y := e.Y.(type) {
	case *rsql.ExprList:
		for _, member := range y.Exprs {
			if _, ok := member.(rsql.SelectExpr); ok {
				return nil, unsupported(member, "a subquery")
			}
			n, err := c.compile(member)
			if err != nil {
				return nil, err
			}
			if x, n, err = coerce(e, x, n); err != nil {
				return nil, err
			}
			members = append(members, n)
		}

	case *rsql.Ident, *rsql.QualifiedRef:
		// A field of several values.
		n, err := c.compile(y)
		if err != nil {
			return nil, err
		}
		path := n.text
		if col, err := c.column(path); err != nil || !col.multi {
			return nil, fmt.Errorf("%w: %s is not a list of values; IN takes a list in parentheses or a field of several values",
				ErrType, path)
		}
		if _, n, err = coerce(e, x, n); err != nil {
			return nil, err
		}
		members = append(members, n)

	default:
		return nil, unsupported(e.Y, "this")
	}

	equal := func(c int) bool { return c == 0 }
	return boolNode(show(e), func(v *V) bool {
		a := x.eval(v)
		for _, member := range members {
			if holds(a, member.eval(v), equal) {
				return !negate
			}
		}
		return negate
	}), nil
}

func (c *core[V]) compileLike(e *rsql.BinaryExpr) (*node[V], error) {
	x, y, err := c.operands(e.X, e.Y)
	if err != nil {
		return nil, err
	}
	for _, operand := range []*node[V]{x, y} {
		if !setOf(kindString, kindNumber).has(operand.kind) {
			return nil, operandError(e, operand, kindString)
		}
	}

	glob := e.Op == rsql.GLOB || e.Op == rsql.NOTGLOB
	negate := e.Op == rsql.NOTLIKE || e.Op == rsql.NOTGLOB
	many, one := '%', '_'
	if glob {
		many, one = '*', '?'
	}

	var escape rune
	hasEscape := false
	if e.EscapeExpr != nil {
		n, err := c.compile(e.EscapeExpr)
		if err != nil {
			return nil, err
		}
		s, _ := n.lit.(string)
		if !n.isLit || len([]rune(s)) != 1 {
			return nil, fmt.Errorf("%w: ESCAPE takes a string of one character in %s", ErrType, show(e))
		}
		escape, hasEscape = []rune(s)[0], true
	}
	build := func(s string) *pattern {
		return newPattern(s, many, one, escape, hasEscape, !glob)
	}

	var precompiled *pattern
	if s, ok := y.lit.(string); ok && y.isLit {
		precompiled = build(s)
	}
	return boolNode(show(e), func(v *V) bool {
		p := y.eval(v)
		return anyOf(x.eval(v), func(a any) bool {
			s, ok := text(a)
			if !ok {
				return false
			}
			if precompiled != nil {
				return precompiled.matches(s)
			}
			return anyOf(p, func(p any) bool {
				ps, ok := text(p)
				return ok && build(ps).matches(s)
			})
		}) != negate
	}), nil
}

func (c *core[V]) compileCall(e *rsql.Call) (*node[V], error) {
	name := strings.ToLower(e.Name.Name)
	f, ok := functions[name]
	if !ok {
		var custom Function
		if custom, ok = c.functions.byName[name]; ok {
			f = custom.internal()
		}
	}
	if !ok || e.Schema != nil {
		return nil, fmt.Errorf("%w %q", ErrUnknownFunction, e.Name.Name)
	}
	if e.Star.IsValid() || e.Distinct.IsValid() || e.All.IsValid() || e.Filter != nil || e.Over != nil {
		return nil, unsupported(e, "this form of a call")
	}
	if len(e.Args) < f.min || (f.max >= 0 && len(e.Args) > f.max) {
		return nil, fmt.Errorf("%w: %s takes %s, not %d", ErrType, name, arity(f), len(e.Args))
	}

	args := make([]*node[V], len(e.Args))
	kinds := make([]kind, len(e.Args))
	for i, arg := range e.Args {
		n, err := c.compile(arg)
		if err != nil {
			return nil, err
		}
		accepts := f.accepts[min(i, len(f.accepts)-1)]
		if !accepts.has(n.kind) {
			return nil, fmt.Errorf("%w: argument %d of %s is %s, which it does not take: %s",
				ErrType, i+1, name, n.kind, show(e))
		}
		args[i], kinds[i] = n, n.kind
	}

	return &node[V]{kind: f.result(kinds), text: show(e), eval: func(v *V) any {
		values := make([]any, len(args))
		for i, arg := range args {
			values[i] = arg.eval(v)
		}
		if f.lifted {
			return lift(values, f.call)
		}
		return f.call(values)
	}}, nil
}

func arity(f *function) string {
	plural := func(n int) string {
		if n == 1 {
			return "1 argument"
		}
		return fmt.Sprintf("%d arguments", n)
	}
	switch {
	case f.max < 0:
		return "at least " + plural(f.min)
	case f.min == f.max:
		return plural(f.min)
	}
	return fmt.Sprintf("%d to %d arguments", f.min, f.max)
}

func (c *core[V]) compileCase(e *rsql.CaseExpr) (*node[V], error) {
	var operand *node[V]
	if e.Operand != nil {
		var err error
		if operand, err = c.compile(e.Operand); err != nil {
			return nil, err
		}
	}

	type branch struct {
		when, then *node[V]
	}
	var (
		branches []branch
		kinds    []kind
	)
	for _, block := range e.Blocks {
		when, err := c.compile(block.Condition)
		if err != nil {
			return nil, err
		}
		if operand != nil {
			if _, when, err = coerce(e, operand, when); err != nil {
				return nil, err
			}
		} else if !conditional(when.kind) {
			return nil, notACondition(block.Condition, when.kind)
		}
		then, err := c.compile(block.Body)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch{when, then})
		kinds = append(kinds, then.kind)
	}
	var otherwise *node[V]
	if e.ElseExpr != nil {
		var err error
		if otherwise, err = c.compile(e.ElseExpr); err != nil {
			return nil, err
		}
		kinds = append(kinds, otherwise.kind)
	}

	equal := func(c int) bool { return c == 0 }
	return &node[V]{kind: unify(kinds...), text: show(e), eval: func(v *V) any {
		var subject any
		if operand != nil {
			subject = operand.eval(v)
		}
		for _, b := range branches {
			w := b.when.eval(v)
			if operand != nil && holds(subject, w, equal) || operand == nil && truth(w) {
				return b.then.eval(v)
			}
		}
		if otherwise != nil {
			return otherwise.eval(v)
		}
		return nil
	}}, nil
}
