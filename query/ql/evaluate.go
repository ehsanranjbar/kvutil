package ql

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/value"
	"github.com/araddon/qlbridge/vm"
	"github.com/ehsanranjbar/kvutil/cond"
)

// evaluated is a condition that qlbridge evaluates on every record: the part of a text that
// is not a comparison of a field with values. An index cannot serve it.
type evaluated[V any] struct {
	text    string
	node    expr.Node
	columns map[string]*column[V]
}

// evaluate returns the condition of a node that is evaluated, after checking what can be
// checked without a record: that the fields and the functions that it names are there.
func (p *Parser[V]) evaluate(node expr.Node) (cond.Cond[V], error) {
	e := &evaluated[V]{
		text:    node.String(),
		node:    node,
		columns: map[string]*column[V]{},
	}
	if err := p.check(node, e.columns); err != nil {
		return nil, err
	}
	return e, nil
}

// check finds the fields that a node names, and fails for a field or a function that there
// is not.
func (p *Parser[V]) check(node expr.Node, columns map[string]*column[V]) error {
	var args []expr.Node
	switch n := node.(type) {
	case *expr.IdentityNode:
		if _, ok := boolean(n); ok {
			return nil
		}
		col, err := p.column(n.Text)
		if err != nil {
			return err
		}
		columns[n.Text] = col
		return nil

	case *expr.FuncNode:
		if n.Missing || n.F.CustomFunc == nil {
			return fmt.Errorf("%w %q", ErrUnknownFunction, n.Name)
		}
		args = n.Args

	case *expr.BinaryNode:
		args = n.Args
	case *expr.BooleanNode:
		args = n.Args
	case *expr.TriNode:
		args = n.Args
	case *expr.ArrayNode:
		args = n.Args
	case *expr.UnaryNode:
		args = []expr.Node{n.Arg}

	case *expr.StringNode, *expr.NumberNode, *expr.NullNode, *expr.ValueNode:
		return nil

	default:
		return fmt.Errorf("%w: %s is not supported", ErrSyntax, node)
	}

	for _, arg := range args {
		if err := p.check(arg, columns); err != nil {
			return err
		}
	}
	return nil
}

// Match implements the cond.Cond interface.
func (e *evaluated[V]) Match(v *V) (matched bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			matched, err = false, fmt.Errorf("%w: %s: %v", ErrEvaluation, e.text, r)
		}
	}()

	result, ok := vm.Eval(row[V]{v: v, columns: e.columns}, e.node)
	if !ok || result == nil || result.Nil() {
		// The evaluator has one answer for everything that it has nothing to say of: a field
		// that has no value, a function of an empty text, and also a division by zero or the
		// comparison of a number with a text. They cannot be told apart, so none of them is
		// an error: the record does not satisfy the condition.
		return false, nil
	}
	if b, ok := result.(value.BoolValue); ok {
		return b.Val(), nil
	}
	return false, fmt.Errorf("%w: %s is a %s, not true or false", ErrEvaluation, e.text, result.Type())
}

// String implements the cond.Cond interface.
func (e *evaluated[V]) String() string {
	return e.text
}

// row is a record as the evaluator reads it.
type row[V any] struct {
	v       *V
	columns map[string]*column[V]
}

// Get implements the expr.ContextReader interface.
func (r row[V]) Get(name string) (value.Value, bool) {
	col, ok := r.columns[name]
	if !ok {
		return nil, false
	}
	v, ok := col.value(r.v)
	if !ok {
		return nil, false
	}
	return value.NewValue(v), true
}

// Row implements the expr.ContextReader interface. The fields are only read by their names.
func (r row[V]) Row() map[string]value.Value {
	return nil
}

// Ts implements the expr.ContextReader interface. A record has no time of its own.
func (r row[V]) Ts() time.Time {
	return time.Time{}
}

// plain returns a value in a form that the evaluator knows: a value of a named type as one
// of the type that it is defined on, and a slice as one of such values. It reports whether
// there is a value, which there is not behind a pointer that is nil.
func plain(rv reflect.Value) (any, bool) {
	for rv.IsValid() && (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil, false
	}
	if t, ok := rv.Interface().(time.Time); ok {
		return t, true
	}

	switch rv.Kind() {
	case reflect.String:
		return rv.String(), true
	case reflect.Bool:
		return rv.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if u := rv.Uint(); u > math.MaxInt64 {
			return float64(u), true
		}
		return int64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true

	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return rv.Bytes(), true
		}
		return plainSlice(rv), true

	case reflect.Array:
		// An array of bytes, like a UUID, is its text if it has one.
		if s, ok := rv.Interface().(fmt.Stringer); ok {
			return s.String(), true
		}
		return plainSlice(rv), true

	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			break
		}
		m := make(map[string]any, rv.Len())
		for iter := rv.MapRange(); iter.Next(); {
			if element, ok := plain(iter.Value()); ok {
				m[iter.Key().String()] = element
			}
		}
		return m, true
	}
	return rv.Interface(), true
}

// plainSlice returns the elements of a slice or of an array in the form that plain returns.
// A slice of texts is one of strings, which is the list that the evaluator knows best.
func plainSlice(rv reflect.Value) any {
	elements := make([]any, 0, rv.Len())
	texts := make([]string, 0, rv.Len())
	for i := range rv.Len() {
		element, ok := plain(rv.Index(i))
		if !ok {
			continue
		}
		elements = append(elements, element)
		if text, ok := element.(string); ok {
			texts = append(texts, text)
		}
	}
	if len(texts) == len(elements) {
		return texts
	}
	return elements
}
