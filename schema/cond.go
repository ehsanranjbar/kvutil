package schema

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil"
)

// Cond is a condition on the records of the type V.
//
// A condition is made from a field, like Author.Eq("Herbert"), or from other conditions with
// And, Or and Not. It can be checked on a record, and it can be read by an index to find the
// records that satisfy it without checking each of them. The conditions of this package are
// the types Comparison, Conjunction, Disjunction, Negation and Predicate.
type Cond[V any] interface {
	// Match reports whether a record satisfies the condition.
	Match(v *V) (bool, error)
	// String returns the condition as text. Two conditions with the same text are the same.
	String() string
}

// Op is the kind of a comparison.
type Op int

const (
	// OpEq compares a field with one value that it must be equal to.
	OpEq Op = iota + 1
	// OpIn compares a field with several values that it must be equal to one of.
	OpIn
	// OpRange compares a field with a range that it must be in.
	OpRange
)

// Bound is a side of the range of a comparison.
type Bound struct {
	// Value is the encoded value of the bound.
	Value []byte
	// Exclusive is set if the value itself is out of the range.
	Exclusive bool
}

// Comparison is a condition that compares a field with values.
// The values are encoded as the field encodes them, in the form that marks where they end and
// that sorts as the values compare, so an index on the field can look them up as they are.
// A record that has several values for the field satisfies the condition if one of them does.
type Comparison[V any] struct {
	// Path is the path of the field.
	Path string
	// Codec is the name of the key codec that the values are encoded with.
	Codec string
	// Op is the kind of the comparison.
	Op Op
	// Values are the encoded values of an OpEq, which has one, or of an OpIn.
	Values [][]byte
	// Low and High are the sides of the range of an OpRange. A nil side is not bounded.
	Low, High *Bound

	keys func(*V) ([][]byte, error)
	text string
	err  error
}

// Match implements the Cond interface.
func (c *Comparison[V]) Match(v *V) (bool, error) {
	if c.err != nil {
		return false, c.err
	}

	keys, err := c.keys(v)
	if err != nil {
		return false, err
	}
	for _, key := range keys {
		if c.has(key) {
			return true, nil
		}
	}
	return false, nil
}

// has reports whether an encoded value satisfies the comparison.
func (c *Comparison[V]) has(key []byte) bool {
	if c.Op != OpRange {
		for _, value := range c.Values {
			if bytes.Equal(key, value) {
				return true
			}
		}
		return false
	}

	if c.Low != nil {
		if cmp := bytes.Compare(key, c.Low.Value); cmp < 0 || (cmp == 0 && c.Low.Exclusive) {
			return false
		}
	}
	if c.High != nil {
		if cmp := bytes.Compare(key, c.High.Value); cmp > 0 || (cmp == 0 && c.High.Exclusive) {
			return false
		}
	}
	return true
}

// String implements the Cond interface.
func (c *Comparison[V]) String() string {
	return c.text
}

// Err returns the error of a value of the comparison that could not be encoded, if there is one.
func (c *Comparison[V]) Err() error {
	return c.err
}

func (f Field[V, F]) comparison(op Op, text string) *Comparison[V] {
	return &Comparison[V]{
		Path:  f.path,
		Codec: f.codec.Name(),
		Op:    op,
		keys:  f.ascending,
		text:  text,
	}
}

// Eq returns the condition of the field being equal to a value.
func (f Field[V, F]) Eq(value F) *Comparison[V] {
	c := f.comparison(OpEq, fmt.Sprintf("%s = %#v", f.path, value))
	key, err := f.Encode(value)
	c.Values, c.err = [][]byte{key}, err
	return c
}

// In returns the condition of the field being equal to one of the values.
func (f Field[V, F]) In(values ...F) *Comparison[V] {
	texts := make([]string, 0, len(values))
	for _, value := range values {
		texts = append(texts, fmt.Sprintf("%#v", value))
	}
	c := f.comparison(OpIn, fmt.Sprintf("%s in (%s)", f.path, strings.Join(texts, ", ")))

	for _, value := range values {
		key, err := f.Encode(value)
		if err != nil {
			c.err = err
		}
		c.Values = append(c.Values, key)
	}
	return c
}

// Gt returns the condition of the field being greater than a value.
func (f Field[V, F]) Gt(value F) *Comparison[V] {
	return f.Range(kvutil.After(value))
}

// Gte returns the condition of the field being greater than a value, or equal to it.
func (f Field[V, F]) Gte(value F) *Comparison[V] {
	return f.Range(kvutil.From(value))
}

// Lt returns the condition of the field being less than a value.
func (f Field[V, F]) Lt(value F) *Comparison[V] {
	return f.Range(kvutil.Until(value))
}

// Lte returns the condition of the field being less than a value, or equal to it.
func (f Field[V, F]) Lte(value F) *Comparison[V] {
	return f.Range(kvutil.Through(value))
}

// Between returns the condition of the field being between two values, or equal to one of them.
func (f Field[V, F]) Between(low, high F) *Comparison[V] {
	return f.Range(kvutil.Between(low, high))
}

// Range returns the condition of the field being in a range.
func (f Field[V, F]) Range(r kvutil.Range[F]) *Comparison[V] {
	var texts []string
	c := f.comparison(OpRange, "")

	if low, exclusive, ok := r.Low(); ok {
		key, err := f.Encode(low)
		if err != nil {
			c.err = err
		}
		c.Low = &Bound{Value: key, Exclusive: exclusive}
		if exclusive {
			texts = append(texts, fmt.Sprintf("%s > %#v", f.path, low))
		} else {
			texts = append(texts, fmt.Sprintf("%s >= %#v", f.path, low))
		}
	}
	if high, exclusive, ok := r.High(); ok {
		key, err := f.Encode(high)
		if err != nil {
			c.err = err
		}
		c.High = &Bound{Value: key, Exclusive: exclusive}
		if exclusive {
			texts = append(texts, fmt.Sprintf("%s < %#v", f.path, high))
		} else {
			texts = append(texts, fmt.Sprintf("%s <= %#v", f.path, high))
		}
	}

	if len(texts) == 0 {
		texts = []string{f.path + " is anything"}
	}
	c.text = strings.Join(texts, " and ")
	return c
}

// Conjunction is the condition of every one of its conditions being satisfied.
type Conjunction[V any] struct {
	Conds []Cond[V]
}

// And returns the condition of every one of the conditions being satisfied.
// It is satisfied by every record if there are no conditions.
func And[V any](conds ...Cond[V]) Cond[V] {
	return &Conjunction[V]{Conds: conds}
}

// Match implements the Cond interface.
func (c *Conjunction[V]) Match(v *V) (bool, error) {
	for _, cond := range c.Conds {
		if ok, err := cond.Match(v); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// String implements the Cond interface.
func (c *Conjunction[V]) String() string {
	return join(c.Conds, " and ", "true")
}

// Disjunction is the condition of one of its conditions at least being satisfied.
type Disjunction[V any] struct {
	Conds []Cond[V]
}

// Or returns the condition of one of the conditions at least being satisfied.
// It is satisfied by no record if there are no conditions.
func Or[V any](conds ...Cond[V]) Cond[V] {
	return &Disjunction[V]{Conds: conds}
}

// Match implements the Cond interface.
func (c *Disjunction[V]) Match(v *V) (bool, error) {
	for _, cond := range c.Conds {
		if ok, err := cond.Match(v); ok || err != nil {
			return ok, err
		}
	}
	return false, nil
}

// String implements the Cond interface.
func (c *Disjunction[V]) String() string {
	return join(c.Conds, " or ", "false")
}

func join[V any](conds []Cond[V], sep, empty string) string {
	if len(conds) == 0 {
		return empty
	}
	texts := make([]string, 0, len(conds))
	for _, cond := range conds {
		texts = append(texts, "("+cond.String()+")")
	}
	return strings.Join(texts, sep)
}

// Negation is the condition of its condition not being satisfied.
type Negation[V any] struct {
	Cond Cond[V]
}

// Not returns the condition of a condition not being satisfied.
func Not[V any](cond Cond[V]) Cond[V] {
	return &Negation[V]{Cond: cond}
}

// Match implements the Cond interface.
func (c *Negation[V]) Match(v *V) (bool, error) {
	ok, err := c.Cond.Match(v)
	return !ok && err == nil, err
}

// String implements the Cond interface.
func (c *Negation[V]) String() string {
	return "not (" + c.Cond.String() + ")"
}

// Predicate is a condition that is a function, which only its name is known of.
type Predicate[V any] struct {
	Name string
	f    func(*V) bool
}

// Func returns the condition of a function returning true for a record.
// As a function cannot be compared with another one, the name stands for what it does:
// the name must change when the function does.
func Func[V any](name string, f func(*V) bool) Cond[V] {
	return &Predicate[V]{Name: name, f: f}
}

// Match implements the Cond interface.
func (c *Predicate[V]) Match(v *V) (bool, error) {
	return c.f(v), nil
}

// String implements the Cond interface.
func (c *Predicate[V]) String() string {
	return c.Name + "()"
}
