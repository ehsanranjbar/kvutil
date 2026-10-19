// Package cond has the conditions on records: what a query asks for, and what an index can be
// limited to.
package cond

import (
	"bytes"
	"strings"
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
	// Multi is set if the field may have several values for a record. Two comparisons of such a
	// field can be satisfied by different values, so they are not the same as one comparison of
	// both: Tags > 'a' AND Tags < 'c' is not Tags in the range from 'a' to 'c'.
	Multi bool

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

// NewComparison makes a comparison of a field, for the packages that define fields.
// The function returns the encoded values that a record has for the field. The values that the
// field is compared with are set on the comparison that is returned.
func NewComparison[V any](path, codec string, op Op, keys func(*V) ([][]byte, error)) *Comparison[V] {
	return &Comparison[V]{
		Path:  path,
		Codec: codec,
		Op:    op,
		keys:  keys,
	}
}

// Describe sets the text of the comparison.
func (c *Comparison[V]) Describe(text string) *Comparison[V] {
	c.text = text
	return c
}

// Fail makes the comparison fail with an error, if the error is not nil.
// It is for a value of the comparison that could not be encoded.
func (c *Comparison[V]) Fail(err error) {
	if err != nil {
		c.err = err
	}
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
