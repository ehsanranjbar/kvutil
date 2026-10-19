package query

import (
	"bytes"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil/cond"
)

// truth is what is known of a condition without a record.
type truth int

const (
	unknown truth = iota
	isTrue
	isFalse
)

// normalise returns the conditions that all have to be satisfied for the conditions to be, in a
// form that the planner reads: nested conjunctions are flattened, a condition that is true or
// false whatever the record is goes (the whole is empty if one is false), and the comparisons of
// one field are one comparison. A comparison of a field that has one value for a record is
// merged with the other comparisons of the field, and the whole is empty if they contradict each
// other: two different values, a value that is outside of a range, a range with nothing in it.
// Comparisons of a field that may have several values are not merged, as different values may
// satisfy them. Negations and disjunctions are kept as they are, to be checked on the records.
func normalise[V any](conds []cond.Cond[V]) (normal []cond.Cond[V], empty bool) {
	flat, t := simplifyAnd(conds)
	if t == isFalse {
		return nil, true
	}

	// The comparisons of a field that can be merged, by the field, in the order that the fields
	// first appear in. Each is at the place of its first comparison.
	var (
		slots  = make([]cond.Cond[V], len(flat))
		groups = map[string][]int{}
		order  []string
	)
	for i, c := range flat {
		slots[i] = c
		cmp, ok := c.(*cond.Comparison[V])
		if !ok {
			continue
		}
		if cmp.Op == cond.OpIn && len(cmp.Values) == 0 {
			// Equal to none of the values: to nothing.
			return nil, true
		}
		if cmp.Err() != nil || cmp.Multi {
			continue
		}

		key := cmp.Path + "\x00" + cmp.Codec
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], i)
	}

	for _, key := range order {
		at := groups[key]
		cmps := make([]*cond.Comparison[V], len(at))
		for i, pos := range at {
			cmps[i] = flat[pos].(*cond.Comparison[V])
		}

		merged, empty := merge(cmps)
		if empty {
			return nil, true
		}
		slots[at[0]] = merged
		for _, pos := range at[1:] {
			slots[pos] = nil
		}
	}

	for _, c := range slots {
		if c != nil {
			normal = append(normal, c)
		}
	}
	return normal, false
}

// simplifyAnd returns the conditions of a conjunction, flattened and simplified.
func simplifyAnd[V any](conds []cond.Cond[V]) ([]cond.Cond[V], truth) {
	var flat []cond.Cond[V]
	for _, c := range conds {
		s, t := simplify(c)
		switch t {
		case isFalse:
			return nil, isFalse
		case isTrue:
			continue
		}
		if and, ok := s.(*cond.Conjunction[V]); ok {
			flat = append(flat, and.Conds...)
			continue
		}
		flat = append(flat, s)
	}
	if len(flat) == 0 {
		return nil, isTrue
	}
	return flat, unknown
}

// simplify returns the condition without the parts that are true or false whatever the record
// is, or says that the whole of it is.
func simplify[V any](c cond.Cond[V]) (cond.Cond[V], truth) {
	switch c := c.(type) {
	case *cond.Conjunction[V]:
		flat, t := simplifyAnd(c.Conds)
		if t != unknown {
			return nil, t
		}
		if len(flat) == 1 {
			return flat[0], unknown
		}
		return cond.And(flat...), unknown

	case *cond.Disjunction[V]:
		var flat []cond.Cond[V]
		for _, child := range c.Conds {
			s, t := simplify(child)
			switch t {
			case isTrue:
				return nil, isTrue
			case isFalse:
				continue
			}
			if or, ok := s.(*cond.Disjunction[V]); ok {
				flat = append(flat, or.Conds...)
				continue
			}
			flat = append(flat, s)
		}
		switch len(flat) {
		case 0:
			return nil, isFalse
		case 1:
			return flat[0], unknown
		}
		return cond.Or(flat...), unknown

	case *cond.Negation[V]:
		s, t := simplify(c.Cond)
		switch t {
		case isTrue:
			return nil, isFalse
		case isFalse:
			return nil, isTrue
		}
		return cond.Not(s), unknown
	}
	return c, unknown
}

// merge returns the one comparison that is satisfied by what all the comparisons of a field are
// satisfied by, or says that there is nothing that is. The field has one value at most.
func merge[V any](cmps []*cond.Comparison[V]) (merged *cond.Comparison[V], empty bool) {
	var (
		values   [][]byte
		hasValue bool
		low      *cond.Bound
		high     *cond.Bound
		texts    []string
	)
	for _, c := range cmps {
		if text := c.String(); !slices.Contains(texts, text) {
			texts = append(texts, text)
		}

		switch c.Op {
		case cond.OpEq, cond.OpIn:
			set := sortedSet(c.Values)
			if hasValue {
				set = intersection(values, set)
			}
			values, hasValue = set, true
		case cond.OpRange:
			low = tighterLow(low, c.Low)
			high = tighterHigh(high, c.High)
		}
	}

	if hasValue {
		values = slices.DeleteFunc(values, func(v []byte) bool { return !inRange(v, low, high) })
		if len(values) == 0 {
			return nil, true
		}
	} else if emptyRange(low, high) {
		return nil, true
	}

	if len(cmps) == 1 {
		c := cmps[0]
		if c.Op != cond.OpIn || slices.EqualFunc(values, c.Values, bytes.Equal) {
			return c, false
		}
	}

	m := *cmps[0]
	m.Describe(strings.Join(texts, " and "))
	if hasValue {
		m.Op, m.Values, m.Low, m.High = cond.OpIn, values, nil, nil
		if len(values) == 1 {
			m.Op = cond.OpEq
		}
	} else {
		m.Op, m.Values, m.Low, m.High = cond.OpRange, nil, low, high
	}
	return &m, false
}

// sortedSet returns the values in ascending order, each once.
func sortedSet(values [][]byte) [][]byte {
	set := slices.Clone(values)
	slices.SortFunc(set, bytes.Compare)
	return slices.CompactFunc(set, bytes.Equal)
}

// intersection returns the values of a sorted set that are in another.
func intersection(a, b [][]byte) [][]byte {
	var both [][]byte
	for _, v := range a {
		if _, found := slices.BinarySearchFunc(b, v, bytes.Compare); found {
			both = append(both, v)
		}
	}
	return both
}

// tighterLow returns the start that excludes more.
func tighterLow(a, b *cond.Bound) *cond.Bound {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	if cmp := bytes.Compare(b.Value, a.Value); cmp > 0 || (cmp == 0 && b.Exclusive) {
		return b
	}
	return a
}

// tighterHigh returns the end that excludes more.
func tighterHigh(a, b *cond.Bound) *cond.Bound {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	if cmp := bytes.Compare(b.Value, a.Value); cmp < 0 || (cmp == 0 && b.Exclusive) {
		return b
	}
	return a
}

// inRange reports whether an encoded value is between the sides of a range.
func inRange(value []byte, low, high *cond.Bound) bool {
	if low != nil {
		if cmp := bytes.Compare(value, low.Value); cmp < 0 || (cmp == 0 && low.Exclusive) {
			return false
		}
	}
	if high != nil {
		if cmp := bytes.Compare(value, high.Value); cmp > 0 || (cmp == 0 && high.Exclusive) {
			return false
		}
	}
	return true
}

// emptyRange reports whether nothing is between the sides of a range.
func emptyRange(low, high *cond.Bound) bool {
	if low == nil || high == nil {
		return false
	}
	cmp := bytes.Compare(low.Value, high.Value)
	return cmp > 0 || (cmp == 0 && (low.Exclusive || high.Exclusive))
}
