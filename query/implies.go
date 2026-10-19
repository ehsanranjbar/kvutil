package query

import (
	"bytes"
	"slices"

	"github.com/ehsanranjbar/kvutil/cond"
)

// An index with a condition (index.Where) has the records that satisfy the condition only, so it
// finds all the records of a query only if every record that the query asks for satisfies the
// condition: if the conditions of the query imply it. The implication is decided here, and
// conservatively: it says yes only when the conditions of the query clearly do. A query whose
// conditions imply the condition of the index in a way that this does not see just does not
// read the index; its results are the same.
//
// The condition of the index is normalised as the conditions of the query are, and each of its
// conjuncts must be implied by the conditions of the query:
//
//   - A comparison is implied by a comparison of the same field, with the same key codec, that
//     allows only values that the first allows too: values that are all among the values of an
//     equality or of IN, or all inside a range; a range that is inside the other range, with the
//     exclusive sides taken into account and a side that is not bounded in the first being
//     bounded in the second. This holds for a field of several values as well, as the
//     comparison is satisfied by one of the values.
//   - A disjunction is implied if one of its branches is, and a conjunction if all of its
//     parts are.
//   - A disjunction among the conditions of the query implies what every one of its branches
//     does, together with the other conditions of the query.
//   - Anything else (a negation, a function, an SQL text that is evaluated) is implied by a
//     condition of the query that has the identical text, and by nothing else.
//   - A condition that is true whatever the record is, is implied by anything.
//
// The conditions of the index that are left after this are not removed from the ones that the
// records are checked against: they are checked as they would be without the index.

// implies reports whether the conditions imply the condition of an index.
func implies[V any](conds []cond.Cond[V], index cond.Cond[V]) bool {
	target, empty := normalise([]cond.Cond[V]{index})
	if empty {
		// Nothing is in the index, which cannot be what a query wants.
		return false
	}
	for _, t := range target {
		if !impliesOne(conds, t, 0) {
			return false
		}
	}
	return true
}

// maxDepth is how many disjunctions of the conditions are taken apart, which keeps the work
// bounded for conditions that are made of many.
const maxDepth = 3

// impliesOne reports whether the conditions imply a condition.
func impliesOne[V any](facts []cond.Cond[V], target cond.Cond[V], depth int) bool {
	switch t := target.(type) {
	case *cond.Conjunction[V]:
		for _, c := range t.Conds {
			if !impliesOne(facts, c, depth) {
				return false
			}
		}
		return true

	case *cond.Disjunction[V]:
		for _, c := range t.Conds {
			if impliesOne(facts, c, depth) {
				return true
			}
		}

	case *cond.Comparison[V]:
		// A comparison is decided by what it compares, never by its text: two fields of one
		// path may be read or encoded differently, and then N = 5 is not N = 5.
		if t.Err() == nil {
			for _, f := range facts {
				if c, ok := f.(*cond.Comparison[V]); ok && c.Err() == nil && within(c, t) {
					return true
				}
			}
		}

	case *cond.Negation[V]:
		// NOT a implies NOT b when b implies a, which is decided as anything else is.
		for _, f := range facts {
			if n, ok := f.(*cond.Negation[V]); ok && impliesOne(conjuncts(t.Cond), n.Cond, depth) {
				return true
			}
		}

	default:
		// What cannot be looked into, like a function, is only implied by itself.
		text := target.String()
		for _, f := range facts {
			if f.String() == text {
				return true
			}
		}
	}

	if depth >= maxDepth {
		return false
	}
	for i, f := range facts {
		or, ok := f.(*cond.Disjunction[V])
		if !ok || len(or.Conds) == 0 {
			continue
		}
		rest := slices.Concat(facts[:i], facts[i+1:])
		all := true
		for _, branch := range or.Conds {
			if !impliesOne(slices.Concat(conjuncts(branch), rest), target, depth+1) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// conjuncts returns the conditions that all have to be satisfied for a condition to be.
func conjuncts[V any](c cond.Cond[V]) []cond.Cond[V] {
	and, ok := c.(*cond.Conjunction[V])
	if !ok {
		return []cond.Cond[V]{c}
	}
	var flat []cond.Cond[V]
	for _, child := range and.Conds {
		flat = append(flat, conjuncts(child)...)
	}
	return flat
}

// within reports whether every value that the comparison q allows is allowed by the comparison t
// of the same field.
func within[V any](q, t *cond.Comparison[V]) bool {
	if q.Path != t.Path || q.Codec != t.Codec {
		return false
	}

	switch t.Op {
	case cond.OpEq, cond.OpIn:
		if q.Op == cond.OpRange || len(q.Values) == 0 {
			return false
		}
		for _, value := range q.Values {
			if !slices.ContainsFunc(t.Values, func(v []byte) bool { return bytes.Equal(v, value) }) {
				return false
			}
		}
		return true

	case cond.OpRange:
		if q.Op != cond.OpRange {
			if len(q.Values) == 0 {
				return false
			}
			for _, value := range q.Values {
				if !inRange(value, t.Low, t.High) {
					return false
				}
			}
			return true
		}
		return lowWithin(q.Low, t.Low) && highWithin(q.High, t.High)
	}
	return false
}

// lowWithin reports whether a range that starts at q starts no sooner than one that starts at t.
func lowWithin(q, t *cond.Bound) bool {
	switch {
	case t == nil:
		return true
	case q == nil:
		return false
	}
	c := bytes.Compare(q.Value, t.Value)
	return c > 0 || (c == 0 && (!t.Exclusive || q.Exclusive))
}

// highWithin reports whether a range that ends at q ends no later than one that ends at t.
func highWithin(q, t *cond.Bound) bool {
	switch {
	case t == nil:
		return true
	case q == nil:
		return false
	}
	c := bytes.Compare(q.Value, t.Value)
	return c < 0 || (c == 0 && (!t.Exclusive || q.Exclusive))
}
