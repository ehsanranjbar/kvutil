package cond

import "slices"

// Reader is implemented by what knows which fields of a record it reads.
//
// The interface of a condition leaves this out, as other packages implement it: whoever needs
// the reads of a condition asserts that it is a Reader, and takes one that is not as reading
// what is not known.
type Reader interface {
	// Reads returns the paths of the fields that are read, and false if they are not known.
	Reads() (paths []string, ok bool)
}

var (
	_ Reader = (*Comparison[struct{}])(nil)
	_ Reader = (*Predicate[struct{}])(nil)
	_ Reader = (*Conjunction[struct{}])(nil)
	_ Reader = (*Disjunction[struct{}])(nil)
	_ Reader = (*Negation[struct{}])(nil)
)

// Reads returns the fields that the conditions read, sorted and each once, and false if what one
// of them reads is not known. A conjunction, a disjunction and a negation read what their
// conditions read; any other condition must be a Reader.
func Reads[V any](conds ...Cond[V]) ([]string, bool) {
	paths := []string{}
	for _, c := range conds {
		r, ok := c.(Reader)
		if !ok {
			return nil, false
		}
		read, ok := r.Reads()
		if !ok {
			return nil, false
		}
		paths = append(paths, read...)
	}
	slices.Sort(paths)
	return slices.Compact(paths), true
}

// Reads implements the Reader interface.
func (c *Conjunction[V]) Reads() ([]string, bool) {
	return Reads(c.Conds...)
}

// Reads implements the Reader interface.
func (c *Disjunction[V]) Reads() ([]string, bool) {
	return Reads(c.Conds...)
}

// Reads implements the Reader interface.
func (c *Negation[V]) Reads() ([]string, bool) {
	return Reads(c.Cond)
}
