package sql

import (
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
)

// Parser parses the WHERE clauses of queries on the records of a store, and runs them.
// It can be used from several goroutines.
type Parser[K, V any] struct {
	core  *core[V]
	store Store[K, V]
}

// For returns a parser of the WHERE clauses of queries on the records of a store.
//
// A field that a text names is one of the fields that the indexes of the store are defined on,
// which a comparison must be written with for the index to serve it, and which may be computed
// by a function. If no index has the field, it is found in the struct V by reflection, as
// field.Reflect does. The indexes are the ones that the store has when a text is first parsed.
func For[K, V any](store Store[K, V], opts ...Option[V]) *Parser[K, V] {
	return &Parser[K, V]{core: newCore(store, opts), store: store}
}

// Parse parses a WHERE clause, without the word WHERE. It returns an error for a text that
// cannot be right for any record: one that is not written as a condition, that names a field or
// a function that there is not, or that compares a field with a value that it cannot have.
func (p *Parser[K, V]) Parse(where string) (cond.Cond[V], error) {
	return p.core.parse(where)
}

// Query parses a WHERE clause and returns the records that satisfy it, using an index of the
// store when one serves the condition, and scanning the store otherwise; Explain says which.
// An error of the text is returned right away, and an error while iterating, like one of a
// condition that cannot be evaluated on a record, stops the iterator and is returned by its Err.
// The records come in the order of the index that serves the condition, and in the order of
// their keys when the store is scanned. The iterator must be closed.
func (p *Parser[K, V]) Query(tx kv.Tx, where string) (iterutil.AdvancedIterator[K, *V], error) {
	var none iterutil.AdvancedIterator[K, *V]

	c, err := p.Parse(where)
	if err != nil {
		return none, err
	}
	pl := p.plan(c)

	if pl.index != nil {
		it, err := pl.index.Instantiate(tx).Lookup(c)
		if err != nil {
			// The index was planned for the condition, so this is the error of reading the first
			// entry, like the one of a condition that cannot be evaluated on its record: it
			// stops the iteration where it is, as the ones that come later do.
			return iterutil.Of[K, *V](&failed[K, V]{err: fmt.Errorf("index %q: %w", pl.index.Name(), err)}), nil
		}
		return iterutil.Of[K, *V](it), nil
	}

	records, err := p.store.Records(tx, kvutil.All[K]())
	if err != nil {
		return none, err
	}
	return iterutil.Of[K, *V](newScan(records, c)), nil
}

// Explain says how Query would run a WHERE clause: by which index and how many ranges of it,
// or by scanning the store, and which conditions the records that are found are checked
// against, as in
//
//	index by_author_year: 1 range, then check ["Title" LIKE '%Dune']
//	scan, then check [Year = 1965; Title LIKE '%Dune']
func (p *Parser[K, V]) Explain(where string) (string, error) {
	c, err := p.Parse(where)
	if err != nil {
		return "", err
	}
	return p.plan(c).String(), nil
}

// plan is how a condition is run.
type plan[K, V any] struct {
	// index is the index that serves the condition, or nil if the store is scanned.
	index  *index.Index[K, V]
	ranges int
	// check are the conditions that the records that are found are checked against.
	check []cond.Cond[V]
}

// String returns the plan as text.
func (pl plan[K, V]) String() string {
	var sb strings.Builder
	if pl.index != nil {
		noun := "ranges"
		if pl.ranges == 1 {
			noun = "range"
		}
		fmt.Fprintf(&sb, "index %s: %d %s", pl.index.Name(), pl.ranges, noun)
	} else {
		sb.WriteString("scan")
	}

	if len(pl.check) > 0 {
		texts := make([]string, 0, len(pl.check))
		for _, c := range pl.check {
			texts = append(texts, c.String())
		}
		fmt.Fprintf(&sb, ", then check [%s]", strings.Join(texts, "; "))
	}
	return sb.String()
}

// plan chooses how to run a condition. An index serves it if the ranges that the index reads
// are fewer than all of its entries. Of the indexes that do, the one is chosen that is unique
// and has nothing left to check, or else has the fewest conditions left to check, or else
// reads the fewest ranges, or else comes first among the extensions of the store.
//
// An index of the records that satisfy a condition is not used: nothing checks that the
// condition of a query implies the one of the index.
func (p *Parser[K, V]) plan(c cond.Cond[V]) plan[K, V] {
	var (
		best      = plan[K, V]{check: conjuncts(c)}
		bestScore [3]int
	)
	for _, ext := range p.store.Extensions() {
		x, ok := ext.(interface{ Index() *index.Index[K, V] })
		if !ok {
			continue
		}
		ix := x.Index()
		if ix.Descriptor().Where != "" {
			continue
		}
		planner, ok := ix.Indexer().(index.RangePlanner[V])
		if !ok {
			continue
		}
		ranges, residual, err := planner.Ranges(c)
		if err != nil || !narrows(ranges) {
			continue
		}

		score := [3]int{1, len(residual), len(ranges)}
		if ix.Descriptor().Unique && len(residual) == 0 {
			score[0] = 0
		}
		if best.index == nil || less(score, bestScore) {
			best = plan[K, V]{index: ix, ranges: len(ranges), check: residual}
			bestScore = score
		}
	}
	return best
}

func less(a, b [3]int) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// narrows reports whether ranges of the keys of an index read fewer than all of them: they are
// none, or more than one, or one that is bounded.
func narrows(ranges []index.Bounds) bool {
	if len(ranges) != 1 {
		return true
	}
	_, _, low := ranges[0].Low()
	_, _, high := ranges[0].High()
	return low || high
}

// conjuncts returns the conditions that are all to be satisfied for a condition to be.
func conjuncts[V any](c cond.Cond[V]) []cond.Cond[V] {
	if and, ok := c.(*cond.Conjunction[V]); ok {
		return and.Conds
	}
	return []cond.Cond[V]{c}
}

// scan is the iterator of the records of a store that satisfy a condition.
type scan[K, V any] struct {
	base   iterutil.Iterator[K, *V]
	cond   cond.Cond[V]
	record *V
	err    error
}

func newScan[K, V any](base iterutil.Iterator[K, *V], c cond.Cond[V]) *scan[K, V] {
	s := &scan[K, V]{base: base, cond: c}
	s.settle()
	return s
}

// settle moves to the first record, from where the iterator is, that satisfies the condition.
func (s *scan[K, V]) settle() {
	s.record = nil
	for s.err == nil && s.base.Valid() {
		record, err := s.base.Value()
		if err != nil {
			s.err = err
			return
		}
		ok, err := s.cond.Match(record)
		if err != nil {
			s.err = fmt.Errorf("failed to check %s: %w", s.cond, err)
			return
		}
		if ok {
			s.record = record
			return
		}
		s.base.Next()
	}
}

// Valid implements the iterutil.Iterator interface.
func (s *scan[K, V]) Valid() bool {
	return s.err == nil && s.base.Valid()
}

// Next implements the iterutil.Iterator interface.
func (s *scan[K, V]) Next() {
	if !s.Valid() {
		return
	}
	s.base.Next()
	s.settle()
}

// Key implements the iterutil.Iterator interface.
func (s *scan[K, V]) Key() (K, error) {
	return s.base.Key()
}

// Value implements the iterutil.Iterator interface.
func (s *scan[K, V]) Value() (*V, error) {
	if s.record != nil {
		return s.record, nil
	}
	return s.base.Value()
}

// Err returns the error that stopped the iterator early, if there is one.
func (s *scan[K, V]) Err() error {
	if s.err != nil {
		return s.err
	}
	return iterutil.Of(s.base).Err()
}

// Close implements the iterutil.Iterator interface.
func (s *scan[K, V]) Close() error {
	return s.base.Close()
}

// failed is the iterator of a query that stopped before its first record.
type failed[K, V any] struct {
	err error
}

// Valid implements the iterutil.Iterator interface.
func (f *failed[K, V]) Valid() bool { return false }

// Next implements the iterutil.Iterator interface.
func (f *failed[K, V]) Next() {}

// Key implements the iterutil.Iterator interface.
func (f *failed[K, V]) Key() (key K, err error) { return key, f.err }

// Value implements the iterutil.Iterator interface.
func (f *failed[K, V]) Value() (*V, error) { return nil, f.err }

// Close implements the iterutil.Iterator interface.
func (f *failed[K, V]) Close() error { return nil }

// Err returns the error that stopped the iterator.
func (f *failed[K, V]) Err() error { return f.err }
