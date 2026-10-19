package sql

import (
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
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

// Where parses a WHERE clause and returns the query of the records of the store that satisfy it.
// The ordering and the paging of the records are not a part of the text: they are set on the
// query with OrderBy, Offset and Limit, and the query is planned, with the indexes of the store
// that serve the condition, when it is run or explained.
func (p *Parser[K, V]) Where(where string) (*query.Query[K, V], error) {
	c, err := p.Parse(where)
	if err != nil {
		return nil, err
	}
	return query.From(p.store).Where(c), nil
}

// Query parses a WHERE clause and returns the records that satisfy it, in no order that is
// promised (the order of the index that serves it, or of the keys of a scan): it is Where and
// then Run, and an order is given to the query of Where. An error of the text is returned right away, and an error
// while iterating, like one of a condition that cannot be evaluated on a record, stops the
// iterator and is returned by its Err. The iterator must be closed.
func (p *Parser[K, V]) Query(tx kv.Tx, where string) (iterutil.AdvancedIterator[K, *V], error) {
	q, err := p.Where(where)
	if err != nil {
		var none iterutil.AdvancedIterator[K, *V]
		return none, err
	}
	return q.Run(tx), nil
}

// Explain says how Query would run a WHERE clause, as query.Query.Explain does:
//
//	IndexLookup by_author_year: 1 range, then check ["Title" LIKE '%Dune']
func (p *Parser[K, V]) Explain(where string) (string, error) {
	q, err := p.Where(where)
	if err != nil {
		return "", err
	}
	return q.Explain()
}
