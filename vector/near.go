package vector

import (
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
)

// To returns the order of the records by the distance of their vectors from q, for a query of
// package query: query.From(docs).Nearest(byEmbedding.To(q)).Limit(10). It is a value, and its
// methods return other ones.
//
// The package does not depend on package query: the order implements the interface that the query
// asks for by its methods. The vector q is checked when the query is run, not here.
func (x *Index[K, V]) To(q []float32) Near[K, V] {
	return Near[K, V]{x: x, query: q}
}

// Near is the order of the records by the distance of their vectors from a vector, which a query
// of package query is given (see Index.To). It runs the search of the index with the options that
// it has, and each of its methods that sets one returns another Near.
type Near[K, V any] struct {
	x         *Index[K, V]
	query     []float32
	probes    int
	hasProbes bool
	rescore   int
	rescoring bool
	exact     bool
	within    float32
	hasWithin bool
}

// Probes makes the search read the nearest n partitions; see Search.Probes.
func (n Near[K, V]) Probes(p int) Near[K, V] {
	n.probes, n.hasProbes = p, true
	return n
}

// Rescore makes the search re-rank r candidates of the codes; see Search.Rescore.
func (n Near[K, V]) Rescore(r int) Near[K, V] {
	n.rescore, n.rescoring = r, true
	return n
}

// Exact makes the search compare the query with every vector; see Search.Exact.
func (n Near[K, V]) Exact() Near[K, V] {
	n.exact = true
	return n
}

// Within makes the search return only the records whose distance is at most d; see Search.Within.
func (n Near[K, V]) Within(d float32) Near[K, V] {
	n.within, n.hasWithin = d, true
	return n
}

// IndexName returns the name of the index, which a query is hinted (Query.Using) with to have the
// search check the conditions on the records.
func (n Near[K, V]) IndexName() string {
	return n.x.name
}

// Describe says how the index searches, as the plan of a query shows it: "exact" for an index
// that has no partitions and for a search that is Exact, and otherwise "partitioned" or
// "partitioned, quantized", then the options that were set.
func (n Near[K, V]) Describe() string {
	parts := []string{"exact"}
	if !n.exact && n.x.size > 0 {
		parts = []string{"partitioned"}
		if n.x.rot != nil {
			parts[0] += ", quantized"
		}
		if n.hasProbes {
			parts = append(parts, fmt.Sprintf("probes %d", n.probes))
		}
		if n.rescoring {
			parts = append(parts, fmt.Sprintf("rescore %d", n.rescore))
		}
	}
	// What is within is a part of the order whichever way the vectors are compared.
	if n.hasWithin {
		parts = append(parts, fmt.Sprintf("within %g", n.within))
	}
	return strings.Join(parts, ", ")
}

// Nearest runs the search in a transaction, and returns the records nearest first: at most limit
// of them, or all if limit is negative, that satisfy all the conditions. If among is not nil only
// its records are candidates. It is closed whatever Nearest returns. The results are *Results,
// which tell the distance of each record.
func (n Near[K, V]) Nearest(
	tx kv.Tx,
	among iterutil.Iterator[K, *V],
	conds []cond.Cond[V],
	limit int,
) (iterutil.Iterator[K, *V], error) {
	s := n.x.Instantiate(tx).Search(n.query)
	if n.hasProbes {
		s = s.Probes(n.probes)
	}
	if n.rescoring {
		s = s.Rescore(n.rescore)
	}
	if n.exact {
		s = s.Exact()
	}
	if n.hasWithin {
		s = s.Within(n.within)
	}
	if among != nil {
		s = s.Among(among)
	}
	s = s.Where(conds...)
	if limit >= 0 {
		s = s.Limit(limit)
	}

	// Run closes the source on every path, and the search is described by now, so every error is
	// one of its.
	res, err := s.Run()
	if err != nil {
		return nil, err
	}
	return res, nil
}
