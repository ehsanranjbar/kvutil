package query

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
)

// A plan can read several indexes for one query, and combine what they find by the primary keys
// of the records:
//
//   - An intersection serves a conjunction with the indexes that serve its different conditions.
//     One index is streamed, and the primary keys of the others are remembered; a record of the
//     streamed index is fetched only if all the others have it. Every index is read with the
//     ranges of the conditions that it serves, so what each finds is a superset of what the
//     query asks for, and the records that are fetched are checked against all the conditions
//     that the streamed index does not serve, which is what makes the result exact.
//   - A union serves a disjunction that is one of the conditions of the query: each of its
//     branches is read with an index, with the conditions of the branch and the other conditions
//     of the query, so each branch is exact, and the records are returned once each. The
//     records come in no order.
//
// The keys that a plan remembers are held in memory, so a plan that is estimated to hold more
// than Query.MaxKeys is not made.

const (
	// maxMembers is the most indexes that an intersection reads.
	maxMembers = 4
	// comboPosition is the position of the candidates that read several indexes, which come after
	// all of the others.
	comboPosition = 1 << 20
	// maxAssignments is the most ways to read the branches of a disjunction that are tried to find
	// the cheapest of those that read the indexes that a hint says.
	maxAssignments = 4096
)

// part is an index that a combination reads.
type part[K, V any] struct {
	index  *index.Index[K, V]
	ranges []index.Bounds
	// conds are the conditions that the index is looked up with, for a union.
	conds []cond.Cond[V]
	// check are the conditions that the records that are found are checked against.
	check []cond.Cond[V]
}

// combo is the reading of several indexes together.
type combo[K, V any] struct {
	// union is set for a union of the parts, and not set for an intersection, which streams the
	// first part and remembers the keys of the others.
	union bool
	// hashed is set if the keys that are remembered are hashes, as the primary keys are not numbers.
	hashed bool
	parts  []part[K, V]
	// keys is the number of keys that the combination is estimated to remember.
	keys float64
}

// reading returns the combination for a plan that reads the streamed part from its end or not.
func (c *combo[K, V]) reading(reversed bool) *combo[K, V] {
	if !reversed || c.union {
		return c
	}
	r := *c
	r.parts = slices.Clone(c.parts)
	r.parts[0].ranges = slices.Clone(r.parts[0].ranges)
	slices.Reverse(r.parts[0].ranges)
	return &r
}

// open starts the indexes in a transaction, and combines what they find. The first part of an
// intersection is read from its end if the plan says so.
func (c *combo[K, V]) open(tx kv.Tx, reversed bool) (index.Keyed[K, V], error) {
	its := make([]index.Keyed[K, V], 0, len(c.parts))
	closeAll := func() {
		for _, it := range its {
			it.Close()
		}
	}

	for i, p := range c.parts {
		ins := p.index.Instantiate(tx)
		var (
			it  *index.Iterator[K, V]
			err error
		)
		if c.union {
			it, err = ins.Lookup(p.conds...)
		} else {
			it, err = ins.ScanAll(p.ranges, reversed && i == 0)
		}
		if err != nil {
			closeAll()
			return nil, fmt.Errorf("index %q: %w", p.index.Name(), err)
		}
		its = append(its, it)
	}

	combiner := index.Combiner{}
	if c.hashed {
		combiner = index.Lossy
	}
	var (
		combined index.Keyed[K, V]
		err      error
	)
	if c.union {
		combined, err = combiner.Union(its...)
	} else {
		combined, err = combiner.Intersect(its...)
	}
	if err != nil {
		closeAll()
		return nil, err
	}
	return combined, nil
}

// tree returns the operators of the combination as text.
func (c *combo[K, V]) tree(pl *plan[K, V]) *treeNode {
	if c.union {
		n := &treeNode{text: "Union"}
		if c.hashed {
			n.text += " (hashed)"
		}
		for _, p := range c.parts {
			text := "IndexLookup " + p.index.Name() + rangesText(len(p.ranges))
			if len(p.check) > 0 {
				text += checkText(p.check)
			}
			n.children = append(n.children, &treeNode{text: text})
		}
		return n
	}

	n := &treeNode{text: "Intersect"}
	for i, p := range c.parts {
		if i == 0 {
			text := "IndexLookup " + p.index.Name() + pl.orderText() + rangesText(len(p.ranges))
			if len(pl.check) > 0 {
				text += checkText(pl.check)
			}
			n.children = append(n.children, &treeNode{text: text})
			continue
		}
		text := "IndexKeys " + p.index.Name() + rangesText(len(p.ranges))
		if c.hashed {
			text += " (hashed)"
		}
		n.children = append(n.children, &treeNode{text: text})
	}
	return n
}

// exactKeys reports whether the primary keys of a store can be remembered exactly in a bitmap:
// they all have the same size, of 8 bytes at most, as numbers have. Other keys are remembered
// as hashes, and a hash that is known is checked against the record.
func exactKeys[K, V any](store Store[K, V]) bool {
	var zero K
	c := store.KeyCodec()
	whole, err := c.Encode(zero)
	if err != nil {
		return false
	}
	// A key of a fixed size is the same whether it has to mark where it ends or not.
	marked, err := c.Append(nil, zero)
	if err != nil {
		return false
	}
	return len(whole) <= 8 && len(whole) == len(marked)
}

// singles returns the candidates of reading one index, for the indexes that can serve the query.
func (e *estimator[K, V]) singles() []*candidate[K, V] {
	var singles []*candidate[K, V]
	for position, ix := range e.indexes {
		if c, err := e.index(position, ix); err == nil {
			singles = append(singles, c)
		}
	}
	return singles
}

// combined returns the candidates that read several indexes, which the planner considers: the
// cheapest union for each disjunction that is a condition of the query, and an intersection that
// is built greedily from the candidates of one index.
func (e *estimator[K, V]) combined(singles []*candidate[K, V]) []*candidate[K, V] {
	var combined []*candidate[K, V]
	if c := e.greedyIntersection(singles); c != nil {
		combined = append(combined, c)
	}
	return append(combined, e.unions(nil, false)...)
}

// ---- intersections

// intersection returns the candidate of the intersection of the members, with one of them
// streamed, or nil if that is not possible: every member but the first must serve a condition
// that the ones before it do not, and the keys that are remembered must be few enough.
func (e *estimator[K, V]) intersection(members []*candidate[K, V], streamed int) *candidate[K, V] {
	ordered := make([]*candidate[K, V], 0, len(members))
	ordered = append(ordered, members[streamed])
	for i, m := range members {
		if i != streamed {
			ordered = append(ordered, m)
		}
	}

	// The fraction of the records that all of the members find: each finds what its conditions
	// say, less what the ones before it said of the same conditions.
	var (
		seen       = map[*cond.Comparison[V]]struct{}{}
		fraction   = 1.0
		remembered = 0.0
		entries    = 0.0
	)
	for i, m := range ordered {
		f := m.rows / storeSize
		shared, fresh := 1.0, false
		for _, c := range m.served {
			if _, ok := seen[c]; ok {
				shared *= fractionOf(c)
			} else {
				fresh = true
			}
			seen[c] = struct{}{}
		}
		if i > 0 {
			if !fresh {
				return nil
			}
			remembered += m.rows
			entries += m.rows
		}
		fraction *= math.Min(1, math.Max(f/shared, 1.0/storeSize))
	}
	if remembered > float64(e.maxKeys) {
		return nil
	}

	st := ordered[0]
	rows := math.Min(math.Max(fraction*storeSize, 1), st.rows)

	// The records are in order of the streamed index, so no more are read than the page needs.
	delivered := st.delivery != notDelivered
	pass := 1.0
	if len(st.residual) > 0 {
		pass = residualPass
	}
	read := st.rows
	if delivered && e.q.limited {
		need := float64(e.q.offset) + float64(e.q.limit)
		found := math.Min(rows, math.Ceil(need/pass))
		read = math.Min(st.rows, found*st.rows/rows)
	}

	// Where no record is looked at, which the count of a query is, none is fetched.
	keysOnly := e.sel != nil && len(e.sel.items) == 0 && len(st.residual) == 0 && delivered
	records := &candidate[K, V]{delivery: st.delivery, check: st.residual, indexOnly: keysOnly}
	cost := e.cost(records, rows, false) + (read+entries)*entryCost

	c := &candidate[K, V]{
		position: comboPosition,
		check:    st.residual,
		delivery: st.delivery,
		rows:     rows,
		cost:     cost,
		combo:    &combo[K, V]{hashed: e.hashed, keys: remembered},
	}
	c.plainCost = cost
	for _, m := range ordered {
		c.combo.parts = append(c.combo.parts, part[K, V]{index: m.index, ranges: m.ranges, check: m.residual})
	}
	return c
}

// bestIntersection returns the cheapest intersection of the members, streaming the one that
// makes it cheapest, or the one with most records, which keeps fewer keys in memory.
func (e *estimator[K, V]) bestIntersection(members []*candidate[K, V]) *candidate[K, V] {
	var best *candidate[K, V]
	var bestStreamed *candidate[K, V]
	for s, m := range members {
		c := e.intersection(members, s)
		if c == nil {
			continue
		}
		if best == nil || c.cost < best.cost || (c.cost == best.cost && m.rows > bestStreamed.rows) {
			best, bestStreamed = c, m
		}
	}
	return best
}

// greedyIntersection returns the intersection that starts with the cheapest candidate of one
// index, and adds the index that lowers the cost most while there is one that lowers it.
func (e *estimator[K, V]) greedyIntersection(singles []*candidate[K, V]) *candidate[K, V] {
	if e.maxKeys <= 0 || len(singles) < 2 {
		return nil
	}

	start := singles[0]
	for _, c := range singles[1:] {
		if c.plainCost < start.plainCost {
			start = c
		}
	}

	var (
		members = []*candidate[K, V]{start}
		cost    = start.plainCost
		result  *candidate[K, V]
	)
	for len(members) < maxMembers {
		var added, best *candidate[K, V]
		for _, c := range singles {
			if len(c.served) == 0 || slices.Contains(members, c) {
				continue
			}
			x := e.bestIntersection(append(slices.Clone(members), c))
			if x != nil && (best == nil || x.cost < best.cost) {
				added, best = c, x
			}
		}
		if best == nil || best.cost >= cost {
			break
		}
		members, cost, result = append(members, added), best.cost, best
	}
	return result
}

// ---- unions

// branchOption is a way to read a branch of a disjunction: with an index, and the conditions
// that the branch is looked up with.
type branchOption[K, V any] struct {
	candidate *candidate[K, V]
	conds     []cond.Cond[V]
}

// branchEstimator returns the estimator of a part of a plan that is read in full, in no order,
// for the conditions.
func (e *estimator[K, V]) branchEstimator(conj []cond.Cond[V]) *estimator[K, V] {
	return &estimator[K, V]{
		q: e.q, conj: conj, constant: map[string]struct{}{}, unordered: true,
		indexes: e.indexes, maxKeys: e.maxKeys, hashed: e.hashed, noLimit: true,
	}
}

// branchOptions returns for each branch of a disjunction, from the cheapest, the indexes that can
// read it with the conditions of the branch and the other conditions of the query. The indexes
// are those whose names are in the set, if there is one. It says no if a branch has none. A
// branch that no record can satisfy is left out.
func (e *estimator[K, V]) branchOptions(or *cond.Disjunction[V], others []cond.Cond[V], names map[string]struct{}) ([][]branchOption[K, V], bool) {
	var branches [][]branchOption[K, V]
	for _, b := range or.Conds {
		conds, empty := normalise(slices.Concat(conjuncts(b), others))
		if empty {
			continue
		}

		var options []branchOption[K, V]
		for _, c := range e.branchEstimator(conds).singles() {
			if _, ok := names[c.index.Name()]; names != nil && !ok {
				continue
			}
			options = append(options, branchOption[K, V]{candidate: c, conds: conds})
		}
		if len(options) == 0 {
			return nil, false
		}
		slices.SortStableFunc(options, func(a, b branchOption[K, V]) int {
			switch {
			case a.candidate.less(b.candidate):
				return -1
			case b.candidate.less(a.candidate):
				return 1
			}
			return 0
		})
		branches = append(branches, options)
	}
	return branches, len(branches) > 0
}

// unions returns the candidates of reading the disjunctions that are conditions of the query as
// unions. For each disjunction it is the union of its branches each read in the cheapest way,
// or, if there are names, the cheapest that reads exactly the indexes of the names. All the ways
// that are worth trying are returned if enumerating, which is for tests.
func (e *estimator[K, V]) unions(names map[string]struct{}, enumerate bool) []*candidate[K, V] {
	if e.maxKeys <= 0 {
		return nil
	}

	var unions []*candidate[K, V]
	for i, c := range e.conj {
		or, ok := c.(*cond.Disjunction[V])
		if !ok || len(or.Conds) < 2 {
			continue
		}
		others := slices.Concat(e.conj[:i], e.conj[i+1:])
		branches, ok := e.branchOptions(or, others, names)
		if !ok {
			continue
		}

		if names == nil && !enumerate {
			if u := e.union(branches, make([]int, len(branches))); u != nil {
				unions = append(unions, u)
			}
			continue
		}

		// Every way to choose an index for each branch, as many as are tried.
		limit, width := maxAssignments, math.MaxInt
		if enumerate {
			limit, width = 48, 3
		}
		var best *candidate[K, V]
		pick := make([]int, len(branches))
		var walk func(b int)
		walk = func(b int) {
			if limit <= 0 {
				return
			}
			if b == len(branches) {
				limit--
				if names != nil {
					used := map[string]struct{}{}
					for i, o := range pick {
						used[branches[i][o].candidate.index.Name()] = struct{}{}
					}
					if len(used) != len(names) {
						return
					}
				}
				u := e.union(branches, pick)
				switch {
				case u == nil:
				case enumerate:
					unions = append(unions, u)
				case best == nil || u.less(best):
					best = u
				}
				return
			}
			for o := 0; o < len(branches[b]) && o < width; o++ {
				pick[b] = o
				walk(b + 1)
			}
		}
		walk(0)
		if best != nil {
			unions = append(unions, best)
		}
	}
	return unions
}

// union returns the candidate of the union of the branches, each read in the way that the
// pick says, or nil if it would hold too many keys.
func (e *estimator[K, V]) union(branches [][]branchOption[K, V], pick []int) *candidate[K, V] {
	var (
		rows, cost float64
		parts      []part[K, V]
	)
	for i, options := range branches {
		o := options[pick[i]]
		rows += o.candidate.rows
		cost += o.candidate.plainCost
		parts = append(parts, part[K, V]{
			index: o.candidate.index, ranges: o.candidate.ranges, conds: o.conds, check: o.candidate.residual,
		})
	}
	rows = math.Min(rows, storeSize)
	if rows > float64(e.maxKeys) {
		return nil
	}

	c := &candidate[K, V]{
		position: comboPosition,
		rows:     rows,
		combo:    &combo[K, V]{union: true, hashed: e.hashed, parts: parts, keys: rows},
	}
	need := float64(e.q.offset) + float64(e.q.limit)
	if e.unordered {
		// The records are in no order, so a limit stops the reading early.
		c.delivery = forward
		if e.q.limited {
			cost *= math.Min(1, math.Max(need, 1)/rows)
		}
	} else {
		kept := rows
		if e.q.limited {
			kept = math.Min(kept, need)
		}
		cost += rows * math.Log2(math.Max(kept, 2)) * sortCost
	}
	c.cost, c.plainCost = cost, cost
	return c
}

// ---- hints and tests

// chooseAll returns the candidate of the hint to read all of some indexes: an intersection of
// them or a union of them.
func (e *estimator[K, V]) chooseAll() (*candidate[K, V], error) {
	names := map[string]struct{}{}
	for _, name := range e.q.hint.indexes {
		names[name] = struct{}{}
	}

	var best *candidate[K, V]
	consider := func(c *candidate[K, V]) {
		if c != nil && (best == nil || c.less(best)) {
			best = c
		}
	}

	var members []*candidate[K, V]
	for _, c := range e.singles() {
		if _, ok := names[c.index.Name()]; ok {
			members = append(members, c)
		}
	}
	if len(members) == len(names) && len(members) > 1 {
		consider(e.bestIntersection(members))
	}
	for _, u := range e.unions(names, false) {
		consider(u)
	}

	if best == nil {
		return nil, fmt.Errorf("%w: the indexes %s cannot be read together to serve the query, as an intersection or as a union, "+
			"within %d keys", ErrHint, strings.Join(e.q.hint.indexes, ", "), e.maxKeys)
	}
	return best, nil
}

// allCombined returns the candidates that read several indexes that are worth trying, whatever
// they cost and however many keys they hold: the ones that the planner considers, every union
// of the disjunctions that are conditions of the query with a few ways to read the branches,
// and the intersections of up to maxMembers of the indexes, each with every index streamed.
func (e *estimator[K, V]) allCombined() []*candidate[K, V] {
	e.maxKeys = math.MaxInt
	singles := e.singles()

	all := e.combined(singles)
	all = append(all, e.unions(nil, true)...)

	var eligible []*candidate[K, V]
	for _, c := range singles {
		if len(c.served) > 0 {
			eligible = append(eligible, c)
		}
	}
	limit := 40
	var subsets func(from int, members []*candidate[K, V])
	subsets = func(from int, members []*candidate[K, V]) {
		if len(members) >= 2 {
			for s := range members {
				if c := e.intersection(members, s); c != nil && limit > 0 {
					limit--
					all = append(all, c)
				}
			}
		}
		if len(members) == maxMembers {
			return
		}
		for i := from; i < len(eligible); i++ {
			subsets(i+1, append(slices.Clone(members), eligible[i]))
		}
	}
	subsets(0, nil)
	return all
}
