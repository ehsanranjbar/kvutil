package query

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
)

// The numbers that the planner estimates the cost of a plan with. There are no statistics of
// the data, so these are assumptions, and they are all here.
const (
	// storeSize is the number of records that a store is assumed to have.
	storeSize = 1_000_000

	// The fraction of the records that a comparison is assumed to be satisfied by.
	eqFraction        = 0.01 // a field equal to a value; a field in n values is n times this
	rangeBothFraction = 0.1  // a range with both sides
	rangeOneFraction  = 0.3  // a range with one side

	// entryCost is the cost of reading an entry of an index.
	entryCost = 1.0
	// fetchCost is the cost of getting a record and checking it.
	fetchCost = 4.0
	// sortCost is the factor of rows × log2(rows) that sorting costs, and that a top-N costs
	// with the number of rows that it keeps in place of the rows.
	sortCost = 0.2
	// residualPass is the fraction of the records that are found that is assumed to satisfy the
	// conditions that are checked on them. It is what a limit is divided by to know how many
	// records are read for it.
	residualPass = 0.5
)

// delivery is whether, and how, the records of an access path come in the order that is asked for.
type delivery int

const (
	// notDelivered means that the records have to be sorted.
	notDelivered delivery = iota
	// forward means that the records are read in the order that the entries are in.
	forward
	// reverse means that the records are read in the opposite order to the one that the entries
	// are in.
	reverse
	// single means that there is one record at most.
	single
)

// plan is how a query is run.
type plan[K, V any] struct {
	// empty is set if nothing is read: the conditions contradict each other, or nothing is asked for.
	empty bool

	// index is the index that is read, or nil if the store is scanned.
	index *index.Index[K, V]
	// ranges are the ranges of the keys of the index that are read, in the order that they are read.
	ranges []index.Bounds
	// reversed is set if the index is read from the end.
	reversed bool
	// check are the conditions that the records that are found are checked against.
	check []cond.Cond[V]
	// combo is set instead of index and ranges if the records are those of several indexes
	// together: their intersection or their union.
	combo *combo[K, V]

	// indexOnly is set if the rows are made from the entries of the index, without getting the
	// records: the index holds what is selected, what the check needs, and what the order needs
	// if the rows are sorted. The following are then set for the rows.
	indexOnly bool
	// accesses are where the index holds the selected items, in their order.
	accesses []*index.Access[V]
	// entryChecks are the check, on the entries.
	entryChecks []entryCheck[K, V]
	// sortAccesses are where the index holds the fields of the order, if the rows are sorted.
	sortAccesses []*index.Access[V]

	// order is the order that is asked for, and ordered is set if the records come in it.
	order []field.Component[V]
	// wanted is the order without the fields that have one value in all the records that are found.
	wanted  []field.Component[V]
	ordered bool
	// inOrder says that the index is what delivers the order, and not a scan of the store or a
	// single record.
	inOrder bool
	// topN is set if the first offset+limit records of a sort are kept only.
	topN bool

	offset, limit int
	limited       bool

	// cost is the estimated cost of the plan.
	cost float64
}

// candidate is a way to read the records that satisfy the conditions.
type candidate[K, V any] struct {
	// position is the position of the index among the extensions, and -1 for a scan. A candidate
	// that combines indexes comes after all of the others.
	position int
	index    *index.Index[K, V]
	ranges   []index.Bounds
	check    []cond.Cond[V]
	delivery delivery
	// indexOnly is set if the rows can be made from the index alone; see plan.
	indexOnly bool
	cost      float64

	// combo is set for a candidate that reads several indexes.
	combo *combo[K, V]
	// rows is the number of the records that the candidate is estimated to find, before its
	// conditions are checked.
	rows float64
	// residual are the conditions that the ranges of the index leave to be checked on the records,
	// which check is when the rows are made from the records.
	residual []cond.Cond[V]
	// served are the comparisons that the ranges of the index serve.
	served []*cond.Comparison[V]
	// plainCost is the cost of reading the records instead of making the rows from the index.
	plainCost float64
}

// selection is what a projection selects, which decides if an index can do without the records.
type selection[V any] struct {
	items []field.Item[V]
}

// less reports whether a candidate is preferred: it costs less, or else it has fewer conditions
// left to check, or else it reads fewer ranges, or else it comes first.
func (c *candidate[K, V]) less(o *candidate[K, V]) bool {
	if c.cost != o.cost {
		return c.cost < o.cost
	}
	// A plan of one index or of the store is simpler than one of several.
	if (c.combo == nil) != (o.combo == nil) {
		return c.combo == nil
	}
	if len(c.check) != len(o.check) {
		return len(c.check) < len(o.check)
	}
	if len(c.ranges) != len(o.ranges) {
		return len(c.ranges) < len(o.ranges)
	}
	return c.position < o.position
}

// plan chooses how to run the query. The selection is nil for a query that gives its records, and
// is what a projection selects otherwise, which an index may hold so that no record is read.
func (q *Query[K, V]) plan(sel *selection[V]) (*plan[K, V], error) {
	if q.offset < 0 {
		return nil, fmt.Errorf("%w: the offset is %d", ErrNegative, q.offset)
	}
	if q.limited && q.limit < 0 {
		return nil, fmt.Errorf("%w: the limit is %d", ErrNegative, q.limit)
	}

	if q.maxKeys < 0 {
		return nil, fmt.Errorf("%w: the number of keys is %d", ErrNegative, q.maxKeys)
	}
	order, err := q.orderFields()
	if err != nil {
		return nil, err
	}

	e, pl, err := q.prepare(sel, order)
	if err != nil || pl.empty {
		return pl, err
	}

	best, err := e.choose()
	if err != nil {
		return nil, err
	}
	e.fill(pl, best)
	return pl, nil
}

// orderFields returns the fields that the records are ordered by, each path once, or says why
// they cannot be.
func (q *Query[K, V]) orderFields() ([]field.Component[V], error) {
	var order []field.Component[V]
	for _, f := range q.order {
		if f == nil {
			return nil, fmt.Errorf("%w: a field is nil", ErrOrder)
		}
		if f.Multi() {
			return nil, fmt.Errorf("%w: %s may have several values for a record", ErrOrder, f.Path())
		}
		// What comes after a field of the same path never decides anything.
		if !slices.ContainsFunc(order, func(o field.Component[V]) bool { return o.Path() == f.Path() }) {
			order = append(order, f)
		}
	}
	return order, nil
}

// prepare checks the hints and normalises the conditions, and returns the estimator of the query
// and the plan to fill, which is empty if nothing is read.
func (q *Query[K, V]) prepare(sel *selection[V], order []field.Component[V]) (*estimator[K, V], *plan[K, V], error) {
	indexes := q.indexes()
	var hinted *index.Index[K, V]
	switch q.hint.kind {
	case hintIndex:
		for _, ix := range indexes {
			if ix.Name() == q.hint.index {
				hinted = ix
			}
		}
		if hinted == nil {
			return nil, nil, fmt.Errorf("%w: the store has no index %q", ErrHint, q.hint.index)
		}
	case hintAll:
		for _, name := range q.hint.indexes {
			if !slices.ContainsFunc(indexes, func(ix *index.Index[K, V]) bool { return ix.Name() == name }) {
				return nil, nil, fmt.Errorf("%w: the store has no index %q", ErrHint, name)
			}
		}
		if len(q.hint.indexes) == 0 {
			return nil, nil, fmt.Errorf("%w: no indexes are given to read", ErrHint)
		}
	}

	pl := &plan[K, V]{order: order, offset: q.offset, limit: q.limit, limited: q.limited}
	conj, empty := normalise(q.conds)
	if empty || (q.limited && q.limit == 0) {
		pl.empty = true
		return nil, pl, nil
	}

	// The fields that have one value in all the records that are found: their order is no order.
	constant := map[string]struct{}{}
	for _, c := range conj {
		if cmp, ok := c.(*cond.Comparison[V]); ok && !cmp.Multi && cmp.Err() == nil &&
			(cmp.Op == cond.OpEq || cmp.Op == cond.OpIn) && len(cmp.Values) == 1 {
			constant[cmp.Path] = struct{}{}
		}
	}
	var wanted []field.Component[V]
	for _, f := range order {
		if _, ok := constant[f.Path()]; !ok {
			wanted = append(wanted, f)
		}
	}

	pl.wanted = wanted
	e := &estimator[K, V]{
		q: q, sel: sel, order: order, conj: conj, constant: constant, wanted: wanted,
		unordered: len(order) == 0, indexes: indexes, hinted: hinted, maxKeys: q.maxKeys,
		hashed: !exactKeys(q.store),
	}
	return e, pl, nil
}

// choose returns the candidate that the query is run with: the cheapest, or the one that a hint says.
func (e *estimator[K, V]) choose() (*candidate[K, V], error) {
	q := e.q
	switch q.hint.kind {
	case hintScan:
		return e.scan(), nil

	case hintIndex:
		for position, ix := range e.indexes {
			if ix != e.hinted {
				continue
			}
			c, err := e.index(position, ix)
			if err != nil {
				return nil, fmt.Errorf("%w: the index %q cannot serve the query: %v", ErrHint, ix.Name(), err)
			}
			return c, nil
		}

	case hintAll:
		return e.chooseAll()
	}

	best := e.scan()
	singles := e.singles()
	for _, c := range singles {
		if c.less(best) {
			best = c
		}
	}
	for _, c := range e.combined(singles) {
		if c.less(best) {
			best = c
		}
	}
	return best, nil
}

// fill sets the plan to read the records the way a candidate says.
func (e *estimator[K, V]) fill(pl *plan[K, V], best *candidate[K, V]) {
	sel, order := e.sel, e.order
	pl.index, pl.ranges, pl.check, pl.cost = best.index, best.ranges, best.check, best.cost
	pl.ordered = best.delivery != notDelivered
	pl.inOrder = (best.delivery == forward || best.delivery == reverse) && len(e.wanted) > 0
	pl.reversed = best.delivery == reverse
	pl.topN = !pl.ordered && e.q.limited
	if pl.reversed {
		pl.ranges = slices.Clone(pl.ranges)
		slices.Reverse(pl.ranges)
	}
	if best.combo != nil {
		pl.combo = best.combo.reading(pl.reversed)
	}
	if best.indexOnly {
		pl.indexOnly = true
		pl.accesses = make([]*index.Access[V], len(sel.items))
		for i, item := range sel.items {
			pl.accesses[i], _ = best.index.Access(item)
		}
		if !pl.ordered {
			pl.sortAccesses = make([]*index.Access[V], len(order))
			for i, f := range order {
				pl.sortAccesses[i], _ = best.index.AccessField(f.Path(), f.CodecName())
			}
		}
		for _, c := range pl.check {
			pl.entryChecks = append(pl.entryChecks, compileCheck(best.index, c))
		}
	}
}

// entryCheck checks a condition on the entry that an iterator of an index is at.
type entryCheck[K, V any] func(it *index.Iterator[K, V]) (bool, error)

// compileCheck returns the check of a condition that the index can check, which holdsCondition says.
func compileCheck[K, V any](ix *index.Index[K, V], c cond.Cond[V]) entryCheck[K, V] {
	switch c := c.(type) {
	case *cond.Comparison[V]:
		access, _ := ix.AccessField(c.Path, c.Codec)
		return func(it *index.Iterator[K, V]) (bool, error) {
			values, err := it.Encoded(access)
			if err != nil {
				return false, err
			}
			return c.MatchKeys(values), nil
		}

	case *cond.Conjunction[V]:
		parts := compileChecks(ix, c.Conds)
		return func(it *index.Iterator[K, V]) (bool, error) {
			for _, part := range parts {
				if ok, err := part(it); !ok || err != nil {
					return false, err
				}
			}
			return true, nil
		}

	case *cond.Disjunction[V]:
		parts := compileChecks(ix, c.Conds)
		return func(it *index.Iterator[K, V]) (bool, error) {
			for _, part := range parts {
				if ok, err := part(it); ok || err != nil {
					return ok, err
				}
			}
			return false, nil
		}

	case *cond.Negation[V]:
		inner := compileCheck(ix, c.Cond)
		return func(it *index.Iterator[K, V]) (bool, error) {
			ok, err := inner(it)
			return !ok && err == nil, err
		}
	}
	panic(fmt.Sprintf("query: the condition %s cannot be checked on an index", c))
}

func compileChecks[K, V any](ix *index.Index[K, V], conds []cond.Cond[V]) []entryCheck[K, V] {
	checks := make([]entryCheck[K, V], len(conds))
	for i, c := range conds {
		checks[i] = compileCheck(ix, c)
	}
	return checks
}

// holdsCondition reports whether an index holds what a condition needs, so that the condition can
// be checked on its entries with the same result as on the record: it is a comparison of a field of
// the key that has one value at most for a record, or of a field that the index includes (which
// holds all the values of the record), or a conjunction, a disjunction or a negation of such.
// A function needs the record, and so does a comparison that has an error, which Match returns.
func holdsCondition[K, V any](ix *index.Index[K, V], c cond.Cond[V]) bool {
	switch c := c.(type) {
	case *cond.Comparison[V]:
		if c.Err() != nil {
			return false
		}
		access, ok := ix.AccessField(c.Path, c.Codec)
		return ok && !access.ByEntry()
	case *cond.Conjunction[V]:
		return holdsConditions(ix, c.Conds)
	case *cond.Disjunction[V]:
		return holdsConditions(ix, c.Conds)
	case *cond.Negation[V]:
		return holdsCondition(ix, c.Cond)
	}
	return false
}

// indexes returns the indexes of the store, in the order of its extensions.
func (q *Query[K, V]) indexes() []*index.Index[K, V] {
	var indexes []*index.Index[K, V]
	for _, ext := range q.store.Extensions() {
		if x, ok := ext.(interface{ Index() *index.Index[K, V] }); ok {
			indexes = append(indexes, x.Index())
		}
	}
	return indexes
}

// holdsConditions reports whether an index holds what every one of the conditions needs.
func holdsConditions[K, V any](ix *index.Index[K, V], conds []cond.Cond[V]) bool {
	for _, c := range conds {
		if !holdsCondition(ix, c) {
			return false
		}
	}
	return true
}

// canDoWithout reports whether the rows of the selection can be made from the entries of an index
// that is read with the conditions left to check, and delivers the order that is asked for or
// not. It can if the index holds, as one value of the record, every selected item; every one of
// the conditions; and, if the rows have to be sorted, every field of the order.
func (e *estimator[K, V]) canDoWithout(ix *index.Index[K, V], residual []cond.Cond[V], d delivery) bool {
	if e.sel == nil {
		return false
	}
	for _, item := range e.sel.items {
		if access, ok := ix.Access(item); !ok || access.ByEntry() {
			return false
		}
	}
	if !holdsConditions(ix, residual) {
		return false
	}
	if d == notDelivered {
		for _, f := range e.order {
			if access, ok := ix.AccessField(f.Path(), f.CodecName()); !ok || access.ByEntry() {
				return false
			}
		}
	}
	return true
}

// estimator estimates the cost of the ways to read the records of a query.
type estimator[K, V any] struct {
	q *Query[K, V]
	// sel is what is selected, which is nil if the records are asked for.
	sel *selection[V]
	// order is the order that is asked for, with each path once.
	order []field.Component[V]
	conj  []cond.Cond[V]
	// constant are the paths of the fields that all the records that are found have one value of.
	constant map[string]struct{}
	// wanted is the order that is asked for, without the fields that are constant.
	wanted []field.Component[V]
	// unordered is set if the query has no order, so any order of the records will do.
	unordered bool

	// indexes are the indexes of the store, and hinted the one that Using says.
	indexes []*index.Index[K, V]
	hinted  *index.Index[K, V]
	// maxKeys is the number of keys that a plan may be estimated to hold.
	maxKeys int
	// hashed is set if the keys of a plan that holds keys are hashes, as the primary keys are not
	// numbers.
	hashed bool
	// noLimit is set for the estimate of a part of a plan, which is read in full whatever the
	// limit of the query is, as the limit is for what the parts make together.
	noLimit bool
}

// scan returns the candidate of scanning the store.
func (e *estimator[K, V]) scan() *candidate[K, V] {
	c := &candidate[K, V]{position: -1, check: e.conj}
	if e.unordered || len(e.wanted) == 0 {
		// The records of the store come in the order of their keys.
		c.delivery = forward
	}
	c.cost = e.cost(c, storeSize, false)
	return c
}

// index returns the candidate of reading an index, or says why it cannot be.
func (e *estimator[K, V]) index(position int, ix *index.Index[K, V]) (*candidate[K, V], error) {
	desc := ix.Descriptor()
	// The fraction of the records of the store that an index of some of them has.
	kept := 1.0
	if desc.Where != "" {
		cnd, ok := desc.Condition.(cond.Cond[V])
		if !ok {
			return nil, fmt.Errorf("it has the records that satisfy %s only, which is not a condition that is known", desc.Where)
		}
		if !implies(e.conj, cnd) {
			return nil, fmt.Errorf("it has the records that satisfy %s only, which the conditions do not imply", desc.Where)
		}
		kept = conditionFraction(cnd)
	}
	planner, ok := ix.Indexer().(index.RangePlanner[V])
	if !ok {
		return nil, errors.New("it cannot be looked up with conditions")
	}
	// An index has no entry for a record that has no value for one of its fields, so it is only
	// read if none of the records that the query asks for lacks one.
	if err := ix.Complete(e.conj...); err != nil {
		return nil, err
	}
	ranges, residual, err := planner.Ranges(e.conj...)
	if err != nil {
		return nil, err
	}
	// The fields of an index that is not made of fields are not known, so it is read if a
	// condition narrows it only.
	if ix.Fields() == nil && !narrows(ranges) {
		return nil, errors.New("no condition narrows it")
	}
	ranges = sortRanges(ranges)

	// The comparisons that the index serves are the ones that it does not leave to be checked.
	left := map[*cond.Comparison[V]]struct{}{}
	for _, r := range residual {
		if cmp, ok := r.(*cond.Comparison[V]); ok {
			left[cmp] = struct{}{}
		}
	}
	served := map[string]*cond.Comparison[V]{}
	var servedList []*cond.Comparison[V]
	fraction := 1.0
	for _, c := range e.conj {
		cmp, ok := c.(*cond.Comparison[V])
		if !ok {
			continue
		}
		if _, ok := left[cmp]; ok {
			continue
		}
		served[cmp.Path] = cmp
		servedList = append(servedList, cmp)
		fraction *= fractionOf(cmp)
	}

	fields := ix.Fields()
	exact := fields != nil
	for _, f := range fields {
		if cmp, ok := served[f.Path()]; !ok || cmp.Op == cond.OpRange {
			exact = false
		}
	}

	c := &candidate[K, V]{position: position, index: ix, ranges: ranges, check: residual, residual: residual, served: servedList}
	rows := math.Max(fraction*kept, 1.0/storeSize) * storeSize
	if desc.Unique && exact {
		// Every field has a value or several, so a record is found by each of the ranges at most.
		rows = float64(len(ranges))
	}
	c.rows = rows
	c.delivery = e.deliver(ix, fields, ranges, exact)
	if e.sel != nil {
		// Every entry of an index of some of the records satisfies its condition, so the
		// conditions that it implies need no check on the entries.
		rest := residual
		if cnd, ok := desc.Condition.(cond.Cond[V]); ok && desc.Where != "" {
			rest = slices.DeleteFunc(slices.Clone(residual), func(r cond.Cond[V]) bool {
				return implies([]cond.Cond[V]{cnd}, r)
			})
		}
		if e.canDoWithout(ix, rest, c.delivery) {
			c.indexOnly, c.check = true, rest
		}
	}
	c.cost = e.cost(c, rows, true)
	c.plainCost = c.cost
	if c.indexOnly {
		plain := *c
		plain.indexOnly, plain.check = false, residual
		c.plainCost = e.cost(&plain, rows, true)
	}
	return c, nil
}

// conditionFraction returns the fraction of the records that the condition of an index is assumed
// to be satisfied by, which are the ones that the index has. A part that is not a comparison is
// assumed to be satisfied by half of them.
func conditionFraction[V any](c cond.Cond[V]) float64 {
	conj, empty := normalise([]cond.Cond[V]{c})
	if empty {
		return 1.0 / storeSize
	}
	fraction := 1.0
	for _, part := range conj {
		if cmp, ok := part.(*cond.Comparison[V]); ok {
			fraction *= fractionOf(cmp)
		} else {
			fraction *= residualPass
		}
	}
	return fraction
}

// fractionOf returns the fraction of the records that a comparison is assumed to be satisfied by.
func fractionOf[V any](c *cond.Comparison[V]) float64 {
	switch c.Op {
	case cond.OpEq:
		return eqFraction
	case cond.OpIn:
		return math.Min(1, float64(len(c.Values))*eqFraction)
	}
	switch {
	case c.Low != nil && c.High != nil:
		return rangeBothFraction
	case c.Low != nil || c.High != nil:
		return rangeOneFraction
	}
	return 1
}

// cost estimates the cost of reading the records of a candidate, given the number of the
// records that it finds before its conditions are checked.
func (e *estimator[K, V]) cost(c *candidate[K, V], rows float64, indexed bool) float64 {
	pass := 1.0
	if len(c.check) > 0 {
		pass = residualPass
	}

	limited := e.q.limited && !e.noLimit
	read := rows
	if c.delivery != notDelivered && limited {
		// The records come in order, so no more are read than the page needs.
		need := float64(e.q.offset) + float64(e.q.limit)
		read = math.Min(rows, math.Ceil(need/pass))
	}
	// The records are not got where the rows are made from the index.
	cost := 0.0
	if !c.indexOnly {
		cost = read * fetchCost
	}
	if indexed {
		cost += read * entryCost
	}

	if c.delivery == notDelivered {
		sorted := rows * pass
		kept := sorted
		if limited {
			kept = math.Min(kept, float64(e.q.offset)+float64(e.q.limit))
		}
		cost += sorted * math.Log2(math.Max(kept, 2)) * sortCost
	}
	return cost
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

// sortRanges returns the ranges in the order of their keys.
func sortRanges(ranges []index.Bounds) []index.Bounds {
	sorted := slices.Clone(ranges)
	slices.SortStableFunc(sorted, func(a, b index.Bounds) int {
		la, _, oka := a.Low()
		lb, _, okb := b.Low()
		switch {
		case !oka && !okb:
			return 0
		case !oka:
			return -1
		case !okb:
			return 1
		}
		return bytes.Compare(la, lb)
	})
	return sorted
}

// disjoint reports whether ranges that are sorted are each before the next one, so that reading
// one after the other reads the entries in the order of their keys.
func disjoint(ranges []index.Bounds) bool {
	for i := 1; i < len(ranges); i++ {
		high, _, ok := ranges[i-1].High()
		low, _, ok2 := ranges[i].Low()
		// A bound that is the beginning of a key stands for the keys that begin with it.
		if !ok || !ok2 || bytes.Compare(high, low) >= 0 || bytes.HasPrefix(low, high) {
			return false
		}
	}
	return true
}

// deliver says whether the entries of an index, read from the ranges, come in the order that
// the query asks for, which is the order of the fields and then the order of the primary keys.
//
// The entries are in the order of the fields of the index, and then of the primary keys. So the
// records are in order if the fields that are asked for are the ones of the index, in its
// order: the fields that have one value in all the records that are found are in any order,
// and are left out of both. They come in the opposite order when all the fields are in the
// opposite direction, but only if no two records can have the same fields, as the primary keys
// would then be in the opposite order too. The records of an index with several entries for a
// record come where the first entry of the record is, which is not an order.
func (e *estimator[K, V]) deliver(ix *index.Index[K, V], fields []field.Component[V], ranges []index.Bounds, exact bool) delivery {
	desc := ix.Descriptor()
	if e.unordered {
		// Any order will do, and the entries have one.
		return forward
	}
	if desc.MultiEntry || fields == nil {
		return notDelivered
	}
	if desc.Unique && exact && len(ranges) == 1 {
		return single
	}
	if !disjoint(ranges) {
		return notDelivered
	}

	var rest []field.Component[V]
	for _, f := range fields {
		if _, ok := e.constant[f.Path()]; !ok {
			rest = append(rest, f)
		}
	}
	if len(rest) != len(e.wanted) {
		return notDelivered
	}

	same, opposite := true, true
	for i, f := range rest {
		w := e.wanted[i]
		if f.Path() != w.Path() || f.CodecName() != w.CodecName() {
			return notDelivered
		}
		if f.Descending() == w.Descending() {
			opposite = false
		} else {
			same = false
		}
	}
	switch {
	case same:
		return forward
	case opposite && desc.Unique:
		return reverse
	}
	return notDelivered
}

// orderText returns an order as text.
func orderText[V any](order []field.Component[V]) string {
	if len(order) == 0 {
		return "primary key"
	}
	parts := make([]string, 0, len(order))
	for _, f := range order {
		if f.Descending() {
			parts = append(parts, f.Path()+" desc")
		} else {
			parts = append(parts, f.Path())
		}
	}
	return strings.Join(parts, ", ")
}

// conditionsText returns conditions as text.
func conditionsText[V any](conds []cond.Cond[V]) string {
	texts := make([]string, 0, len(conds))
	for _, c := range conds {
		texts = append(texts, c.String())
	}
	return strings.Join(texts, "; ")
}
