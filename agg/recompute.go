package agg

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Recompute defines an aggregation on a store that is kept by computing the state of a span
// again from what the records in it contribute, whenever one of them changes.
//
// Writing a record stores what it contributes and marks its span as changed, and the collector
// computes the states of the marked spans. The function can be any function, as it is given
// all the contributions of a span, in the order of their times. The cost of a change is that
// its whole span is computed again, so the spans should not be large: WithMerge keeps them
// small by storing the first bucket only and putting the others together from it.
func Recompute[K, V, X, S any](
	store extstore.Extensible[K, V],
	name string,
	extract Extract[V, X],
	compute func(contributions iters.Seq[time.Time, X]) (S, error),
	opts ...Option,
) *Agg[K, V, X, S] {
	return define(store, name, "recompute", extract, opts, func(c *core[X, S], o options) kind[X, S] {
		if o.appendOnly {
			panic(fmt.Sprintf("agg %q: AppendOnly is for delta aggregations", name))
		}

		r := &recomputeKind[X, S]{
			core:    c,
			compute: compute,
			merge:   option[func(a, b S) S](name, "the function of WithMerge", o.merge),
			stored:  c.buckets,
		}
		if r.merge != nil {
			// The other buckets are put together from the first one, which must fit in them.
			r.stored = c.buckets[:1]
			for _, b := range c.buckets[1:] {
				if !nests(c.buckets[0], b) {
					panic(fmt.Sprintf("agg %q: the spans of %s cannot be put together from the spans of %s",
						name, b.Name(), c.buckets[0].Name()))
				}
			}
		}
		return r
	})
}

type recomputeKind[X, S any] struct {
	*core[X, S]
	compute func(iters.Seq[time.Time, X]) (S, error)
	merge   func(a, b S) S
	stored  []Bucket // the buckets whose states are stored
}

func (r *recomputeKind[X, S]) isStored(b Bucket) bool {
	return slices.ContainsFunc(r.stored, func(s Bucket) bool { return s.ID() == b.ID() })
}

// change implements the kind interface.
func (r *recomputeKind[X, S]) change(tx *prefix.Tx, rawKey []byte, old, new *contribution[X]) error {
	if old != nil {
		key := append(appendTime(groupPrefix(contributionKeys, old.group), old.time), rawKey...)
		if err := tx.Delete(key); err != nil {
			return err
		}
	}
	if new != nil {
		key := append(appendTime(groupPrefix(contributionKeys, new.group), new.time), rawKey...)
		if err := tx.Set(key, new.xbz); err != nil {
			return err
		}
	}

	// The spans that the record was in and is in now have to be computed again. A marker is the
	// same key for every change of a span, and it is written without being read first.
	for _, c := range []*contribution[X]{old, new} {
		if c == nil {
			continue
		}
		for _, b := range r.stored {
			marker := appendTime(bucketPrefix(markerKeys, b, c.group), b.Start(c.time))
			if err := tx.Set(marker, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// contributions iterates over what the records of a group contribute, by their times.
type contributions[X any] struct {
	base   kv.Iterator
	offset int // where the time is in the keys
	codec  codec.ValueCodec[X]
}

var _ kvutil.Iterator[time.Time, int] = (*contributions[int])(nil)

// Valid implements the kvutil.Iterator interface.
func (it *contributions[X]) Valid() bool { return it.base.Valid() }

// Next implements the kvutil.Iterator interface.
func (it *contributions[X]) Next() { it.base.Next() }

// Close implements the kvutil.Iterator interface.
func (it *contributions[X]) Close() error { return it.base.Close() }

// Key implements the kvutil.Iterator interface.
func (it *contributions[X]) Key() (time.Time, error) {
	key := it.base.Key()
	if len(key) < it.offset+timeSize {
		return time.Time{}, fmt.Errorf("agg: contribution key of %d bytes", len(key))
	}
	return timeCodec.Decode(key[it.offset : it.offset+timeSize])
}

// Value implements the kvutil.Iterator interface.
func (it *contributions[X]) Value() (x X, err error) {
	bz, err := it.base.Value()
	if err != nil {
		return x, err
	}
	return it.codec.Decode(bz)
}

// computeSpan computes the state of a span from the contributions of the records in it.
// It reports whether the span has any record.
func (r *recomputeKind[X, S]) computeSpan(tx *prefix.Tx, b Bucket, group []byte, spanStart time.Time) (state S, ok bool, err error) {
	keys := groupPrefix(contributionKeys, group)
	start, end := timeRange(keys, spanStart, b.Next(spanStart), b == AllTime)
	base, err := tx.Iterator(start, end)
	if err != nil {
		return state, false, err
	}
	it := &contributions[X]{base: base, offset: len(keys), codec: r.xCodec}
	defer it.Close()

	if !it.Valid() {
		return state, false, nil
	}

	// A function of the user that panics must not take the collector down with it.
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("agg %q: the function panicked: %v", r.name, p)
		}
	}()
	state, err = r.compute(iters.From[time.Time, X](it))
	if err != nil {
		return state, false, fmt.Errorf("agg %q: %w", r.name, err)
	}
	return state, true, nil
}

// series implements the kind interface.
func (r *recomputeKind[X, S]) series(
	tx *prefix.Tx,
	b Bucket,
	group []byte,
	lo, hi time.Time,
	all, collectedOnly bool,
) ([]Point[S], error) {
	if r.isStored(b) {
		return r.storedSeries(tx, b, group, lo, hi, all, collectedOnly)
	}

	// The spans of the bucket are put together from the spans of the first bucket that are in them.
	fine, err := r.storedSeries(tx, r.stored[0], group, lo, hi, all, collectedOnly)
	if err != nil {
		return nil, err
	}

	var points []Point[S]
	for _, p := range fine {
		start := b.Start(p.Start)
		if n := len(points); n > 0 && points[n-1].Start.Equal(start) {
			points[n-1].State = r.merge(points[n-1].State, p.State)
			continue
		}
		points = append(points, Point[S]{Start: start, State: p.State})
	}
	return points, nil
}

// storedSeries returns the states of the spans of a bucket whose states are stored.
func (r *recomputeKind[X, S]) storedSeries(
	tx *prefix.Tx,
	b Bucket,
	group []byte,
	lo, hi time.Time,
	all, collectedOnly bool,
) ([]Point[S], error) {
	// The spans that are marked have states that are out of date, or none yet.
	marked := map[string]time.Time{}
	if !collectedOnly {
		start, end := timeRange(bucketPrefix(markerKeys, b, group), lo, hi, all)
		it, err := tx.Iterator(start, end)
		if err != nil {
			return nil, err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			_, _, spanStart, err := splitBucketKey(it.Key())
			if err != nil {
				return nil, err
			}
			marked[string(appendTime(nil, spanStart))] = spanStart
		}
	}

	var points []Point[S]
	start, end := timeRange(bucketPrefix(stateKeys, b, group), lo, hi, all)
	it, err := tx.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		_, _, spanStart, err := splitBucketKey(it.Key())
		if err != nil {
			return nil, err
		}
		if _, ok := marked[string(appendTime(nil, spanStart))]; ok {
			continue
		}

		value, err := it.Value()
		if err != nil {
			return nil, err
		}
		state, err := r.sCodec.Decode(value)
		if err != nil {
			return nil, err
		}
		points = append(points, Point[S]{Start: spanStart, State: state})
	}

	for _, spanStart := range marked {
		state, ok, err := r.computeSpan(tx, b, group, spanStart)
		if err != nil {
			return nil, err
		}
		if ok {
			points = append(points, Point[S]{Start: spanStart, State: state})
		}
	}
	slices.SortFunc(points, func(a, b Point[S]) int { return a.Start.Compare(b.Start) })
	return points, nil
}

// collect implements the kind interface.
func (r *recomputeKind[X, S]) collect(db kv.DB, cursor []byte, batch int, report func(error)) ([]byte, error) {
	// The markers of the batch are listed first, and each of their spans is then computed in a
	// transaction of its own, so that a span that is written to meanwhile only has itself retried.
	var markers [][]byte
	err := db.View(func(root kv.Tx) error {
		markers = nil

		start := []byte{markerKeys}
		if cursor != nil {
			start = append(bytes.Clone(cursor), 0x00)
		}
		it, err := r.ns.Open(root).Iterator(start, []byte{markerKeys + 1})
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid() && len(markers) < batch; it.Next() {
			markers = append(markers, bytes.Clone(it.Key()))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, marker := range markers {
		var failure error
		err := db.Update(func(root kv.Tx) error {
			failure = nil
			tx := r.ns.Open(root)

			// Reading the marker makes the transaction conflict with a write to the span.
			if _, err := tx.Get(marker); errors.Is(err, kv.ErrNotFound) {
				return nil
			} else if err != nil {
				return err
			}

			id, group, spanStart, err := splitBucketKey(marker)
			if err != nil {
				failure = err
				return nil
			}
			b, ok := r.bucket(id)
			if !ok {
				// The mark of a bucket that the aggregation does not have any more.
				return tx.Delete(marker)
			}

			stateKey := appendTime(bucketPrefix(stateKeys, b, group), spanStart)
			state, ok, err := r.computeSpan(tx, b, group, spanStart)
			switch {
			case err != nil:
				// The span stays marked, and is read from its records until it can be computed.
				failure = err
				return nil
			case !ok:
				// A span that has no record left has no state.
				err = tx.Delete(stateKey)
			default:
				var bz []byte
				if bz, err = r.sCodec.Encode(state); err == nil {
					err = tx.Set(stateKey, bz)
				}
			}
			if err != nil {
				return err
			}
			return tx.Delete(marker)
		})
		if err != nil {
			return nil, err
		}
		if failure != nil {
			report(fmt.Errorf("agg %q: a span was left to be computed again: %w", r.name, failure))
		}
	}

	if len(markers) < batch {
		return nil, nil
	}
	return markers[len(markers)-1], nil
}
