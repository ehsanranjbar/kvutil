package agg

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Fold says how a delta aggregation takes the contributions of the records into a state.
// The functions must give the same state in whatever order the contributions come,
// as they are not collected in the order that they were written in.
type Fold[X, S any] struct {
	// Add adds a contribution to a state. The state of a span with no record is the zero value.
	Add func(state S, x X) S
	// Remove takes a contribution that was added out of a state again, for a record that is
	// changed or deleted. Only an aggregation that is declared AppendOnly can be without it.
	Remove func(state S, x X) S
}

// Delta defines an aggregation on a store that is kept by folding what the records contribute
// into its states.
//
// Writing a record notes what it adds and what it removes in a temporary record of its own,
// and the collector folds the temporary records into the states. That costs the same however
// many records a span has, so it suits the aggregations with large spans, like a total for all
// of time. The fold must be able to remove what it has added, and must not depend on the order.
func Delta[K, V, G, X, S any](
	store extstore.Extensible[K, V],
	name string,
	extract Extractor[V, G, X],
	fold Fold[X, S],
	opts ...Option,
) *Aggregation[K, V, G, X, S] {
	return define(store, name, "delta", extract, opts, func(c *core[X, S], o options) kind[X, S] {
		if fold.Add == nil {
			panic(fmt.Sprintf("agg %q: a delta aggregation needs a function to add a contribution", name))
		}
		if fold.Remove == nil && !o.appendOnly {
			panic(fmt.Sprintf("agg %q: a delta aggregation needs a function to remove a contribution, "+
				"unless it is declared with AppendOnly", name))
		}
		if o.merge != nil {
			panic(fmt.Sprintf("agg %q: WithMerge is for recomputed aggregations", name))
		}
		return &deltaKind[X, S]{core: c, fold: fold, appendOnly: o.appendOnly}
	})
}

type deltaKind[X, S any] struct {
	*core[X, S]
	fold       Fold[X, S]
	appendOnly bool
}

// The first byte of a temporary record says whether its contribution is added or removed.
const (
	signRemove = 0
	signAdd    = 1
)

// idSize is the size of the id that makes the key of a temporary record its own.
const idSize = 12

// lastID is the counter that the ids are taken from. It starts from the clock, so that the
// ids of a process come after the ids of the processes before it.
var lastID atomic.Uint64

func init() {
	lastID.Store(uint64(time.Now().UnixNano()))
}

// appendID appends an id that no other temporary record has: a counter, so that the ids of a
// process are in the order that they were made in, and random bytes in case the clock went back.
func appendID(dst []byte) []byte {
	var id [idSize]byte
	binary.BigEndian.PutUint64(id[:8], lastID.Add(1))
	rand.Read(id[8:])
	return append(dst, id[:]...)
}

// change implements the kind interface.
func (d *deltaKind[X, S]) change(tx *prefix.Tx, _ []byte, old, new *contribution[X]) error {
	if old != nil && d.appendOnly {
		return fmt.Errorf("%w: %q", ErrAppendOnly, d.name)
	}

	for _, c := range []struct {
		c    *contribution[X]
		sign byte
	}{{old, signRemove}, {new, signAdd}} {
		if c.c == nil {
			continue
		}
		key := appendID(appendTime(groupPrefix(tempKeys, c.c.group), c.c.time))
		if err := tx.Set(key, append([]byte{c.sign}, c.c.xbz...)); err != nil {
			return err
		}
	}
	return nil
}

// tally is a state with the number of the contributions that are in it,
// which tells a span with no record from a span whose state is the zero value.
type tally[S any] struct {
	count int64
	state S
}

func (d *deltaKind[X, S]) decodeTally(bz []byte) (t tally[S], err error) {
	if len(bz) < 8 {
		return t, fmt.Errorf("agg %q: state of %d bytes", d.name, len(bz))
	}
	t.count = int64(binary.BigEndian.Uint64(bz[:8]))
	t.state, err = d.sCodec.Decode(bz[8:])
	return t, err
}

func (d *deltaKind[X, S]) encodeTally(t tally[S]) ([]byte, error) {
	sbz, err := d.sCodec.Encode(t.state)
	if err != nil {
		return nil, err
	}
	return append(binary.BigEndian.AppendUint64(nil, uint64(t.count)), sbz...), nil
}

// apply folds a temporary record into a tally.
func (d *deltaKind[X, S]) apply(t tally[S], sign byte, x X) (_ tally[S], err error) {
	// A function of the user that panics must not take the collector down with it.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("agg %q: the fold panicked: %v", d.name, r)
		}
	}()

	if sign == signAdd {
		t.state = d.fold.Add(t.state, x)
		t.count++
		return t, nil
	}
	if d.fold.Remove == nil {
		return t, fmt.Errorf("%w: %q", ErrAppendOnly, d.name)
	}
	t.state = d.fold.Remove(t.state, x)
	t.count--
	return t, nil
}

// temp is a temporary record.
type temp[X any] struct {
	group []byte // the encoded group
	time  time.Time
	sign  byte
	x     X
}

func (d *deltaKind[X, S]) decodeTemp(key, value []byte) (t temp[X], err error) {
	n, err := d.skipGroup(key[1:])
	if err != nil {
		return t, err
	}
	group, rest := key[1:1+n], key[1+n:]
	if len(rest) != timeSize+idSize || len(value) < 1 {
		return t, fmt.Errorf("agg %q: temporary record of %d and %d bytes", d.name, len(key), len(value))
	}

	t.group = group
	t.sign = value[0]
	if t.time, err = timeCodec.Decode(rest[:timeSize]); err != nil {
		return t, err
	}
	t.x, err = d.xCodec.Decode(value[1:])
	return t, err
}

// series implements the kind interface.
func (d *deltaKind[X, S]) series(
	tx *prefix.Tx,
	b Bucket,
	group []byte,
	lo, hi time.Time,
	all, collectedOnly bool,
) ([]Point[S], error) {
	tallies := map[string]tally[S]{} // by the encoded start of the span
	starts := map[string]time.Time{}
	at := func(start time.Time) string {
		k := string(appendTime(nil, start))
		starts[k] = start
		return k
	}

	start, end := timeRange(bucketPrefix(stateKeys, b, group), lo, hi, all)
	it, err := tx.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		_, _, spanStart, err := d.splitBucketKey(it.Key())
		if err != nil {
			return nil, err
		}
		value, err := it.Value()
		if err != nil {
			return nil, err
		}
		t, err := d.decodeTally(value)
		if err != nil {
			return nil, err
		}
		tallies[at(spanStart)] = t
	}

	if !collectedOnly {
		// What is not collected yet is folded in, which gives what the states will be.
		start, end := timeRange(groupPrefix(tempKeys, group), lo, hi, all)
		tit, err := tx.Iterator(start, end)
		if err != nil {
			return nil, err
		}
		defer tit.Close()
		for ; tit.Valid(); tit.Next() {
			value, err := tit.Value()
			if err != nil {
				return nil, err
			}
			t, err := d.decodeTemp(tit.Key(), value)
			if err != nil {
				return nil, err
			}

			k := at(b.Start(t.time))
			if tallies[k], err = d.apply(tallies[k], t.sign, t.x); err != nil {
				return nil, err
			}
		}
	}

	points := make([]Point[S], 0, len(tallies))
	for k, t := range tallies {
		if t.count > 0 {
			points = append(points, Point[S]{Start: starts[k], State: t.state})
		}
	}
	slices.SortFunc(points, func(a, b Point[S]) int { return a.Start.Compare(b.Start) })
	return points, nil
}

// collect implements the kind interface.
func (d *deltaKind[X, S]) collect(db kv.DB, cursor []byte, batch int, report func(error)) (next []byte, err error) {
	err = db.Update(func(root kv.Tx) error {
		next = nil
		tx := d.ns.Open(root)

		start := []byte{tempKeys}
		if cursor != nil {
			start = append(bytes.Clone(cursor), 0x00)
		}
		it, err := tx.Iterator(start, []byte{tempKeys + 1})
		if err != nil {
			return err
		}
		defer it.Close()

		// The states that the batch changes, by their keys, as they will be.
		tallies := map[string]tally[S]{}
		load := func(key []byte) (tally[S], error) {
			if t, ok := tallies[string(key)]; ok {
				return t, nil
			}
			bz, err := tx.Get(key)
			if errors.Is(err, kv.ErrNotFound) {
				return tally[S]{}, nil
			}
			if err != nil {
				return tally[S]{}, err
			}
			return d.decodeTally(bz)
		}

		n := 0
		for ; it.Valid() && n < batch; it.Next() {
			n++
			key := bytes.Clone(it.Key())
			next = key
			value, err := it.Value()
			if err != nil {
				return err
			}
			value = bytes.Clone(value)

			// A temporary record is folded into all of its states, or into none of them.
			changed := map[string]tally[S]{}
			t, err := d.decodeTemp(key, value)
			for i := 0; err == nil && i < len(d.buckets); i++ {
				b := d.buckets[i]
				stateKey := appendTime(bucketPrefix(stateKeys, b, t.group), b.Start(t.time))

				var current tally[S]
				if current, err = load(stateKey); err != nil {
					return err
				}
				changed[string(stateKey)], err = d.apply(current, t.sign, t.x)
			}
			if err != nil {
				// What cannot be folded is put aside, so that it does not come up again.
				report(fmt.Errorf("agg %q: a temporary record was put aside: %w", d.name, err))
				if err := tx.Set(append([]byte{failedKeys}, key[1:]...), value); err != nil {
					return err
				}
			} else {
				for k, t := range changed {
					tallies[k] = t
				}
			}

			if err := tx.Delete(key); err != nil {
				return err
			}
		}
		if n < batch {
			next = nil
		}

		for key, t := range tallies {
			if t.count <= 0 {
				// A span that has no record left has no state.
				if err := tx.Delete([]byte(key)); err != nil {
					return err
				}
				continue
			}
			bz, err := d.encodeTally(t)
			if err != nil {
				return err
			}
			if err := tx.Set([]byte(key), bz); err != nil {
				return err
			}
		}
		return nil
	})
	return next, err
}
