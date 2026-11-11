package vector

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
)

// upkeeper is implemented by the indexes, whatever the types of their records are. It is what a
// Maintainer keeps the partitions of an index with.
type upkeeper interface {
	// upkeepName names the index.
	upkeepName() string
	// upkeepPartitioned reports whether the index has partitions to keep.
	upkeepPartitioned() bool
	// upkeepPass does a round of what there is to do for the index, and reports whether it did
	// anything. recount is the number of partitions that it counts, or all of them if it is
	// negative.
	upkeepPass(ctx context.Context, db kv.DB, recount int) (did bool, err error)
}

// upkept is implemented by the extensions of the indexes, which is how a Maintainer finds the
// indexes among the extensions of a store.
type upkept interface {
	upkeeper() upkeeper
}

func (x *Index[K, V]) upkeepName() string {
	return x.name
}

func (x *Index[K, V]) upkeepPartitioned() bool {
	return x.size > 0
}

// The operations of the upkeep below are each a transaction, or a few, and are safe to run
// again: the function of a transaction may run more than once if the engine finds a conflict,
// so it works out everything from what it reads and what it keeps (a cursor, the partitions
// that the index remembers) is only kept once the transaction is committed.
//
// They are safe to run while records are written, which is the point. A writer that changes or
// deletes a record whose vector is in the unplaced area writes the key of that vector, and
// reads the key that says where the record is placed. An operation that places the vector reads
// the first and writes the second. Whichever of the two commits second has read what the other
// wrote, so it conflicts and is run again, and then sees the other's result. The same goes for a
// record whose vector is in a partition that is split or dissolved, whose keys the operation
// reads and writes. A writer never touches the counts or the centroids, so no writer conflicts
// with them.
//
// The passes of the Maintainers of one program over an index take turns. A maintainer of another
// program is kept from leaving a vector in a partition that is gone by the count of the
// partition: placing a vector in a partition writes its count, and splitting or dissolving the
// partition reads it, so the two conflict.

// upkeepPass does a round of what there is to do for the index: it makes the partitions if
// there are enough vectors and none yet, places the vectors that are unplaced, splits the
// partitions that have too many vectors, and counts some partitions exactly, splitting the ones
// that turn out to be too big and dissolving the ones that turn out to have few.
func (x *Index[K, V]) upkeepPass(ctx context.Context, db kv.DB, recount int) (did bool, err error) {
	// One pass at a time, whichever Maintainer it is of.
	x.upkeep.Lock()
	defer x.upkeep.Unlock()

	made, err := x.bootstrap(ctx, db)
	if err != nil {
		return false, err
	}
	did = made

	parts, err := x.currentPartitions(db)
	if err != nil || parts == nil {
		return did, err
	}

	placed, err := x.placeAll(ctx, db)
	did = did || placed
	if err != nil {
		return did, err
	}

	split, err := x.splitCounted(ctx, db)
	did = did || split
	if err != nil {
		return did, err
	}

	changed, err := x.recountSome(ctx, db, recount)
	return did || changed, err
}

// currentPartitions returns the partitions that the index has now, or nil if there are none.
func (x *Index[K, V]) currentPartitions(db kv.DB) (parts *partitions, err error) {
	err = db.View(func(tx kv.Tx) (err error) {
		parts, err = x.loadPartitions(prefix.New(tx, x.prefix))
		return err
	})
	return parts, err
}

// bootstrap makes the first partitions of an index that has none, if it has more vectors than a
// partition. They are made as a build makes them, from a sample of the unplaced vectors. It
// reports whether it made any.
func (x *Index[K, V]) bootstrap(ctx context.Context, db kv.DB) (bool, error) {
	// Is there anything to do? Counting stops as soon as there are too many to leave unplaced.
	var needed bool
	err := db.View(func(tx kv.Tx) error {
		needed = false
		data := prefix.New(tx, x.prefix)
		if _, err := data.Get([]byte{stateKey}); err == nil {
			return nil
		} else if !errors.Is(err, kv.ErrNotFound) {
			return err
		}

		it, err := data.Iterator(vectorKey(unplaced, nil), vectorKey(unplaced+1, nil))
		if err != nil {
			return err
		}
		defer it.Close()
		n := 0
		for ; it.Valid() && n <= x.size; it.Next() {
			n++
		}
		needed = n > x.size
		return nil
	})
	if err != nil || !needed {
		return false, err
	}

	// The unplaced vectors are gone through in batches, and a sample is picked from them.
	sample := x.newSampler()
	batch := x.batchSize()
	var cursor []byte
	for more := true; more; {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		var (
			items []item
			next  []byte
		)
		err := db.View(func(tx kv.Tx) error {
			var err error
			items, next, err = x.readUnplaced(prefix.New(tx, x.prefix), cursor, batch)
			return err
		})
		if err != nil {
			return false, fmt.Errorf("failed to sample the index %q: %w", x.name, err)
		}
		for _, it := range items {
			sample.add(it.stored)
		}
		cursor, more = next, len(items) == batch
	}
	// The records may have gone while the sample was picked.
	if sample.count <= x.size {
		return false, nil
	}

	if _, err := x.writePartitions(ctx, db, sample.count, sample.sample); err != nil {
		return false, err
	}
	return true, nil
}

// readUnplaced reads up to n of the vectors of the unplaced area after a cursor, which is the
// key of the last one that was read, or nil to start at the beginning. It returns them, copied,
// with the cursor to go on from. A vector that is not of the dimensions of the index is
// ErrCorrupt.
func (x *Index[K, V]) readUnplaced(data *prefix.Tx, cursor []byte, n int) (items []item, next []byte, err error) {
	start := vectorKey(unplaced, cursor)
	if cursor != nil {
		// The least key that is greater than the cursor.
		start = append(start, 0)
	}
	it, err := data.Iterator(start, vectorKey(unplaced+1, nil))
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()

	next = cursor
	for ; it.Valid() && len(items) < n; it.Next() {
		rawKey, _, err := splitVectorKey(it.Key())
		if err != nil {
			return nil, nil, err
		}
		value, err := it.Value()
		if err != nil {
			return nil, nil, err
		}
		if len(value) != 4*x.dims {
			return nil, nil, fmt.Errorf("%w: the entry of the key %x has %d bytes and %d are needed",
				ErrCorrupt, rawKey, len(value), 4*x.dims)
		}
		items = append(items, item{rawKey: bytes.Clone(rawKey), stored: bytes.Clone(value)})
		next = items[len(items)-1].rawKey
	}
	return items, next, nil
}

// placeAll goes through the unplaced vectors once, from the first to the last, placing them a
// batch at a time and splitting the partitions that a batch fills too much. What is written
// behind the cursor while it goes is for the next pass. It reports whether it placed any.
func (x *Index[K, V]) placeAll(ctx context.Context, db kv.DB) (placed bool, err error) {
	var cursor []byte
	for {
		if err := ctx.Err(); err != nil {
			return placed, err
		}

		next, done, n, over, err := x.placeBatch(db, cursor)
		if err != nil {
			return placed, fmt.Errorf("failed to place the vectors of the index %q: %w", x.name, err)
		}
		placed = placed || n > 0

		for _, pid := range over {
			if err := ctx.Err(); err != nil {
				return placed, err
			}
			if _, err := x.split(db, pid); err != nil {
				return placed, err
			}
		}
		if done {
			return placed, nil
		}
		cursor = next
	}
}

// placeBatch places a batch of the unplaced vectors after a cursor in the partitions with the
// nearest centroids. It returns the cursor to go on from, whether it reached the end of the
// unplaced area, the number of vectors it placed, and the partitions that have more vectors than
// their size now, as far as they are counted.
func (x *Index[K, V]) placeBatch(db kv.DB, cursor []byte) (next []byte, done bool, placed int, over []uint32, err error) {
	err = db.Update(func(tx kv.Tx) error {
		next, done, placed, over = cursor, true, 0, nil
		data := prefix.New(tx, x.prefix)

		parts, err := x.loadPartitions(data)
		if err != nil || parts == nil {
			return err
		}
		items, last, err := x.readUnplaced(data, cursor, x.batchSize())
		if err != nil {
			return err
		}
		next, done = last, len(items) < x.batchSize()
		if len(items) == 0 {
			return nil
		}

		pids, codes := x.assignAll(parts, items)
		added := map[uint32]uint32{}
		for i, it := range items {
			if err := data.Set(vectorKey(pids[i], it.rawKey), it.stored); err != nil {
				return err
			}
			if codes != nil {
				if err := data.Set(codeKey(pids[i], it.rawKey), codes[i]); err != nil {
					return err
				}
			}
			if err := data.Set(placeKey(it.rawKey), binary.BigEndian.AppendUint32(nil, pids[i])); err != nil {
				return err
			}
			if err := data.Delete(vectorKey(unplaced, it.rawKey)); err != nil {
				return err
			}
			added[pids[i]]++
		}
		placed = len(items)

		// The counts are upper bounds, and a partition that has none stays without.
		for _, pid := range slices.Sorted(maps.Keys(added)) {
			count, known, err := readCount(data, pid)
			if err != nil {
				return err
			}
			if !known {
				continue
			}
			count += added[pid]
			if err := data.Set(countKey(pid), binary.BigEndian.AppendUint32(nil, count)); err != nil {
				return err
			}
			if int(count) > x.size {
				over = append(over, pid)
			}
		}
		return nil
	})
	return next, done, placed, over, err
}

// readCount returns the number of vectors that are counted for a partition, if it is counted.
func readCount(data *prefix.Tx, pid uint32) (count uint32, known bool, err error) {
	b, err := data.Get(countKey(pid))
	if errors.Is(err, kv.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(b) != 4 {
		return 0, false, fmt.Errorf("%w: the count of the partition %d has %d bytes", ErrCorrupt, pid, len(b))
	}
	return binary.BigEndian.Uint32(b), true, nil
}

// readPartition reads the vectors of a partition, copied. The transaction has no iterator open
// when it returns.
func (x *Index[K, V]) readPartition(data *prefix.Tx, pid uint32) ([]item, error) {
	it, err := data.Iterator(vectorKey(pid, nil), vectorKey(pid+1, nil))
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var items []item
	for ; it.Valid(); it.Next() {
		rawKey, _, err := splitVectorKey(it.Key())
		if err != nil {
			return nil, err
		}
		value, err := it.Value()
		if err != nil {
			return nil, err
		}
		if len(value) != 4*x.dims {
			return nil, fmt.Errorf("%w: the entry of the key %x has %d bytes and %d are needed",
				ErrCorrupt, rawKey, len(value), 4*x.dims)
		}
		items = append(items, item{rawKey: bytes.Clone(rawKey), stored: bytes.Clone(value)})
	}
	return items, nil
}

// countPartition counts the vectors of a partition exactly, without reading them.
func countPartition(data *prefix.Tx, pid uint32) (int, error) {
	it, err := data.Iterator(vectorKey(pid, nil), vectorKey(pid+1, nil))
	if err != nil {
		return 0, err
	}
	defer it.Close()

	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	return n, nil
}

// split splits a partition that has more vectors than its size into two, around the centroids
// of two halves of its vectors. It reports whether it did. The vectors move to two partitions
// that are new, so that no vector is in a partition whose centroid is not its own.
func (x *Index[K, V]) split(db kv.DB, pid uint32) (did bool, err error) {
	var published *partitions
	err = db.Update(func(tx kv.Tx) error {
		did, published = false, nil
		data := prefix.New(tx, x.prefix)

		parts, err := x.loadPartitions(data)
		if err != nil {
			return err
		}
		if _, ok := parts.index(pid); !ok {
			return nil
		}
		// The count is read so that a vector that is placed in the partition meanwhile, which
		// writes it, is a conflict; see above.
		if _, _, err := readCount(data, pid); err != nil {
			return err
		}
		items, err := x.readPartition(data, pid)
		if err != nil {
			return err
		}
		if len(items) <= x.size {
			return data.Set(countKey(pid), binary.BigEndian.AppendUint32(nil, uint32(len(items))))
		}

		vectors := make([][]byte, len(items))
		for i, it := range items {
			vectors[i] = it.stored
		}
		side, c0, c1 := bisectBalanced(x.metric, x.dims, vectors)

		ids := [2]uint32{parts.next, parts.next + 1}

		// The codes are of the vectors against the centroids of the partitions they move to.
		var codes [][]byte
		if x.rot != nil {
			centroids := [2][]byte{encode(c0), encode(c1)}
			codes = make([][]byte, len(items))
			parallel(len(items), len(items)*x.codeCost(), func(from, to int) {
				c := x.rot.newCoder()
				for i := from; i < to; i++ {
					codes[i] = c.encode(items[i].stored, centroids[side[i]])
				}
			})
		}

		var counts [2]uint32
		for i, it := range items {
			to := ids[side[i]]
			counts[side[i]]++
			if err := data.Set(vectorKey(to, it.rawKey), it.stored); err != nil {
				return err
			}
			if err := data.Delete(vectorKey(pid, it.rawKey)); err != nil {
				return err
			}
			if codes != nil {
				if err := data.Set(codeKey(to, it.rawKey), codes[i]); err != nil {
					return err
				}
				if err := data.Delete(codeKey(pid, it.rawKey)); err != nil {
					return err
				}
			}
			if err := data.Set(placeKey(it.rawKey), binary.BigEndian.AppendUint32(nil, to)); err != nil {
				return err
			}
		}
		if err := data.Delete(centroidKey(pid)); err != nil {
			return err
		}
		if err := data.Delete(countKey(pid)); err != nil {
			return err
		}
		for s, c := range [2][]float32{c0, c1} {
			if err := data.Set(centroidKey(ids[s]), encode(c)); err != nil {
				return err
			}
			if err := data.Set(countKey(ids[s]), binary.BigEndian.AppendUint32(nil, counts[s])); err != nil {
				return err
			}
		}

		var nonce [8]byte
		if _, err := crand.Read(nonce[:]); err != nil {
			return err
		}
		if err := data.Set([]byte{stateKey}, state(nonce, parts.next+2)); err != nil {
			return err
		}

		// The partitions as they are now, for whoever looks next. They are only published once
		// the transaction is committed.
		published = parts.without(nonce, pid, x.dims)
		published.next = parts.next + 2
		published.ids = append(published.ids, ids[0], ids[1])
		published.centroids = append(published.centroids, encode(c0)...)
		published.centroids = append(published.centroids, encode(c1)...)
		if x.rot != nil {
			published.rotated = x.appendRotated(published.rotated, encode(c0))
			published.rotated = x.appendRotated(published.rotated, encode(c1))
		}
		did = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to split the partition %d of the index %q: %w", pid, x.name, err)
	}
	if published != nil {
		x.parts.Store(published)
	}
	return did, nil
}

// dissolve dissolves a partition that has few vectors left, unless it is the only one: its
// vectors are unplaced again, to be placed in the partitions with the nearest centroids. It
// reports whether it did.
func (x *Index[K, V]) dissolve(db kv.DB, pid uint32) (did bool, err error) {
	var published *partitions
	err = db.Update(func(tx kv.Tx) error {
		did, published = false, nil
		data := prefix.New(tx, x.prefix)

		parts, err := x.loadPartitions(data)
		if err != nil {
			return err
		}
		if _, ok := parts.index(pid); !ok || parts.count() < 2 {
			return nil
		}
		// Read for the same reason as in a split.
		if _, _, err := readCount(data, pid); err != nil {
			return err
		}
		items, err := x.readPartition(data, pid)
		if err != nil {
			return err
		}

		for _, it := range items {
			if err := data.Set(vectorKey(unplaced, it.rawKey), it.stored); err != nil {
				return err
			}
			if err := data.Delete(vectorKey(pid, it.rawKey)); err != nil {
				return err
			}
			if x.rot != nil {
				if err := data.Delete(codeKey(pid, it.rawKey)); err != nil {
					return err
				}
			}
			if err := data.Delete(placeKey(it.rawKey)); err != nil {
				return err
			}
		}
		if err := data.Delete(centroidKey(pid)); err != nil {
			return err
		}
		if err := data.Delete(countKey(pid)); err != nil {
			return err
		}

		var nonce [8]byte
		if _, err := crand.Read(nonce[:]); err != nil {
			return err
		}
		if err := data.Set([]byte{stateKey}, state(nonce, parts.next)); err != nil {
			return err
		}
		published = parts.without(nonce, pid, x.dims)
		did = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("failed to dissolve the partition %d of the index %q: %w", pid, x.name, err)
	}
	if published != nil {
		x.parts.Store(published)
	}
	return did, nil
}

// splitCounted splits the partitions whose counts say that they have too many vectors, and
// reports whether it split any.
func (x *Index[K, V]) splitCounted(ctx context.Context, db kv.DB) (did bool, err error) {
	counts := map[uint32]uint32{}
	err = db.View(func(tx kv.Tx) error {
		clear(counts)
		it, err := prefix.New(tx, x.prefix).Iterator([]byte{countsKey}, []byte{countsKey + 1})
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			if err != nil {
				return err
			}
			if len(it.Key()) != 5 || len(value) != 4 {
				return fmt.Errorf("%w: the count %x", ErrCorrupt, it.Key())
			}
			counts[binary.BigEndian.Uint32(it.Key()[1:])] = binary.BigEndian.Uint32(value)
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	for _, pid := range slices.Sorted(maps.Keys(counts)) {
		if int(counts[pid]) <= x.size {
			continue
		}
		if err := ctx.Err(); err != nil {
			return did, err
		}
		split, err := x.split(db, pid)
		did = did || split
		if err != nil {
			return did, err
		}
	}
	return did, nil
}

// recountSome counts exactly the vectors of some partitions, going on from the one that was
// counted last, or of all of them if n is negative. A partition that has more vectors than its
// size is split, and one that has fewer than an eighth of it is dissolved unless it is the only
// one. It reports whether it split or dissolved any.
func (x *Index[K, V]) recountSome(ctx context.Context, db kv.DB, n int) (did bool, err error) {
	parts, err := x.currentPartitions(db)
	if err != nil || parts == nil {
		return false, err
	}

	ids := slices.Clone(parts.ids)
	if n >= 0 && n < len(ids) {
		// Round-robin: the ones after the last that was counted come first.
		at, _ := slices.BinarySearch(ids, x.recounted.Load()+1)
		ids = slices.Concat(ids[at:], ids[:at])[:n]
	}

	for _, pid := range ids {
		if err := ctx.Err(); err != nil {
			return did, err
		}
		changed, dissolved, err := x.recount(db, pid)
		did = did || changed
		if err != nil {
			return did, err
		}
		x.recounted.Store(pid)

		// The vectors of a partition that was dissolved are placed at once, so that the partitions
		// that are counted next have them. Otherwise the partitions that are all small, because a
		// lot of records were deleted, would all be dissolved before any of them took the others'.
		if dissolved {
			if _, err := x.placeAll(ctx, db); err != nil {
				return did, err
			}
		}
	}
	return did, nil
}

// recount counts the vectors of a partition exactly and writes the count if it is not right. It
// then splits the partition, or dissolves it, if it has to be. It reports whether it did.
func (x *Index[K, V]) recount(db kv.DB, pid uint32) (did, dissolved bool, err error) {
	var (
		count   int
		found   bool
		differs bool
	)
	check := func(tx kv.Tx, write bool) error {
		found, differs = false, false
		data := prefix.New(tx, x.prefix)

		parts, err := x.loadPartitions(data)
		if err != nil {
			return err
		}
		if _, ok := parts.index(pid); !ok {
			return nil
		}
		if count, err = countPartition(data, pid); err != nil {
			return err
		}
		found = true

		known, ok, err := readCount(data, pid)
		if err != nil {
			return err
		}
		if ok && int(known) == count {
			return nil
		}
		differs = true
		if !write {
			return nil
		}
		return data.Set(countKey(pid), binary.BigEndian.AppendUint32(nil, uint32(count)))
	}

	// Most of the time the count is right, and nothing is written.
	err = db.View(func(tx kv.Tx) error { return check(tx, false) })
	if err == nil && differs {
		err = db.Update(func(tx kv.Tx) error { return check(tx, true) })
	}
	if err != nil {
		return false, false, fmt.Errorf("failed to count the partition %d of the index %q: %w", pid, x.name, err)
	}

	switch {
	case !found:
		return false, false, nil
	case count > x.size:
		did, err = x.split(db, pid)
		return did, false, err
	case count < x.size/8:
		did, err = x.dissolve(db, pid)
		return did, did, err
	}
	return false, false, nil
}
