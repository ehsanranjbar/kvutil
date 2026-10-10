// Package memkv is an in-memory kv engine for tests.
// Readers get a consistent snapshot and writers are serialized, so there are no conflicts.
// It has none of the optional capabilities.
package memkv

import (
	"bytes"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/kv"
)

type entry struct {
	key   []byte
	value []byte
}

// snapshot is an immutable list of entries sorted by key.
type snapshot struct {
	entries []entry
}

// lowerBound returns the index of the first entry whose key is not less than the given key.
func lowerBound(entries []entry, key []byte) int {
	return sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].key, key) >= 0
	})
}

func (s *snapshot) get(key []byte) ([]byte, bool) {
	i := lowerBound(s.entries, key)
	if i < len(s.entries) && bytes.Equal(s.entries[i].key, key) {
		return s.entries[i].value, true
	}
	return nil, false
}

// slice returns the entries in [start, end).
func (s *snapshot) slice(start, end []byte) []entry {
	lo, hi := 0, len(s.entries)
	if len(start) > 0 {
		lo = lowerBound(s.entries, start)
	}
	if len(end) > 0 {
		hi = lowerBound(s.entries, end)
	}
	if lo > hi {
		return nil
	}
	return s.entries[lo:hi]
}

// DB is an in-memory store.
type DB struct {
	mu      sync.Mutex // serializes writers
	current atomic.Pointer[snapshot]
}

// New creates a new empty DB.
func New() *DB {
	db := &DB{}
	db.current.Store(&snapshot{})
	return db
}

// View implements the kv.DB interface.
func (db *DB) View(fn func(kv.Tx) error) error {
	return fn(&tx{base: db.current.Load()})
}

// Update implements the kv.DB interface. Calls must not be nested.
func (db *DB) Update(fn func(kv.Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	t := &tx{
		base:     db.current.Load(),
		writable: true,
		pending:  make(map[string]write),
	}
	if err := fn(t); err != nil {
		return err
	}

	db.current.Store(&snapshot{entries: slices.Clone(t.merged(nil, nil))})
	return nil
}

type write struct {
	value   []byte
	deleted bool
}

type tx struct {
	base     *snapshot
	writable bool
	pending  map[string]write
}

// Get implements the kv.Tx interface.
func (t *tx) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, kv.ErrEmptyKey
	}

	if w, ok := t.pending[string(key)]; ok {
		if w.deleted {
			return nil, kv.ErrNotFound
		}
		return bytes.Clone(w.value), nil
	}

	value, ok := t.base.get(key)
	if !ok {
		return nil, kv.ErrNotFound
	}
	return bytes.Clone(value), nil
}

// Has implements the kv.Tx interface.
func (t *tx) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, kv.ErrEmptyKey
	}

	if w, ok := t.pending[string(key)]; ok {
		return !w.deleted, nil
	}

	_, ok := t.base.get(key)
	return ok, nil
}

// Set implements the kv.Tx interface.
func (t *tx) Set(key, value []byte) error {
	if !t.writable {
		return kv.ErrReadOnly
	}
	if len(key) == 0 {
		return kv.ErrEmptyKey
	}

	t.pending[string(key)] = write{value: bytes.Clone(value)}
	return nil
}

// Delete implements the kv.Tx interface.
func (t *tx) Delete(key []byte) error {
	if !t.writable {
		return kv.ErrReadOnly
	}
	if len(key) == 0 {
		return kv.ErrEmptyKey
	}

	t.pending[string(key)] = write{deleted: true}
	return nil
}

// Writable implements the kv.Tx interface.
func (t *tx) Writable() bool {
	return t.writable
}

// Iterator implements the kv.Tx interface.
func (t *tx) Iterator(start, end []byte) (kv.Iterator, error) {
	return &iterator{entries: t.merged(start, end)}, nil
}

// ReverseIterator implements the kv.Tx interface.
func (t *tx) ReverseIterator(start, end []byte) (kv.Iterator, error) {
	entries := t.merged(start, end)
	return &iterator{entries: entries, i: len(entries) - 1, reverse: true}, nil
}

// merged returns the entries in [start, end) as the transaction sees them right now.
// The result is shared with the snapshot when the transaction has no writes, so it must not be modified.
func (t *tx) merged(start, end []byte) []entry {
	base := t.base.slice(start, end)
	if len(t.pending) == 0 {
		return base
	}

	keys := make([]string, 0, len(t.pending))
	for k := range t.pending {
		if len(start) > 0 && k < string(start) {
			continue
		}
		if len(end) > 0 && k >= string(end) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	merged := make([]entry, 0, len(base)+len(keys))
	i := 0
	for _, k := range keys {
		for i < len(base) && string(base[i].key) < k {
			merged = append(merged, base[i])
			i++
		}
		if i < len(base) && string(base[i].key) == k {
			i++
		}
		if w := t.pending[k]; !w.deleted {
			merged = append(merged, entry{key: []byte(k), value: w.value})
		}
	}
	return append(merged, base[i:]...)
}

type iterator struct {
	entries []entry
	i       int
	reverse bool
}

// Valid implements the kv.Iterator interface.
func (it *iterator) Valid() bool {
	return it.i >= 0 && it.i < len(it.entries)
}

// Next implements the kv.Iterator interface.
func (it *iterator) Next() {
	if !it.Valid() {
		return
	}

	if it.reverse {
		it.i--
	} else {
		it.i++
	}
}

// Seek implements the kv.Iterator interface.
func (it *iterator) Seek(key []byte) {
	if !it.reverse {
		it.i = lowerBound(it.entries, key)
		return
	}

	// The last entry whose key is not greater than the given key.
	it.i = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].key, key) > 0
	}) - 1
}

// Key implements the kv.Iterator interface.
func (it *iterator) Key() []byte {
	return it.entries[it.i].key
}

// Value implements the kv.Iterator interface.
func (it *iterator) Value() ([]byte, error) {
	return it.entries[it.i].value, nil
}

// Close implements the kv.Iterator interface.
func (it *iterator) Close() error {
	return nil
}
