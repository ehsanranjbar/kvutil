// Package relation provides relations between the records of stores: a many-to-many Relation
// between the keys of two stores, and a one-to-many Association between a parent store and a
// child store. Both keep their references under a prefix of their own, record their version and
// their codecs there as a unit that is migrated, and are kept consistent with the stores as the
// records of the stores are deleted.
//
// The prefix of a relation or an association must not be the prefix of a store, nor begin or
// be the beginning of one, as migrate.Run does not take units whose prefixes overlap. The key
// that is the prefix followed by 'm' is where the unit is recorded, and is not to be used.
package relation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/ref"
)

// ErrMissingRecord is returned when a relation is set to a record that does not exist.
var ErrMissingRecord = errors.New("relation: record does not exist")

// The bytes after the prefix of a relation that the references of its two directions are kept
// under.
const (
	leftKey  = 'l'
	rightKey = 'r'
)

// under returns the prefix followed by a byte, in a slice of its own.
func under(prefix []byte, b byte) []byte {
	p := make([]byte, 0, len(prefix)+1)
	p = append(p, prefix...)
	return append(p, b)
}

// Relation is a many-to-many relation between the records of two stores, with data of the
// type D for each pair. Use struct{} for D if a pair has no data.
//
// Every pair is kept in both directions, so that the records that a record is related to are
// found from either side of the relation. Deleting a record of either store deletes its pairs.
//
// It is a unit of its own, which records its version and its codecs under its prefix: the
// pairs are under the prefix followed by 'l' and by 'r'.
type Relation[L, R, D any] struct {
	unit       *migrate.Unit
	leftCodec  codec.KeyCodec[L]
	rightCodec codec.KeyCodec[R]
	dataCodec  codec.ValueCodec[D]
	left       ext.Referable[L]
	right      ext.Referable[R]
	// lr has the left key, in the form that marks where it ends, refer to the right key.
	lr *ref.Store
	// rl is the mirror image of lr.
	rl *ref.Store
}

// New defines a new Relation between the records of two stores under a prefix, with the
// codec that the type of its data has by default. D comes first so that it is the only type
// argument that has to be written:
//
//	relation.New[Membership]("memberships", users, groups)
//
// It panics if the prefix is empty or either store is nil.
func New[D, L, R any, P ~string | ~[]byte](prefix P, left ext.Referable[L], right ext.Referable[R]) *Relation[L, R, D] {
	return NewWithCodec[D](prefix, left, right, nil)
}

// NewWithCodec defines a new Relation between the records of two stores under a prefix,
// with the given codec for its data. A codec that is nil is the one that New would use.
// It panics if the prefix is empty or either store is nil.
func NewWithCodec[D, L, R any, P ~string | ~[]byte](
	prefix P,
	left ext.Referable[L],
	right ext.Referable[R],
	dataCodec codec.ValueCodec[D],
) *Relation[L, R, D] {
	// These are mistakes in the definition, which is made in code when the program starts.
	p := bytes.Clone([]byte(prefix))
	if len(p) == 0 {
		panic("relation: the prefix is required")
	}
	label := migrate.Label(p)
	if isNil(left) {
		panic(fmt.Sprintf("relation %q: the left store is required", label))
	}
	if isNil(right) {
		panic(fmt.Sprintf("relation %q: the right store is required", label))
	}
	if dataCodec == nil {
		dataCodec = codec.ValueFor[D](nil)
	}

	r := &Relation[L, R, D]{
		leftCodec:  left.KeyCodec(),
		rightCodec: right.KeyCodec(),
		dataCodec:  dataCodec,
		left:       left,
		right:      right,
	}
	r.unit = migrate.NewUnit(p, "relation", func() map[string]string {
		return map[string]string{
			"left":  r.leftCodec.Name(),
			"right": r.rightCodec.Name(),
			"data":  r.dataCodec.Name(),
		}
	})
	r.lr = ref.New(under(p, leftKey))
	r.rl = ref.New(under(p, rightKey))

	// The hooks have different names for a relation of a store with itself. The label of the
	// prefix is the name of the relation, which is unique as prefixes do not overlap.
	left.AddDeleteHook(label+"/left", func(tx kv.Tx, key L, _ []byte) error {
		return removeAll(tx, r.lr, r.rl, r.leftCodec, r.rightCodec, key)
	})
	right.AddDeleteHook(label+"/right", func(tx kv.Tx, key R, _ []byte) error {
		return removeAll(tx, r.rl, r.lr, r.rightCodec, r.leftCodec, key)
	})
	return r
}

// Name returns the name of the relation, which is its prefix as text unless the name of its
// unit was set.
func (r *Relation[L, R, D]) Name() string {
	return r.unit.Name()
}

// Prefix returns the prefix of the relation. The slice must not be modified.
func (r *Relation[L, R, D]) Prefix() []byte {
	return r.unit.Prefix()
}

// Unit returns what the relation records of itself, which is migrated as a whole.
func (r *Relation[L, R, D]) Unit() *migrate.Unit {
	return r.unit
}

// WithVersion sets the version of the data of the relation, which is 1 by default. It is
// raised when the data must be converted, like when the codec of its data is changed, and a
// migration is added to the unit from every version before it.
func (r *Relation[L, R, D]) WithVersion(v uint64) *Relation[L, R, D] {
	r.unit.SetVersion(v)
	return r
}

// Migrate brings the database to what the relation defines; see migrate.Run.
func (r *Relation[L, R, D]) Migrate(ctx context.Context, db kv.DB) (*migrate.Report, error) {
	return migrate.Run(ctx, db, r)
}

// Check compares the database with what the relation defines, without changing it; see
// migrate.Check.
func (r *Relation[L, R, D]) Check(db kv.DB) error {
	return migrate.Check(db, r)
}

// Instantiate creates the instance of the relation for a transaction.
func (r *Relation[L, R, D]) Instantiate(tx kv.Tx) *Instance[L, R, D] {
	return &Instance[L, R, D]{
		rel: r,
		tx:  tx,
		lr:  r.lr.Instantiate(tx),
		rl:  r.rl.Instantiate(tx),
	}
}

// Instance is a Relation in a transaction.
type Instance[L, R, D any] struct {
	rel *Relation[L, R, D]
	tx  kv.Tx
	lr  *ref.Instance
	rl  *ref.Instance
}

// Set relates the left record to the right record, with the data. A pair that is already related
// has its data replaced. A nil data is the zero D. It returns ErrMissingRecord if either
// record does not exist.
func (i *Instance[L, R, D]) Set(left L, right R, data *D) error {
	r := i.rel
	ok, err := r.left.Contains(i.tx, left)
	if err != nil {
		return fmt.Errorf("failed to look up the left record: %w", err)
	}
	if !ok {
		return fmt.Errorf("%w: left record %v in relation %q", ErrMissingRecord, left, r.unit.Name())
	}
	ok, err = r.right.Contains(i.tx, right)
	if err != nil {
		return fmt.Errorf("failed to look up the right record: %w", err)
	}
	if !ok {
		return fmt.Errorf("%w: right record %v in relation %q", ErrMissingRecord, right, r.unit.Name())
	}

	lk, rk, err := i.forms(left, right)
	if err != nil {
		return err
	}
	if data == nil {
		data = new(D)
	}
	value, err := r.dataCodec.Encode(*data)
	if err != nil {
		return fmt.Errorf("failed to encode the data: %w", err)
	}

	if err := i.lr.Set(lk.app, rk.enc, value); err != nil {
		return err
	}
	return i.rl.Set(rk.app, lk.enc, value)
}

// Get returns the data of the pair, or kv.ErrNotFound if the records are not related.
func (i *Instance[L, R, D]) Get(left L, right R) (*D, error) {
	lk, rk, err := i.forms(left, right)
	if err != nil {
		return nil, err
	}

	value, err := i.lr.Get(lk.app, rk.enc)
	if err != nil {
		return nil, err
	}
	data, err := i.rel.dataCodec.Decode(value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the data: %w", err)
	}
	return &data, nil
}

// Has reports whether the records are related.
func (i *Instance[L, R, D]) Has(left L, right R) (bool, error) {
	lk, rk, err := i.forms(left, right)
	if err != nil {
		return false, err
	}
	return i.lr.Has(lk.app, rk.enc)
}

// Delete deletes the pair. Deleting a pair that is not related does nothing.
func (i *Instance[L, R, D]) Delete(left L, right R) error {
	lk, rk, err := i.forms(left, right)
	if err != nil {
		return err
	}

	if err := i.lr.Delete(lk.app, rk.enc); err != nil {
		return err
	}
	return i.rl.Delete(rk.app, lk.enc)
}

// Rights iterates over the records that the left record is related to, in the order of their keys.
// The iterator must be closed.
func (i *Instance[L, R, D]) Rights(left L) (iterutil.Iterator[R, *D], error) {
	lk, err := form(i.rel.leftCodec, left)
	if err != nil {
		return nil, err
	}

	rightCodec := i.rel.rightCodec
	return iterate(i.lr, lk.app, i.rel.dataCodec, func(it *ref.Iterator) (R, error) {
		pk, err := it.PrimaryKey()
		if err != nil {
			var zero R
			return zero, err
		}
		return rightCodec.Decode(pk)
	})
}

// Lefts iterates over the records that are related to the right record, in the order of their keys.
// The iterator must be closed.
func (i *Instance[L, R, D]) Lefts(right R) (iterutil.Iterator[L, *D], error) {
	rk, err := form(i.rel.rightCodec, right)
	if err != nil {
		return nil, err
	}

	leftCodec := i.rel.leftCodec
	return iterate(i.rl, rk.app, i.rel.dataCodec, func(it *ref.Iterator) (L, error) {
		pk, err := it.PrimaryKey()
		if err != nil {
			var zero L
			return zero, err
		}
		return leftCodec.Decode(pk)
	})
}

// All iterates over every pair, ordered by the left key and then by the right key.
// The iterator must be closed.
func (i *Instance[L, R, D]) All() (iterutil.Iterator[codec.Pair[L, R], *D], error) {
	leftCodec, rightCodec := i.rel.leftCodec, i.rel.rightCodec
	return iterate(i.lr, nil, i.rel.dataCodec, func(it *ref.Iterator) (codec.Pair[L, R], error) {
		var pair codec.Pair[L, R]
		key, err := it.Key()
		if err != nil {
			return pair, err
		}
		pair.First, _, err = leftCodec.Read(key)
		if err != nil {
			return pair, err
		}

		pk, err := it.PrimaryKey()
		if err != nil {
			return pair, err
		}
		pair.Second, err = rightCodec.Decode(pk)
		return pair, err
	})
}

// forms returns the forms that the keys of a pair are stored in.
func (i *Instance[L, R, D]) forms(left L, right R) (lk, rk keyForms, err error) {
	lk, err = form(i.rel.leftCodec, left)
	if err != nil {
		return lk, rk, err
	}
	rk, err = form(i.rel.rightCodec, right)
	return lk, rk, err
}

// keyForms are the two forms that a key is stored in. A key that is followed by other bytes is stored
// in the form that marks where it ends, and a key that is the last component in the other form.
type keyForms struct {
	app []byte
	enc []byte
}

// form returns the forms of a key.
func form[K any](c codec.KeyCodec[K], key K) (keyForms, error) {
	app, err := c.Append(nil, key)
	if err != nil {
		return keyForms{}, fmt.Errorf("failed to encode key %v: %w", key, err)
	}
	enc, err := c.Encode(key)
	if err != nil {
		return keyForms{}, fmt.Errorf("failed to encode key %v: %w", key, err)
	}
	return keyForms{app: app, enc: enc}, nil
}

// removeAll deletes every pair of a key, in both directions. from has the references that begin with
// the key, and to has the ones that lead back to it.
func removeAll[A, B any](
	tx kv.Tx,
	from, to *ref.Store,
	fromCodec codec.KeyCodec[A],
	toCodec codec.KeyCodec[B],
	key A,
) error {
	fk, err := form(fromCodec, key)
	if err != nil {
		return err
	}
	fromRefs, toRefs := from.Instantiate(tx), to.Instantiate(tx)

	// The references are collected first, as deleting them while iterating over them is not allowed.
	var others [][]byte
	it, err := fromRefs.Iterator(fk.app, kv.PrefixEnd(fk.app))
	if err != nil {
		return err
	}
	for ; it.Valid(); it.Next() {
		pk, err := it.PrimaryKey()
		if err != nil {
			it.Close()
			return err
		}
		others = append(others, bytes.Clone(pk))
	}
	if err := it.Close(); err != nil {
		return err
	}

	for _, enc := range others {
		other, err := toCodec.Decode(enc)
		if err != nil {
			return fmt.Errorf("failed to decode a related key: %w", err)
		}
		back, err := form(toCodec, other)
		if err != nil {
			return err
		}
		if err := toRefs.Delete(back.app, fk.enc); err != nil {
			return err
		}
		if err := fromRefs.Delete(fk.app, enc); err != nil {
			return err
		}
	}
	return nil
}

// iterator iterates over the references of a relation, with the keys that the function makes of them.
type iterator[K, D any] struct {
	base  *ref.Iterator
	keyOf func(*ref.Iterator) (K, error)
	data  codec.ValueCodec[D]
}

// iterate iterates over the references of a key, or over all of them if the key is nil.
func iterate[K, D any](
	refs *ref.Instance,
	key []byte,
	data codec.ValueCodec[D],
	keyOf func(*ref.Iterator) (K, error),
) (iterutil.Iterator[K, *D], error) {
	var end []byte
	if key != nil {
		end = kv.PrefixEnd(key)
	}
	base, err := refs.Iterator(key, end)
	if err != nil {
		return nil, err
	}
	return &iterator[K, D]{base: base, keyOf: keyOf, data: data}, nil
}

// Valid implements the iterutil.Iterator interface.
func (it *iterator[K, D]) Valid() bool {
	return it.base.Valid()
}

// Next implements the iterutil.Iterator interface.
func (it *iterator[K, D]) Next() {
	it.base.Next()
}

// Key implements the iterutil.Iterator interface.
func (it *iterator[K, D]) Key() (K, error) {
	return it.keyOf(it.base)
}

// Value implements the iterutil.Iterator interface.
func (it *iterator[K, D]) Value() (*D, error) {
	value, err := it.base.Value()
	if err != nil {
		return nil, err
	}
	data, err := it.data.Decode(value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the data: %w", err)
	}
	return &data, nil
}

// Close implements the iterutil.Iterator interface.
func (it *iterator[K, D]) Close() error {
	return it.base.Close()
}

// isNil reports whether a store that is given to a definition is nil, including a nil pointer
// of a store type.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
		return rv.IsNil()
	}
	return false
}
