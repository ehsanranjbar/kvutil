package rectools

import (
	"context"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/legacy"
	extstore "github.com/ehsanranjbar/kvutil/legacy/store/ext"
	pstore "github.com/ehsanranjbar/kvutil/legacy/store/prefix"
	recstore "github.com/ehsanranjbar/kvutil/legacy/store/rec"
	refstore "github.com/ehsanranjbar/kvutil/legacy/store/ref"
	sstore "github.com/ehsanranjbar/kvutil/legacy/store/serialized"
)

// Relation is a struct that represents a relation between two stores.
type Relation[
	LI comparable,
	LT any,
	LR recstore.Record[LI, LT],
	RI comparable,
	RT any,
	RR recstore.Record[RI, RT],
	D any,
	PD sstore.BSP[D],
] struct {
	name string

	leftStore  *recstore.Store[LI, LT, LR]
	l2r        *refstore.Store
	rightStore *recstore.Store[RI, RT, RR]
	r2l        *refstore.Store
}

// NewRelation relates two stores with a relation.
func NewRelation[
	LI comparable,
	LT any,
	LR recstore.Record[LI, LT],
	RI comparable,
	RT any,
	RR recstore.Record[RI, RT],
	D any,
	PD sstore.BSP[D],
](
	name string,
	leftStore *recstore.Store[LI, LT, LR],
	rightStore *recstore.Store[RI, RT, RR],
) *Relation[LI, LT, LR, RI, RT, RR, D, PD] {
	r := &Relation[LI, LT, LR, RI, RT, RR, D, PD]{
		name:       name,
		leftStore:  leftStore,
		rightStore: rightStore,
	}

	r.leftStore.WithExtension(name, &relExt[LI, LT, LR, RI, RT, RR, D, PD]{
		name:              name,
		mainIdCodec:       leftStore.IdCodec(),
		m2c:               &r.l2r,
		counterpartyStore: rightStore,
		c2m:               &r.r2l,
	})
	r.rightStore.WithExtension(name, &relExt[RI, RT, RR, LI, LT, LR, D, PD]{
		name:              name,
		mainIdCodec:       rightStore.IdCodec(),
		m2c:               &r.r2l,
		counterpartyStore: leftStore,
		c2m:               &r.l2r,
	})

	return r
}

// Instantiate creates a new instance.
func (r *Relation[LI, LT, LR, RI, RT, RR, D, PD]) Instantiate(txn *badger.Txn) *RelationInstance[LI, LT, LR, RI, RT, RR, D, PD] {
	return &RelationInstance[LI, LT, LR, RI, RT, RR, D, PD]{
		name:          r.name,
		leftStore:     r.leftStore.Instantiate(txn),
		leftIdCodec:   r.leftStore.IdCodec(),
		leftRawStore:  pstore.New(nil, append(r.leftStore.Prefix(), dataStorePrefix...)).Instantiate(txn),
		l2r:           r.l2r.Instantiate(txn).(*refstore.Instance),
		rightStore:    r.rightStore.Instantiate(txn),
		rightIdCodec:  r.rightStore.IdCodec(),
		rightRawStore: pstore.New(nil, append(r.rightStore.Prefix(), dataStorePrefix...)).Instantiate(txn),
		r2l:           r.r2l.Instantiate(txn).(*refstore.Instance),
	}
}

// RelationInstance is an instance of a relation that can be used to interact with the stores.
type RelationInstance[
	LI comparable,
	LT any,
	LR recstore.Record[LI, LT],
	RI comparable,
	RT any,
	RR recstore.Record[RI, RT],
	D any,
	PD sstore.BSP[D],
] struct {
	name string

	leftStore     *recstore.Instance[LI, LT, LR]
	leftIdCodec   codec.Codec[LI]
	leftRawStore  legacy.BadgerStore
	l2r           *refstore.Instance
	rightStore    *recstore.Instance[RI, RT, RR]
	rightIdCodec  codec.Codec[RI]
	rightRawStore legacy.BadgerStore
	r2l           *refstore.Instance
}

// Delete removes a relation between a left record and a right record.
func (ri *RelationInstance[LI, LT, LR, RI, RT, RR, D, PD]) Delete(key CompoundKey[LI, RI]) error {
	lk, rk, err := ri.serializeKey(key)
	if err != nil {
		return err
	}

	err = ri.l2r.Delete(append(lk, rk...))
	if err != nil {
		return fmt.Errorf("failed to delete %T -> %T ref record: %w", *new(LR), *new(RR), err)
	}

	err = ri.r2l.Delete(append(rk, lk...))
	if err != nil {
		return fmt.Errorf("failed to delete %T -> %T ref record: %w", *new(RR), *new(LR), err)
	}

	return nil
}

// CompoundKey is a struct that represents a compound key with two parts.
type CompoundKey[LI, RI any] struct {
	Left  LI
	Right RI
}

// NewCompoundKey creates a new compound key.
func NewCompoundKey[LI, RI any](left LI, right RI) CompoundKey[LI, RI] {
	return CompoundKey[LI, RI]{Left: left, Right: right}
}

func (ri *RelationInstance[LI, LT, LR, RI, RT, RR, D, PD]) serializeKey(key CompoundKey[LI, RI]) ([]byte, []byte, error) {
	lk, err := ri.leftIdCodec.Encode(key.Left)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal left key: %w", err)
	}
	rk, err := ri.rightIdCodec.Encode(key.Right)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal right key: %w", err)
	}

	return lk, rk, nil
}

// Get returns the relation between a left record and a right record.
func (ri *RelationInstance[LI, LT, LR, RI, RT, RR, D, PD]) Get(key CompoundKey[LI, RI]) (*D, error) {
	lk, rk, err := ri.serializeKey(key)
	if err != nil {
		return nil, err
	}

	item, _, err := ri.l2r.GetWithItem(append(lk, rk...))
	if err != nil {
		return nil, fmt.Errorf("failed to get %T -> %T ref record: %w", *new(LR), *new(RR), err)
	}

	var d D
	err = item.Value(func(val []byte) error {
		return PD(&d).UnmarshalBinary(val)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return &d, nil
}

// NewIterator creates a new iterator.
func (ri *RelationInstance[LI, LT, LR, RI, RT, RR, D, PD]) NewIterator(opts badger.IteratorOptions) legacy.Iterator[CompoundKey[LI, RI], *D] {
	return &relationIterator[LI, RI, D, PD]{
		base:         ri.l2r.NewIterator(opts),
		leftIdCodec:  ri.leftIdCodec,
		rightIdCodec: ri.rightIdCodec,
		swapKeys:     false,
	}
}

type relationIterator[LI comparable, RI any, D any, PD sstore.BSP[D]] struct {
	base         legacy.Iterator[[]byte, []byte]
	leftIdCodec  codec.Codec[LI]
	rightIdCodec codec.Codec[RI]
	swapKeys     bool
}

// Close implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Close() {
	i.base.Close()
}

// Item implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Item() *badger.Item {
	return i.base.Item()
}

// Next implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Next() {
	i.base.Next()
}

// Rewind implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Rewind() {
	i.base.Rewind()
}

// Seek implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Seek(key []byte) {
	i.base.Seek(key)
}

// Valid implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Valid() bool {
	return i.base.Valid()
}

// Key returns the current key.
func (it *relationIterator[LI, RI, D, PD]) Key() (k CompoundKey[LI, RI], err error) {
	lk, err := it.base.Key()
	if err != nil {
		return k, err
	}
	rk, err := it.base.Value()
	if err != nil {
		return k, err
	}

	if it.swapKeys {
		lk, rk = rk, lk
	}

	left, err := it.leftIdCodec.Decode(lk)
	if err != nil {
		return k, err
	}
	right, err := it.rightIdCodec.Decode(rk)
	if err != nil {
		return k, err
	}

	return NewCompoundKey(left, right), nil
}

// Value implements the Iterator interface
func (i *relationIterator[LI, RI, D, PD]) Value() (*D, error) {
	item := i.base.Item()
	if item == nil {
		return nil, nil
	}

	var d D
	err := item.Value(func(val []byte) error {
		return PD(&d).UnmarshalBinary(val)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return &d, nil
}

// Set creates a relation between a left record and a right record.
func (ri *RelationInstance[LI, LT, LR, RI, RT, RR, D, PD]) Set(key CompoundKey[LI, RI], value *D) error {
	lk, rk, err := ri.serializeKey(key)
	if err != nil {
		return err
	}

	exists, err := doesKeyExist(ri.leftRawStore, lk)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("no record with id %v found in left store", key.Left)
	}
	exists, err = doesKeyExist(ri.rightRawStore, rk)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("no record with id %v found in right store", key.Right)
	}

	data, err := PD(value).MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	err = ri.l2r.Set(rk, refstore.NewRefEntry(lk).WithValue(data))
	if err != nil {
		return fmt.Errorf("failed to set %T -> %T ref record: %w", *new(LR), *new(RR), err)
	}
	err = ri.r2l.Set(lk, refstore.NewRefEntry(rk))
	if err != nil {
		return fmt.Errorf("failed to set %T -> %T ref record: %w", *new(RR), *new(LR), err)
	}

	return nil
}

func doesKeyExist(store legacy.BadgerStore, lk []byte) (bool, error) {
	_, err := store.Get(lk)
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get key: %w", err)
	}

	return true, nil
}

type relExt[
	MI comparable,
	MT any,
	MR recstore.Record[MI, MT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
	D any,
	PD sstore.BSP[D],
] struct {
	name string

	mainIdCodec       codec.Codec[MI]
	m2c               **refstore.Store
	counterpartyStore *recstore.Store[CI, CT, CR]
	c2m               **refstore.Store
}

func (e *relExt[MI, MT, MR, CI, CT, CR, D, PD]) Instantiate(txn *badger.Txn) extstore.ExtensionInstance[MI, MT] {
	return &relExtInstance[MI, MT, MR, CI, CT, CR, D, PD]{
		name:                 e.name,
		mainIdCodec:          e.mainIdCodec,
		m2c:                  (*e.m2c).Instantiate(txn).(*refstore.Instance),
		counterpartyIdCodec:  e.counterpartyStore.IdCodec(),
		counterpartyRawStore: pstore.New(nil, append(e.counterpartyStore.Prefix(), dataStorePrefix...)).Instantiate(txn),
		c2m:                  (*e.c2m).Instantiate(txn).(*refstore.Instance),
	}
}

func (e *relExt[MI, MT, MR, CI, CT, CR, D, PD]) RegisterStore(store legacy.Instantiator[legacy.BadgerStore]) {
	s := refstore.New(store)
	*e.m2c = s
}

type relExtInstance[
	MI comparable,
	MT any,
	MR recstore.Record[MI, MT],
	CI comparable,
	CT any,
	CR recstore.Record[CI, CT],
	D any,
	PD sstore.BSP[D],
] struct {
	name string

	mainIdCodec          codec.Codec[MI]
	m2c                  *refstore.Instance
	counterpartyIdCodec  codec.Codec[CI]
	counterpartyRawStore legacy.BadgerStore // To check for existence of counterparties without deserializing
	c2m                  *refstore.Instance
}

func (ei *relExtInstance[MI, MT, MR, CI, CT, CR, D, PD]) OnDelete(ctx context.Context, key MI, value *MT) error {
	kbz := extstore.GetKeyBytesFromContext(ctx)
	it := ei.m2c.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         kbz,
	})
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		cpk, _ := it.Value()

		err := ei.c2m.Delete(append(cpk, kbz...))
		if err != nil {
			return fmt.Errorf("failed to delete %T -> %T ref record: %w", *new(CT), *new(MT), err)
		}
	}

	err := ei.m2c.Delete(kbz)
	if err != nil {
		return fmt.Errorf("failed to delete %T -> %T ref records: %w", *new(MT), *new(CT), err)
	}

	return nil
}

func (ei *relExtInstance[MI, MT, MR, CI, CT, CR, D, PD]) OnSet(ctx context.Context, key MI, _, _ *MT, opts ...any) error {
	cpids := findAs[CI](opts)

	for _, cpid := range cpids {
		cpk, err := ei.counterpartyIdCodec.Encode(cpid)
		if err != nil {
			return fmt.Errorf("failed to marshal counterparty key: %w", err)
		}

		exists, err := doesKeyExist(ei.counterpartyRawStore, cpk)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("no record with id %v found in counterparty store", cpid)
		}

		kbz := extstore.GetKeyBytesFromContext(ctx)
		err = ei.m2c.Set(cpk, refstore.NewRefEntry(kbz))
		if err != nil {
			return fmt.Errorf("failed to set %T -> %T ref record: %w", *new(MT), *new(CT), err)
		}

		err = ei.c2m.Set(kbz, refstore.NewRefEntry(cpk))
		if err != nil {
			return fmt.Errorf("failed to set %T -> %T ref record: %w", *new(CT), *new(MT), err)
		}
	}

	return nil
}
