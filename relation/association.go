package relation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/ref"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// ErrNoParent is returned when a child is set without a parent, in an association that is not partial.
var ErrNoParent = errors.New("relation: child has no parent")

// The bytes after the prefix of an association that the references of its two directions are
// kept under.
const (
	parentsKey  = 'p'
	childrenKey = 'c'
)

// Association is a one-to-many relation between the records of a parent store and the records of
// a child store: a child belongs to one parent. Deleting a parent deletes its children, through
// the child store, so the extensions of the child store see the deletes.
//
// Which parent a child belongs to is given when the child is set, with the option that Under
// makes, or it is a field of the child if WithParentID is used. The association keeps the
// parent of every child, and the children of every parent, under its prefix: the children of
// every parent are under the prefix followed by 'p', and the parent of every child under the
// prefix followed by 'c'.
//
// It is a unit of its own, which records its version and its codecs under its prefix.
type Association[PI comparable, PT any, CI comparable, CT any] struct {
	unit *migrate.Unit
	// label is the prefix as text, which names the extension and the delete hook that the
	// association adds to the stores.
	label    string
	parents  *record.Store[PI, PT]
	children *record.Store[CI, CT]
	pidCodec codec.KeyCodec[PI]
	cidCodec codec.KeyCodec[CI]
	partial  bool
	parentID func(*CT) (PI, bool)
	// pc has the parent key, in the form that marks where it ends, refer to the key of a child.
	pc *ref.Store
	// cp has the key of a child refer to the key of its parent.
	cp *ref.Store
}

// Associate defines a new Association under a prefix, which makes the records of the
// child store belong to the records of the parent store.
// It panics if the prefix is empty or either store is nil.
func Associate[PI comparable, PT any, CI comparable, CT any, P ~string | ~[]byte](
	prefix P,
	parents *record.Store[PI, PT],
	children *record.Store[CI, CT],
) *Association[PI, PT, CI, CT] {
	// These are mistakes in the definition, which is made in code when the program starts.
	p := bytes.Clone([]byte(prefix))
	if len(p) == 0 {
		panic("association: the prefix is required")
	}
	label := migrate.Label(p)
	if parents == nil {
		panic(fmt.Sprintf("association %q: the parent store is required", label))
	}
	if children == nil {
		panic(fmt.Sprintf("association %q: the child store is required", label))
	}

	a := &Association[PI, PT, CI, CT]{
		label:    label,
		parents:  parents,
		children: children,
		pidCodec: parents.KeyCodec(),
		cidCodec: children.KeyCodec(),
	}
	a.unit = migrate.NewUnit(p, "association", func() map[string]string {
		return map[string]string{
			"parent":  a.pidCodec.Name(),
			"child":   a.cidCodec.Name(),
			"partial": strconv.FormatBool(a.partial),
		}
	})
	a.pc = ref.New(under(p, parentsKey))
	a.cp = ref.NewUnique(under(p, childrenKey))

	// The label of the prefix is the name of the association, which is unique as prefixes do
	// not overlap.
	children.AddExtension(label, childExtension[PI, PT, CI, CT]{a: a})
	parents.AddDeleteHook(label, a.deleteChildren)
	return a
}

// Partial allows children to exist without a parent. A child that is set without one, or whose
// parent id function says that it has none, has no parent: a child that had one loses it.
func (a *Association[PI, PT, CI, CT]) Partial() *Association[PI, PT, CI, CT] {
	a.partial = true
	return a
}

// WithParentID makes the parent of a child a field of the child. The function returns the id of
// the parent of a child, or false if the child has none. The option of Under is not needed
// then, and is ignored.
func (a *Association[PI, PT, CI, CT]) WithParentID(f func(*CT) (PI, bool)) *Association[PI, PT, CI, CT] {
	a.parentID = f
	return a
}

// Name returns the name of the association, which is its prefix as text unless the name of its
// unit was set.
func (a *Association[PI, PT, CI, CT]) Name() string {
	return a.unit.Name()
}

// Prefix returns the prefix of the association. The slice must not be modified.
func (a *Association[PI, PT, CI, CT]) Prefix() []byte {
	return a.unit.Prefix()
}

// Unit returns what the association records of itself, which is migrated as a whole.
func (a *Association[PI, PT, CI, CT]) Unit() *migrate.Unit {
	return a.unit
}

// WithVersion sets the version of the data of the association, which is 1 by default. It is
// raised when the data must be converted, and a migration is added to the unit from every
// version before it.
func (a *Association[PI, PT, CI, CT]) WithVersion(v uint64) *Association[PI, PT, CI, CT] {
	a.unit.SetVersion(v)
	return a
}

// Migrate brings the database to what the association defines; see migrate.Run.
func (a *Association[PI, PT, CI, CT]) Migrate(ctx context.Context, db kv.DB) (*migrate.Report, error) {
	return migrate.Run(ctx, db, a)
}

// Check compares the database with what the association defines, without changing it; see
// migrate.Check.
func (a *Association[PI, PT, CI, CT]) Check(db kv.DB) error {
	return migrate.Check(db, a)
}

// underOption says which parent a child that is being set belongs to.
type underOption[PI comparable] struct {
	id PI
}

// Under returns the option that says which parent a child that is being set belongs to, for
// associations without WithParentID:
//
//	children.Instantiate(tx).Set(child, pets.Under(ownerID))
//
// Only the association receives it, so a store can be the child store of several associations.
func (a *Association[PI, PT, CI, CT]) Under(parentID PI) any {
	return ext.For(a.label, underOption[PI]{id: parentID})
}

// Instantiate creates the instance of the association for a transaction.
func (a *Association[PI, PT, CI, CT]) Instantiate(tx kv.Tx) *AssociationInstance[PI, PT, CI, CT] {
	return &AssociationInstance[PI, PT, CI, CT]{
		a:        a,
		parents:  a.parents.Instantiate(tx),
		children: a.children.Instantiate(tx),
		pc:       a.pc.Instantiate(tx),
		cp:       a.cp.Instantiate(tx),
	}
}

// parentOf returns the id of the parent of a child that is being set.
func (a *Association[PI, PT, CI, CT]) parentOf(child *CT, opts []any) (PI, bool) {
	if a.parentID != nil {
		return a.parentID(child)
	}

	for _, opt := range opts {
		if u, ok := opt.(underOption[PI]); ok {
			return u.id, true
		}
	}
	var zero PI
	return zero, false
}

// deleteChildren is the delete hook of the parent store.
func (a *Association[PI, PT, CI, CT]) deleteChildren(tx kv.Tx, id PI, _ []byte) error {
	pk, err := form(a.pidCodec, id)
	if err != nil {
		return err
	}
	pc, cp := a.pc.Instantiate(tx), a.cp.Instantiate(tx)

	// The children are collected first, as deleting a child deletes the reference that is iterated over.
	raws, err := childrenOf(pc, pk.app)
	if err != nil {
		return err
	}

	store := a.children.Instantiate(tx)
	for _, raw := range raws {
		cid, err := a.cidCodec.Decode(raw)
		if err != nil {
			return fmt.Errorf("failed to decode the id of a child: %w", err)
		}
		if err := store.Delete(cid); err != nil {
			return fmt.Errorf("failed to delete child %v: %w", cid, err)
		}

		// A reference whose child does not exist is dropped all the same.
		if err := pc.Delete(pk.app, raw); err != nil {
			return err
		}
		if err := cp.Delete(raw, pk.enc); err != nil {
			return err
		}
	}
	return nil
}

// childrenOf returns the keys of the children of a parent, as the child store encodes them.
func childrenOf(pc *ref.Instance, parent []byte) ([][]byte, error) {
	it, err := pc.Iterator(parent, kv.PrefixEnd(parent))
	if err != nil {
		return nil, err
	}

	var children [][]byte
	for ; it.Valid(); it.Next() {
		pk, err := it.PrimaryKey()
		if err != nil {
			it.Close()
			return nil, err
		}
		children = append(children, bytes.Clone(pk))
	}
	return children, it.Close()
}

// parentOfChild returns the key of the parent of a child, as the parent store encodes it, and
// whether the child has one.
func parentOfChild(cp *ref.Instance, child []byte) ([]byte, bool, error) {
	// The range has the one key that is the child alone.
	it, err := cp.Iterator(child, append(bytes.Clone(child), 0))
	if err != nil {
		return nil, false, err
	}
	defer it.Close()

	if !it.Valid() {
		return nil, false, nil
	}
	pk, err := it.PrimaryKey()
	if err != nil {
		return nil, false, err
	}
	return bytes.Clone(pk), true, nil
}

// childExtension is the extension of the child store that keeps the references of the association.
type childExtension[PI comparable, PT any, CI comparable, CT any] struct {
	a *Association[PI, PT, CI, CT]
}

// Instantiate implements the ext.Extension interface.
func (e childExtension[PI, PT, CI, CT]) Instantiate(tx kv.Tx) ext.ExtensionInstance[CI, CT] {
	return &childMaintenance[PI, PT, CI, CT]{
		a:  e.a,
		tx: tx,
		pc: e.a.pc.Instantiate(tx),
		cp: e.a.cp.Instantiate(tx),
	}
}

type childMaintenance[PI comparable, PT any, CI comparable, CT any] struct {
	a  *Association[PI, PT, CI, CT]
	tx kv.Tx
	pc *ref.Instance
	cp *ref.Instance
}

// OnSet implements the ext.ExtensionInstance interface.
func (m *childMaintenance[PI, PT, CI, CT]) OnSet(key CI, rawKey []byte, _, new *CT, opts ...any) error {
	a := m.a
	pid, ok := a.parentOf(new, opts)
	if !ok && !a.partial {
		return fmt.Errorf("%w: child %v in association %q", ErrNoParent, key, a.label)
	}

	current, has, err := parentOfChild(m.cp, rawKey)
	if err != nil {
		return err
	}
	if !ok {
		// A child of a partial association that has no parent now.
		if has {
			return m.unlink(rawKey, current)
		}
		return nil
	}

	exists, err := a.parents.Contains(m.tx, pid)
	if err != nil {
		return fmt.Errorf("failed to look up the parent: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: parent %v of child %v in association %q", ErrMissingRecord, pid, key, a.label)
	}

	parent, err := form(a.pidCodec, pid)
	if err != nil {
		return err
	}
	if has && !bytes.Equal(current, parent.enc) {
		// The child moves to another parent.
		if err := m.unlink(rawKey, current); err != nil {
			return err
		}
	}

	if err := m.cp.Set(rawKey, parent.enc, nil); err != nil {
		return err
	}
	return m.pc.Set(parent.app, rawKey, nil)
}

// OnDelete implements the ext.ExtensionInstance interface.
func (m *childMaintenance[PI, PT, CI, CT]) OnDelete(_ CI, rawKey []byte, _ *CT) error {
	current, has, err := parentOfChild(m.cp, rawKey)
	if err != nil || !has {
		return err
	}
	return m.unlink(rawKey, current)
}

// unlink removes the references between a child and its parent.
func (m *childMaintenance[PI, PT, CI, CT]) unlink(child, parent []byte) error {
	pid, err := m.a.pidCodec.Decode(parent)
	if err != nil {
		return fmt.Errorf("failed to decode the id of a parent: %w", err)
	}
	pk, err := form(m.a.pidCodec, pid)
	if err != nil {
		return err
	}

	if err := m.pc.Delete(pk.app, child); err != nil {
		return err
	}
	return m.cp.Delete(child, parent)
}

// AssociationInstance is an Association in a transaction.
type AssociationInstance[PI comparable, PT any, CI comparable, CT any] struct {
	a        *Association[PI, PT, CI, CT]
	parents  *record.Instance[PI, PT]
	children *record.Instance[CI, CT]
	pc       *ref.Instance
	cp       *ref.Instance
}

// Set sets the parent, and then each of the children under it. A parent without an id is given
// one by its store, as it is when it is set there. If the association has a parent id function,
// the children must carry the id of the parent already, and an error is returned for a child
// that carries another one.
func (i *AssociationInstance[PI, PT, CI, CT]) Set(parent *PT, children ...*CT) error {
	a := i.a
	if err := i.parents.Set(parent); err != nil {
		return fmt.Errorf("failed to set parent: %w", err)
	}
	// The id that the parent has now, which its store may have given it.
	pid := a.parents.IDOf(parent)

	for _, child := range children {
		if child == nil {
			return fmt.Errorf("failed to set child: %w", typed.ErrNilValue)
		}

		if a.parentID == nil {
			if err := i.children.Set(child, a.Under(pid)); err != nil {
				return fmt.Errorf("failed to set child: %w", err)
			}
			continue
		}

		got, ok := a.parentID(child)
		switch {
		case !ok:
			return fmt.Errorf("%w: the child does not carry the id of its parent %v in association %q",
				ErrNoParent, pid, a.label)
		case got != pid:
			return fmt.Errorf("association %q: the child carries the id %v of another parent than %v",
				a.label, got, pid)
		}
		if err := i.children.Set(child); err != nil {
			return fmt.Errorf("failed to set child: %w", err)
		}
	}
	return nil
}

// ParentID returns the id of the parent of a child, or false if the child has none.
func (i *AssociationInstance[PI, PT, CI, CT]) ParentID(childID CI) (PI, bool, error) {
	var zero PI
	child, err := i.a.cidCodec.Encode(childID)
	if err != nil {
		return zero, false, fmt.Errorf("failed to encode key %v: %w", childID, err)
	}

	parent, ok, err := parentOfChild(i.cp, child)
	if err != nil || !ok {
		return zero, false, err
	}
	pid, err := i.a.pidCodec.Decode(parent)
	if err != nil {
		return zero, false, fmt.Errorf("failed to decode the id of a parent: %w", err)
	}
	return pid, true, nil
}

// Parent returns the parent of a child, or kv.ErrNotFound if the child has none.
func (i *AssociationInstance[PI, PT, CI, CT]) Parent(childID CI) (*PT, error) {
	pid, ok, err := i.ParentID(childID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, kv.ErrNotFound
	}
	return i.parents.Get(pid)
}

// Children iterates over the children of a parent, in the order of their ids. The records are
// read when they are asked for, and have their ids set. The iterator must be closed.
func (i *AssociationInstance[PI, PT, CI, CT]) Children(parentID PI) (iterutil.Iterator[CI, *CT], error) {
	pk, err := form(i.a.pidCodec, parentID)
	if err != nil {
		return nil, err
	}

	base, err := i.pc.Iterator(pk.app, kv.PrefixEnd(pk.app))
	if err != nil {
		return nil, err
	}
	return &childIterator[CI, CT]{base: base, codec: i.a.cidCodec, children: i.children}, nil
}

// childIterator iterates over the children of a parent.
type childIterator[CI comparable, CT any] struct {
	base     *ref.Iterator
	codec    codec.KeyCodec[CI]
	children *record.Instance[CI, CT]
}

// Valid implements the iterutil.Iterator interface.
func (it *childIterator[CI, CT]) Valid() bool {
	return it.base.Valid()
}

// Next implements the iterutil.Iterator interface.
func (it *childIterator[CI, CT]) Next() {
	it.base.Next()
}

// Key implements the iterutil.Iterator interface.
func (it *childIterator[CI, CT]) Key() (CI, error) {
	pk, err := it.base.PrimaryKey()
	if err != nil {
		var zero CI
		return zero, err
	}
	return it.codec.Decode(pk)
}

// Value implements the iterutil.Iterator interface. The record is read from the child store.
func (it *childIterator[CI, CT]) Value() (*CT, error) {
	id, err := it.Key()
	if err != nil {
		return nil, err
	}
	return it.children.Get(id)
}

// Close implements the iterutil.Iterator interface.
func (it *childIterator[CI, CT]) Close() error {
	return it.base.Close()
}
