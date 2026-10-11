package relation_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/stretchr/testify/require"
)

// person is a parent record.
type person struct {
	id   int64
	Name string
}

func (p *person) GetId() int64 { return p.id }

func (p *person) SetId(id int64) { p.id = id }

// pet is a child record, which carries the id of its owner as well.
type pet struct {
	id    int64
	Name  string
	Owner int64
}

func (p *pet) GetId() int64 { return p.id }

func (p *pet) SetId(id int64) { p.id = id }

// ownerOf is the parent id function of the pets that carry the id of their owner.
func ownerOf(p *pet) (int64, bool) { return p.Owner, p.Owner != 0 }

// petIDs collects the ids of the children of an iterator, whose records must have them set.
func petIDs(t *testing.T, it iterutil.Iterator[int64, *pet]) []int64 {
	t.Helper()

	ids := []int64{}
	for _, e := range drain(t, it) {
		require.Equal(t, e.Key, e.Data.id)
		ids = append(ids, e.Key)
	}
	return ids
}

// parentOf returns the id of the parent of a child, or 0 if it has none.
func parentOf[PI comparable, PT any, CI comparable, CT any](
	t *testing.T,
	ins *relation.AssociationInstance[PI, PT, CI, CT],
	child CI,
) PI {
	t.Helper()

	pid, _, err := ins.ParentID(child)
	require.NoError(t, err)
	return pid
}

func TestAssociationParentSide(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
		require.NoError(t, sch.Check(db))
		require.Equal(t, "ownership", ownership.Name())

		err := db.Update(func(tx kv.Tx) error {
			ins := ownership.Instantiate(tx)
			p1, p2 := &person{id: 1, Name: "P1"}, &person{id: 2, Name: "P2"}
			c1, c2 := &pet{id: 1, Name: "C1"}, &pet{id: 2, Name: "C2"}
			require.NoError(t, ins.Set(p1, c1))
			require.NoError(t, ins.Set(p2, c2))

			// A parent and a child reference each other once.
			require.Equal(t, 4, rawCount(t, tx, ownership.Namespace()))
			require.Equal(t, int64(1), parentOf(t, ins, 1))
			require.Equal(t, int64(2), parentOf(t, ins, 2))

			// Deleting a parent deletes its children and their references.
			require.NoError(t, owners.Instantiate(tx).Delete(1))
			require.False(t, must(owners.Instantiate(tx).Has(1)))
			require.False(t, must(pets.Instantiate(tx).Has(1)))
			require.True(t, must(owners.Instantiate(tx).Has(2)))
			require.True(t, must(pets.Instantiate(tx).Has(2)))
			require.Equal(t, 2, rawCount(t, tx, ownership.Namespace()))
			_, ok, err := ins.ParentID(1)
			require.NoError(t, err)
			require.False(t, ok)
			require.Equal(t, []int64{2}, petIDs(t, must(ins.Children(2))))
			require.Empty(t, petIDs(t, must(ins.Children(1))))

			// Deleting a parent that has no children is fine.
			require.NoError(t, owners.Instantiate(tx).Delete(2))
			require.NoError(t, owners.Instantiate(tx).Delete(2))
			require.Zero(t, rawCount(t, tx, ownership.Namespace()))
			require.False(t, must(pets.Instantiate(tx).Has(2)))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestAssociationChildSide(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := ownership.Instantiate(tx)
			store := pets.Instantiate(tx)
			c1, c2 := &pet{id: 1, Name: "C1"}, &pet{id: 2, Name: "C2"}

			// A child needs a parent.
			err := store.Set(c1)
			require.ErrorIs(t, err, relation.ErrNoParent)

			// And the parent has to exist.
			err = store.Set(c1, ownership.Under(2))
			require.ErrorIs(t, err, relation.ErrMissingRecord)
			require.False(t, must(store.Has(1)))
			require.Zero(t, rawCount(t, tx, ownership.Namespace()))

			require.NoError(t, owners.Instantiate(tx).Set(&person{id: 1, Name: "P1"}))
			require.NoError(t, store.Set(c1, ownership.Under(1)))
			require.NoError(t, store.Set(c2, ownership.Under(1)))
			require.Equal(t, 4, rawCount(t, tx, ownership.Namespace()))
			require.Equal(t, []int64{1, 2}, petIDs(t, must(ins.Children(1))))

			// Deleting a child leaves its parent and its siblings.
			require.NoError(t, store.Delete(1))
			require.Equal(t, 2, rawCount(t, tx, ownership.Namespace()))
			require.True(t, must(owners.Instantiate(tx).Has(1)))
			require.Equal(t, []int64{2}, petIDs(t, must(ins.Children(1))))
			_, ok, err := ins.ParentID(1)
			require.NoError(t, err)
			require.False(t, ok)

			// A child that is set again under the same parent is not referenced twice.
			require.NoError(t, store.Set(&pet{id: 2, Name: "C2 again"}, ownership.Under(1)))
			require.Equal(t, 2, rawCount(t, tx, ownership.Namespace()))
			require.Equal(t, "C2 again", must(store.Get(2)).Name)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestAssociationParentIDField(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets).WithParentID(ownerOf)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := ownership.Instantiate(tx)
			store := pets.Instantiate(tx)
			add := func(id int64) {
				require.NoError(t, owners.Instantiate(tx).Set(&person{id: id, Name: fmt.Sprint("P", id)}))
			}
			add(1)
			add(2)

			// The parent is a field of the child.
			require.NoError(t, store.Set(&pet{id: 1, Name: "C1", Owner: 1}))
			require.NoError(t, store.Set(&pet{id: 2, Name: "C2", Owner: 1}))
			require.NoError(t, store.Set(&pet{id: 3, Name: "C3", Owner: 2}))
			require.Equal(t, int64(1), parentOf(t, ins, 1))
			require.Equal(t, int64(2), parentOf(t, ins, 3))
			require.Equal(t, []int64{1, 2}, petIDs(t, must(ins.Children(1))))

			// A child without one does not belong anywhere, and one with a missing parent is refused.
			err := store.Set(&pet{id: 4, Name: "C4"})
			require.ErrorIs(t, err, relation.ErrNoParent)
			err = store.Set(&pet{id: 4, Name: "C4", Owner: 9})
			require.ErrorIs(t, err, relation.ErrMissingRecord)
			require.False(t, must(store.Has(4)))

			// Changing the field and setting the child again moves it.
			require.NoError(t, store.Set(&pet{id: 2, Name: "C2", Owner: 2}))
			require.Equal(t, []int64{1}, petIDs(t, must(ins.Children(1))))
			require.Equal(t, []int64{2, 3}, petIDs(t, must(ins.Children(2))))
			require.Equal(t, int64(2), parentOf(t, ins, 2))
			require.Equal(t, 6, rawCount(t, tx, ownership.Namespace()))

			// Deleting a child removes it from its parent, whose other children are left.
			require.NoError(t, store.Delete(3))
			require.Equal(t, []int64{2}, petIDs(t, must(ins.Children(2))))

			// Deleting the parent that the child moved to deletes it.
			require.NoError(t, owners.Instantiate(tx).Delete(2))
			require.False(t, must(store.Has(2)))
			require.True(t, must(store.Has(1)))
			require.Equal(t, 2, rawCount(t, tx, ownership.Namespace()))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestAssociationPartial(t *testing.T) {
	t.Run("under", func(t *testing.T) {
		testkv.Each(t, func(t *testing.T, db kv.DB) {
			sch := schema.New()
			owners := record.New[int64, person](sch.Namespace("owners"))
			pets := record.New[int64, pet](sch.Namespace("pets"))
			ownership := relation.Associate(sch.Namespace("ownership"), owners, pets).Partial()
			require.NoError(t, sch.Check(db))

			err := db.Update(func(tx kv.Tx) error {
				ins := ownership.Instantiate(tx)
				store := pets.Instantiate(tx)
				require.NoError(t, owners.Instantiate(tx).Set(&person{id: 1, Name: "P1"}))

				// A child can exist without a parent.
				require.NoError(t, store.Set(&pet{id: 1, Name: "C1"}))
				_, ok, err := ins.ParentID(1)
				require.NoError(t, err)
				require.False(t, ok)
				_, err = ins.Parent(1)
				require.ErrorIs(t, err, kv.ErrNotFound)
				require.Zero(t, rawCount(t, tx, ownership.Namespace()))

				// But the parent that it is given has to exist.
				err = store.Set(&pet{id: 1, Name: "C1"}, ownership.Under(5))
				require.ErrorIs(t, err, relation.ErrMissingRecord)

				// It can be given a parent, and lose it.
				require.NoError(t, store.Set(&pet{id: 1, Name: "C1"}, ownership.Under(1)))
				require.Equal(t, int64(1), parentOf(t, ins, 1))
				require.NoError(t, store.Set(&pet{id: 1, Name: "C1"}))
				_, ok, err = ins.ParentID(1)
				require.NoError(t, err)
				require.False(t, ok)
				require.Empty(t, petIDs(t, must(ins.Children(1))))
				require.Zero(t, rawCount(t, tx, ownership.Namespace()))

				// A child without a parent is left when the other ones are deleted with theirs.
				require.NoError(t, store.Set(&pet{id: 2, Name: "C2"}, ownership.Under(1)))
				require.NoError(t, owners.Instantiate(tx).Delete(1))
				require.True(t, must(store.Has(1)))
				require.False(t, must(store.Has(2)))
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("field", func(t *testing.T) {
		testkv.Each(t, func(t *testing.T, db kv.DB) {
			sch := schema.New()
			owners := record.New[int64, person](sch.Namespace("owners"))
			pets := record.New[int64, pet](sch.Namespace("pets"))
			ownership := relation.Associate(sch.Namespace("ownership"), owners, pets).
				Partial().WithParentID(ownerOf)
			require.NoError(t, sch.Check(db))

			err := db.Update(func(tx kv.Tx) error {
				ins := ownership.Instantiate(tx)
				store := pets.Instantiate(tx)
				require.NoError(t, owners.Instantiate(tx).Set(&person{id: 1, Name: "P1"}))

				require.NoError(t, store.Set(&pet{id: 1, Name: "C1"}))
				require.NoError(t, store.Set(&pet{id: 2, Name: "C2", Owner: 1}))
				require.Equal(t, []int64{2}, petIDs(t, must(ins.Children(1))))

				// Clearing the field makes the child lose its parent.
				require.NoError(t, store.Set(&pet{id: 2, Name: "C2"}))
				require.Empty(t, petIDs(t, must(ins.Children(1))))
				_, ok, err := ins.ParentID(2)
				require.NoError(t, err)
				require.False(t, ok)
				return nil
			})
			require.NoError(t, err)
		})
	})
}

func TestAssociationReads(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := ownership.Instantiate(tx)
			require.NoError(t, ins.Set(&person{id: 1, Name: "Ann"}, &pet{id: 30, Name: "c"}, &pet{id: 10, Name: "a"}))
			require.NoError(t, ins.Set(&person{id: 2, Name: "Bob"}, &pet{id: 20, Name: "b"}, &pet{id: -5, Name: "z"}))

			// The parent of a child.
			pid, ok, err := ins.ParentID(10)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, int64(1), pid)
			parent, err := ins.Parent(-5)
			require.NoError(t, err)
			require.Equal(t, int64(2), parent.GetId())
			require.Equal(t, "Bob", parent.Name)

			// A child that does not exist has none.
			_, ok, err = ins.ParentID(99)
			require.NoError(t, err)
			require.False(t, ok)
			_, err = ins.Parent(99)
			require.ErrorIs(t, err, kv.ErrNotFound)

			// The children are in the order of their ids, with the ids set on their records.
			require.Equal(t, []int64{10, 30}, petIDs(t, must(ins.Children(1))))
			require.Equal(t, []int64{-5, 20}, petIDs(t, must(ins.Children(2))))
			require.Empty(t, petIDs(t, must(ins.Children(3))))

			// The records are read when they are asked for.
			it := must(ins.Children(1))
			defer it.Close()
			require.True(t, it.Valid())
			require.Equal(t, int64(10), must(it.Key()))
			require.NoError(t, pets.Instantiate(tx).Set(&pet{id: 30, Name: "renamed"}, ownership.Under(1)))
			require.Equal(t, "a", must(it.Value()).Name)
			it.Next()
			require.Equal(t, int64(30), must(it.Key()))
			require.Equal(t, "renamed", must(it.Value()).Name)
			it.Next()
			require.False(t, it.Valid())
			return nil
		})
		require.NoError(t, err)
	})
}

func TestAssociationSet(t *testing.T) {
	t.Run("under", func(t *testing.T) {
		testkv.Each(t, func(t *testing.T, db kv.DB) {
			sch := schema.New()
			next := int64(100)
			owners := record.New[int64, person](sch.Namespace("owners")).
				WithIDFunc(func(*person) (int64, error) { next++; return next, nil })
			pets := record.New[int64, pet](sch.Namespace("pets"))
			ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
			require.NoError(t, sch.Check(db))

			err := db.Update(func(tx kv.Tx) error {
				ins := ownership.Instantiate(tx)

				// The parent gets its id from the store, and the children are set under it.
				ann := &person{Name: "Ann"}
				require.NoError(t, ins.Set(ann, &pet{id: 1, Name: "a"}, &pet{id: 2, Name: "b"}))
				require.Equal(t, int64(101), ann.GetId())
				require.Equal(t, int64(101), parentOf(t, ins, 1))
				require.Equal(t, []int64{1, 2}, petIDs(t, must(ins.Children(101))))

				// A parent with an id keeps it, and children are added to those that it has.
				bob := &person{id: 7, Name: "Bob"}
				require.NoError(t, ins.Set(bob, &pet{id: 3, Name: "c"}))
				require.NoError(t, ins.Set(bob, &pet{id: 4, Name: "d"}))
				require.Equal(t, []int64{3, 4}, petIDs(t, must(ins.Children(7))))

				// A child of another parent is moved.
				require.NoError(t, ins.Set(bob, &pet{id: 1, Name: "a"}))
				require.Equal(t, []int64{2}, petIDs(t, must(ins.Children(101))))
				require.Equal(t, []int64{1, 3, 4}, petIDs(t, must(ins.Children(7))))

				// Without children it only sets the parent.
				require.NoError(t, ins.Set(&person{id: 8, Name: "Cy"}))
				require.True(t, must(owners.Instantiate(tx).Has(8)))

				err := ins.Set(nil, &pet{id: 5})
				require.ErrorIs(t, err, typed.ErrNilValue)
				err = ins.Set(bob, nil)
				require.ErrorIs(t, err, typed.ErrNilValue)
				return nil
			})
			require.NoError(t, err)
		})
	})

	t.Run("field", func(t *testing.T) {
		testkv.Each(t, func(t *testing.T, db kv.DB) {
			sch := schema.New()
			owners := record.New[int64, person](sch.Namespace("owners"))
			pets := record.New[int64, pet](sch.Namespace("pets"))
			ownership := relation.Associate(sch.Namespace("ownership"), owners, pets).WithParentID(ownerOf)
			require.NoError(t, sch.Check(db))

			err := db.Update(func(tx kv.Tx) error {
				ins := ownership.Instantiate(tx)
				ann := &person{id: 1, Name: "Ann"}
				require.NoError(t, ins.Set(ann, &pet{id: 1, Name: "a", Owner: 1}, &pet{id: 2, Name: "b", Owner: 1}))
				require.Equal(t, []int64{1, 2}, petIDs(t, must(ins.Children(1))))

				// The children carry the id of their parent.
				err := ins.Set(ann, &pet{id: 3, Name: "c", Owner: 2})
				require.ErrorContains(t, err, "another parent")
				require.False(t, must(pets.Instantiate(tx).Has(3)))
				err = ins.Set(ann, &pet{id: 3, Name: "c"})
				require.ErrorIs(t, err, relation.ErrNoParent)
				require.False(t, must(pets.Instantiate(tx).Has(3)))
				return nil
			})
			require.NoError(t, err)
		})
	})
}

func TestAssociationCascadeReachesExtensions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		byName := index.On(pets, "by_name", field.Path[pet, string]("Name"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			ins := ownership.Instantiate(tx)
			names := byName.Instantiate(tx)
			require.NoError(t, ins.Set(&person{id: 1, Name: "Ann"},
				&pet{id: 1, Name: "rex"}, &pet{id: 2, Name: "tom"}, &pet{id: 3, Name: "rex"}))
			require.NoError(t, ins.Set(&person{id: 2, Name: "Bob"}, &pet{id: 4, Name: "rex"}, &pet{id: 5, Name: "kit"}))
			require.Equal(t, []int64{1, 3, 4}, must(iterutil.Of(must(names.Match("rex"))).Keys()))
			require.Equal(t, 5, rawCount(t, tx, byName.Namespace()))

			// The index of the children sees them go with their parent.
			require.NoError(t, owners.Instantiate(tx).Delete(1))
			require.Equal(t, []int64{4}, must(iterutil.Of(must(names.Match("rex"))).Keys()))
			require.Empty(t, must(iterutil.Of(must(names.Match("tom"))).Keys()))
			require.Equal(t, []int64{5}, must(iterutil.Of(must(names.Match("kit"))).Keys()))
			require.Equal(t, 2, rawCount(t, tx, byName.Namespace()))

			// And a child that is deleted by itself leaves its parent and siblings.
			require.NoError(t, pets.Instantiate(tx).Delete(4))
			require.Empty(t, must(iterutil.Of(must(names.Match("rex"))).Keys()))
			require.True(t, must(owners.Instantiate(tx).Has(2)))
			require.True(t, must(pets.Instantiate(tx).Has(5)))

			require.NoError(t, owners.Instantiate(tx).Delete(2))
			require.Zero(t, rawCount(t, tx, byName.Namespace()))
			require.Zero(t, rawCount(t, tx, ownership.Namespace()))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestAssociationTwoOnOneStore(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		vets := record.New[int64, person](sch.Namespace("vets"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
		care := relation.Associate(sch.Namespace("care"), vets, pets)
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			require.NoError(t, owners.Instantiate(tx).Set(&person{id: 1}))
			require.NoError(t, vets.Instantiate(tx).Set(&person{id: 5}))
			store := pets.Instantiate(tx)

			// Each association is told its own parent.
			require.NoError(t, store.Set(&pet{id: 1, Name: "rex"}, ownership.Under(1), care.Under(5)))
			require.Equal(t, int64(1), parentOf(t, ownership.Instantiate(tx), 1))
			require.Equal(t, int64(5), parentOf(t, care.Instantiate(tx), 1))

			// Deleting either parent deletes the child, and the other association forgets it.
			require.NoError(t, vets.Instantiate(tx).Delete(5))
			require.False(t, must(store.Has(1)))
			require.Zero(t, rawCount(t, tx, ownership.Namespace()))
			require.Zero(t, rawCount(t, tx, care.Namespace()))
			return nil
		})
		require.NoError(t, err)

		// A parent that one of them does not have is an error of that one. The transaction is not
		// committed, as the other one may have written its references already.
		err = db.Update(func(tx kv.Tx) error {
			return pets.Instantiate(tx).Set(&pet{id: 2, Name: "tom"}, ownership.Under(5), care.Under(5))
		})
		require.ErrorIs(t, err, relation.ErrMissingRecord)
		err = db.Update(func(tx kv.Tx) error {
			return pets.Instantiate(tx).Set(&pet{id: 2, Name: "tom"}, ownership.Under(1))
		})
		require.ErrorIs(t, err, relation.ErrNoParent)
		err = db.View(func(tx kv.Tx) error {
			require.False(t, must(pets.Instantiate(tx).Has(2)))
			require.Zero(t, rawCount(t, tx, ownership.Namespace()))
			return nil
		})
		require.NoError(t, err)
	})
}

// category is a record that is a child of a category.
type category struct {
	id     int64
	Name   string
	Parent int64
}

func (c *category) GetId() int64 { return c.id }

func (c *category) SetId(id int64) { c.id = id }

func TestAssociationOfStoreWithItself(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		sch := schema.New()
		categories := record.New[int64, category](sch.Namespace("categories"))
		tree := relation.Associate(sch.Namespace("tree"), categories, categories).
			Partial().
			WithParentID(func(c *category) (int64, bool) { return c.Parent, c.Parent != 0 })
		require.NoError(t, sch.Check(db))

		err := db.Update(func(tx kv.Tx) error {
			store := categories.Instantiate(tx)
			for _, c := range []*category{
				{id: 1, Name: "root"}, {id: 2, Name: "a", Parent: 1}, {id: 3, Name: "b", Parent: 1},
				{id: 4, Name: "a1", Parent: 2}, {id: 5, Name: "a2", Parent: 2}, {id: 6, Name: "other"},
			} {
				require.NoError(t, store.Set(c))
			}
			ins := tree.Instantiate(tx)
			require.Equal(t, int64(2), parentOf(t, ins, 4))

			// Deleting a category deletes its whole subtree.
			require.NoError(t, store.Delete(2))
			for id, want := range map[int64]bool{1: true, 2: false, 3: true, 4: false, 5: false, 6: true} {
				require.Equal(t, want, must(store.Has(id)), "category %d", id)
			}
			require.NoError(t, store.Delete(1))
			require.False(t, must(store.Has(3)))
			require.True(t, must(store.Has(6)))
			require.Zero(t, rawCount(t, tx, tree.Namespace()))
			return nil
		})
		require.NoError(t, err)
	})
}

func TestAssociationDefinitionPanics(t *testing.T) {
	sch := schema.New()
	owners := record.New[int64, person](sch.Namespace("owners"))
	pets := record.New[int64, pet](sch.Namespace("pets"))

	require.PanicsWithValue(t, "association: the namespace is required", func() {
		relation.Associate[int64, person, int64, pet](nil, owners, pets)
	})
	require.PanicsWithValue(t, `association "a": the parent store is required`, func() {
		relation.Associate[int64, person, int64, pet](sch.Namespace("a"), nil, pets)
	})
	require.PanicsWithValue(t, `association "b": the child store is required`, func() {
		relation.Associate[int64, person, int64, pet](sch.Namespace("b"), owners, nil)
	})
}

func TestAssociationSchema(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		define := func(partial bool) (*schema.Schema, *relation.Association[int64, person, int64, pet]) {
			sch := schema.New()
			owners := record.New[int64, person](sch.Namespace("owners"))
			pets := record.New[int64, pet](sch.Namespace("pets"))
			ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
			if partial {
				ownership.Partial()
			}
			return sch, ownership
		}

		sch, ownership := define(false)
		require.NoError(t, sch.Validate())
		def := ownership.Namespace().Definition()
		require.Equal(t, "association", def.Kind)
		require.Equal(t, map[string]string{"parent": "int64", "child": "int64", "partial": "false"}, def.Attributes())
		require.NoError(t, sch.Check(db))

		// The deletes of the parents reach the children and the association, and those of the children
		// reach the association.
		footprint := func(path string) []kv.KeyRange {
			for _, ns := range sch.Namespaces() {
				if ns.Path() == path {
					return ns.Footprint()
				}
			}
			t.Fatalf("no namespace %q", path)
			return nil
		}
		for _, path := range []string{"owners", "pets"} {
			require.Contains(t, footprint(path), ownership.Namespace().Range(), path)
		}
		require.Contains(t, footprint("owners"), footprint("pets")[0])

		// The same definition is accepted again, and one that is partial is not.
		sch, _ = define(false)
		require.NoError(t, sch.Check(db))
		sch, _ = define(true)
		require.ErrorIs(t, sch.Check(db), schema.ErrChanged)
	})
}

func TestAssociationRandom(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ops := 600
		if t.Name() != "TestAssociationRandom/Mem" {
			ops = 150
		}

		sch := schema.New()
		owners := record.New[int64, person](sch.Namespace("owners"))
		pets := record.New[int64, pet](sch.Namespace("pets"))
		ownership := relation.Associate(sch.Namespace("ownership"), owners, pets)
		// The index is defined after the association, so a child that is refused is not indexed.
		byName := index.On(pets, "by_name", field.Path[pet, string]("Name"))
		require.NoError(t, sch.Check(db))

		var (
			parentIDs = []int64{-2, 1, 2, 3, 40}
			childIDs  = []int64{-7, -1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			rng       = rand.New(rand.NewPCG(3, 5))
			model     = map[int64]int64{}
			have      = map[int64]bool{}
			largest   int
		)

		compare := func(tx kv.Tx) {
			t.Helper()
			ins := ownership.Instantiate(tx)

			for _, c := range childIDs {
				pid, ok, err := ins.ParentID(c)
				require.NoError(t, err)
				want, wantOK := model[c]
				require.Equal(t, wantOK, ok, "child %d", c)
				require.Equal(t, want, pid, "child %d", c)
				// A child exists if and only if it has a parent.
				require.Equal(t, wantOK, must(pets.Instantiate(tx).Has(c)), "child %d", c)
			}
			for _, p := range parentIDs {
				want := []int64{}
				for _, c := range slices.Sorted(slices.Values(childIDs)) {
					if parent, ok := model[c]; ok && parent == p {
						want = append(want, c)
					}
				}
				require.Equal(t, want, petIDs(t, must(ins.Children(p))), "children of %d", p)
			}
			require.Equal(t, 2*len(model), rawCount(t, tx, ownership.Namespace()))
			require.Equal(t, len(model), rawCount(t, tx, byName.Namespace()))
		}

		err := db.Update(func(tx kv.Tx) error {
			for _, p := range parentIDs {
				require.NoError(t, owners.Instantiate(tx).Set(&person{id: p}))
				have[p] = true
			}

			for n := 1; n <= ops; n++ {
				c, p := childIDs[rng.IntN(len(childIDs))], parentIDs[rng.IntN(len(parentIDs))]
				switch op := rng.IntN(20); {
				case op < 12:
					// Creating a child, or moving it to another parent.
					err := pets.Instantiate(tx).Set(&pet{id: c, Name: fmt.Sprint("pet", rng.IntN(4))}, ownership.Under(p))
					if have[p] {
						require.NoError(t, err)
						model[c] = p
					} else {
						require.True(t, errors.Is(err, relation.ErrMissingRecord), "%v", err)
					}
				case op < 16:
					require.NoError(t, pets.Instantiate(tx).Delete(c))
					delete(model, c)
				case op < 18:
					require.NoError(t, owners.Instantiate(tx).Delete(p))
					have[p] = false
					for child, parent := range model {
						if parent == p {
							delete(model, child)
						}
					}
				default:
					require.NoError(t, owners.Instantiate(tx).Set(&person{id: p}))
					have[p] = true
				}

				if n%25 == 0 {
					compare(tx)
					largest = max(largest, len(model))
				}
			}
			compare(tx)
			require.Greater(t, largest, 3)
			return nil
		})
		require.NoError(t, err)

		err = db.View(func(tx kv.Tx) error {
			compare(tx)
			return nil
		})
		require.NoError(t, err)
	})
}
