package record_test

import (
	"fmt"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/stretchr/testify/require"
)

func TestIndexAndAggregation(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		store := record.New[int64, pet]("pets")
		byName := index.On(store, "by_name", field.Of("Name", func(p *pet) string { return p.Name }))
		unique := index.Unique(store, "by_age", field.Of("Age", func(p *pet) int { return p.Age }))
		counted := agg.Count(store, "count", func(p *pet) (string, bool) { return p.Name, true })
		require.NoError(t, store.Check(db))

		// What fails is rolled back with the transaction that it is in.
		err := db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(&pet{id: 4, Name: "baz", Age: 3})
		})
		require.NoError(t, err)
		err = db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Set(&pet{id: 5, Name: "baz", Age: 3})
		})
		require.ErrorIs(t, err, index.ErrUniqueViolation)
		err = db.Update(func(tx kv.Tx) error {
			return store.Instantiate(tx).Delete(4)
		})
		require.NoError(t, err)

		err = db.Update(func(tx kv.Tx) error {
			ins := store.Instantiate(tx)
			require.NoError(t, ins.Set(&pet{id: 1, Name: "foo", Age: 1}))
			require.NoError(t, ins.Set(&pet{id: 2, Name: "foo", Age: 2}))
			require.NoError(t, ins.Set(&pet{id: 3, Name: "bar", Age: 3}))

			names := byName.Instantiate(tx)
			it, err := names.Match("foo")
			require.NoError(t, err)
			// The records are read through the store, so they have their ids.
			require.Equal(t, []int64{1, 2}, idsOf(t, it))

			it, err = names.Range(kvutil.All[string]())
			require.NoError(t, err)
			require.Equal(t, []int64{3, 1, 2}, idsOf(t, it))

			id, found, err := unique.Instantiate(tx).Find(3)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(3), id)

			count, ok, err := counted.Instantiate(tx).Get("foo")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, int64(2), count)

			// What is deleted is taken out.
			require.NoError(t, ins.Delete(1))
			it, err = names.Match("foo")
			require.NoError(t, err)
			require.Equal(t, []int64{2}, idsOf(t, it))
			count, _, err = counted.Instantiate(tx).Get("foo")
			require.NoError(t, err)
			require.Equal(t, int64(1), count)
			return nil
		})
		require.NoError(t, err)
	})
}

// Example_index defines a store of records that are given their ids, with an index on it.
// It uses the User of the example of the package.
func Example_index() {
	next := int64(0)
	users := record.New[int64, User]("users").
		WithIDFunc(func(*User) (int64, error) {
			next++
			return next, nil
		})
	byTeam := index.On(users, "by_team", field.Path[User, string]("Team"))

	db := memkv.New()
	if err := users.Check(db); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		ins := users.Instantiate(tx)
		for _, u := range []*User{
			{Name: "Ann", Team: "red"},
			{Name: "Bob", Team: "blue"},
			{Name: "Cy", Team: "red"},
		} {
			if err := ins.Set(u); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		it, err := byTeam.Instantiate(tx).Match("red")
		if err != nil {
			return err
		}
		red, err := iterutil.Of(it).
			Map(func(id int64, u *User) (string, error) { return fmt.Sprint(id, ":", u.Name), nil }).
			Values()
		if err != nil {
			return err
		}
		fmt.Println("red:", red)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// red: [1:Ann 3:Cy]
}
