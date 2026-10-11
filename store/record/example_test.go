package record_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// User is a record. Its id is the key it is stored under, so it is not part of what is serialized.
type User struct {
	id   int64
	Name string
	Team string
}

func (u *User) GetId() int64 { return u.id }

func (u *User) SetId(id int64) { u.id = id }

// Example defines a store of records that are given their ids, with an index on it.
func Example() {
	sch := schema.New()

	next := int64(0)
	users := record.New[int64, User](sch.Namespace("users")).
		WithIDFunc(func(*User) (int64, error) {
			next++
			return next, nil
		})
	byTeam := index.On(users, "by_team", field.Path[User, string]("Team"))

	db := memkv.New()
	if err := sch.Check(db); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		ins := users.Instantiate(tx)
		for _, u := range []*User{
			{Name: "Ann", Team: "red"},
			{Name: "Bob", Team: "blue"},
			{Name: "Cy", Team: "red"},
		} {
			// A record without an id is given one, which is set on it.
			if err := ins.Set(u); err != nil {
				return err
			}
			fmt.Println("set", u.GetId(), u.Name)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		bob, err := users.Instantiate(tx).Get(2)
		if err != nil {
			return err
		}
		fmt.Println("get:", bob.GetId(), bob.Name)

		_, err = users.Instantiate(tx).Get(9)
		fmt.Println("missing:", errors.Is(err, kv.ErrNotFound))

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
	// set 1 Ann
	// set 2 Bob
	// set 3 Cy
	// get: 2 Bob
	// missing: true
	// red: [1:Ann 3:Cy]
}
