package migrate_test

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

// UserV1 is a user as the first version of the program wrote it. The type stays in the
// program, next to the migration that reads it.
type UserV1 struct {
	Name string
}

// User is a user as the program has it now: the name is in two parts.
type User struct {
	First, Last string
}

// firstVersion is the program as it was: it opens the database and writes users.
func firstVersion(db kv.DB) {
	users := typed.New[int64, UserV1]("users")
	if _, err := users.Migrate(context.Background(), db); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		store := users.Instantiate(tx)
		if err := store.Set(1, &UserV1{Name: "Ann Lee"}); err != nil {
			return err
		}
		return store.Set(2, &UserV1{Name: "Bob Ray"})
	})
	if err != nil {
		panic(err)
	}
}

// Example is the next version of a program, which reads the database of the one before.
func Example() {
	db := memkv.New()
	firstVersion(db)

	// The store is of the second version now, and the migration from the first one is
	// defined with it.
	users := typed.New[int64, User]("users").WithVersion(2).
		MigrateFrom(1, func(id int64, old *UserV1) (*User, error) {
			var u User
			fmt.Sscan(old.Name, &u.First, &u.Last)
			return &u, nil
		})

	// When the program starts, before it uses the database.
	report, err := users.Migrate(context.Background(), db)
	if err != nil {
		panic(err)
	}
	fmt.Println(report)

	err = db.View(func(tx kv.Tx) error {
		it, err := users.Instantiate(tx).All()
		if err != nil {
			return err
		}
		for id, u := range iterutil.Of(it).All(&err) {
			fmt.Println(id, u.Last+",", u.First)
		}
		return err
	})
	if err != nil {
		panic(err)
	}

	// The next time there is nothing to do.
	report, err = users.Migrate(context.Background(), db)
	fmt.Printf("%q %v\n", report, err)

	// Output:
	// migrated typed "users" from version 1 to 2
	// 1 Lee, Ann
	// 2 Ray, Bob
	// "" <nil>
}
