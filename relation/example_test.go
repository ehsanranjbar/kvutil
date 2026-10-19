package relation_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// Account is a record of a store. Its id is the key it is stored under.
type Account struct {
	id   int64
	Name string
}

func (a *Account) GetId() int64 { return a.id }

func (a *Account) SetId(id int64) { a.id = id }

// Team is a record of another store.
type Team struct {
	id   int64
	Name string
}

func (t *Team) GetId() int64 { return t.id }

func (t *Team) SetId(id int64) { t.id = id }

// Role is what an account is in a team. It is the data of the pairs of the relation.
type Role struct {
	Name string
}

// ExampleRelation relates accounts to teams, with the role of an account in a team as the data of
// the pair, and deletes an account and a team.
func ExampleRelation() {
	accounts := record.New[int64, Account]("accounts")
	teams := record.New[int64, Team]("teams")
	// The type of the data is the only type argument to write.
	roles := relation.New[Role]("roles", accounts, teams)

	db := memkv.New()
	if err := migrate.Check(db, accounts, teams, roles); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		for _, a := range []*Account{{id: 1, Name: "ann"}, {id: 2, Name: "bob"}, {id: 3, Name: "cy"}} {
			if err := accounts.Instantiate(tx).Set(a); err != nil {
				return err
			}
		}
		for _, t := range []*Team{{id: 10, Name: "red"}, {id: 20, Name: "blue"}} {
			if err := teams.Instantiate(tx).Set(t); err != nil {
				return err
			}
		}

		ins := roles.Instantiate(tx)
		for _, p := range []struct {
			account, team int64
			role          string
		}{
			{1, 10, "lead"}, {1, 20, "member"}, {2, 10, "member"}, {3, 20, "lead"},
		} {
			if err := ins.Set(p.account, p.team, &Role{Name: p.role}); err != nil {
				return err
			}
		}

		// A record has to exist to be related.
		err := ins.Set(4, 10, nil)
		fmt.Println("missing account:", errors.Is(err, relation.ErrMissingRecord))

		// What an account is related to, and what is related to a team.
		it, err := ins.Rights(1)
		if err != nil {
			return err
		}
		for ; it.Valid(); it.Next() {
			team, _ := it.Key()
			role, _ := it.Value()
			fmt.Println("ann is", role.Name, "of team", team)
		}
		it.Close()

		it2, err := ins.Lefts(10)
		if err != nil {
			return err
		}
		for ; it2.Valid(); it2.Next() {
			account, _ := it2.Key()
			role, _ := it2.Value()
			fmt.Println("team 10 has account", account, "as", role.Name)
		}
		it2.Close()

		// Deleting a record deletes its pairs.
		if err := accounts.Instantiate(tx).Delete(1); err != nil {
			return err
		}
		if err := teams.Instantiate(tx).Delete(20); err != nil {
			return err
		}

		all, err := ins.All()
		if err != nil {
			return err
		}
		defer all.Close()
		for ; all.Valid(); all.Next() {
			pair, _ := all.Key()
			role, _ := all.Value()
			fmt.Println("left:", pair.First, pair.Second, role.Name)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// missing account: true
	// ann is lead of team 10
	// ann is member of team 20
	// team 10 has account 1 as lead
	// team 10 has account 2 as member
	// left: 2 10 member
}

// Owner is the parent record of an association.
type Owner struct {
	id   int64
	Name string
}

func (o *Owner) GetId() int64 { return o.id }

func (o *Owner) SetId(id int64) { o.id = id }

// Pet is the child record of an association. It belongs to one owner.
type Pet struct {
	id   int64
	Name string
}

func (p *Pet) GetId() int64 { return p.id }

func (p *Pet) SetId(id int64) { p.id = id }

// ExampleAssociation makes pets belong to owners, and deletes an owner with its pets.
func ExampleAssociation() {
	owners := record.New[int64, Owner]("owners")
	pets := record.New[int64, Pet]("pets")
	ownership := relation.Associate("owned", owners, pets)

	db := memkv.New()
	if err := migrate.Check(db, owners, pets, ownership); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		ins := ownership.Instantiate(tx)

		// An owner is set with its pets.
		if err := ins.Set(&Owner{id: 1, Name: "ann"}, &Pet{id: 1, Name: "rex"}, &Pet{id: 2, Name: "tom"}); err != nil {
			return err
		}
		if err := ins.Set(&Owner{id: 2, Name: "bob"}); err != nil {
			return err
		}

		// A pet is set on its own under its owner, which has to exist.
		store := pets.Instantiate(tx)
		if err := store.Set(&Pet{id: 3, Name: "kit"}, ownership.Under(2)); err != nil {
			return err
		}
		err := store.Set(&Pet{id: 4, Name: "ace"}, ownership.Under(9))
		fmt.Println("no such owner:", errors.Is(err, relation.ErrMissingRecord))
		err = store.Set(&Pet{id: 4, Name: "ace"})
		fmt.Println("no owner:", errors.Is(err, relation.ErrNoParent))

		owner, err := ins.Parent(2)
		if err != nil {
			return err
		}
		fmt.Println("owner of tom:", owner.Name)

		it, err := ins.Children(1)
		if err != nil {
			return err
		}
		for ; it.Valid(); it.Next() {
			pet, _ := it.Value()
			fmt.Println("ann has", pet.Name)
		}
		it.Close()

		// Deleting an owner deletes its pets.
		if err := owners.Instantiate(tx).Delete(1); err != nil {
			return err
		}
		for _, id := range []int64{1, 2, 3} {
			ok, err := store.Has(id)
			if err != nil {
				return err
			}
			fmt.Println("pet", id, "exists:", ok)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// no such owner: true
	// no owner: true
	// owner of tom: ann
	// ann has rex
	// ann has tom
	// pet 1 exists: false
	// pet 2 exists: false
	// pet 3 exists: true
}
