package index_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/index/concat"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Book struct {
	Title  string
	Author string
	Year   int
	Tags   []string
	ISBN   string
}

// Example defines a store with one index of every kind and looks books up with each of them.
func Example() {
	schema := catalog.NewSchema()
	books := extstore.New[int64, Book](schema.Namespace("books"))

	// Any number of books can have the same author.
	byAuthor := index.Multi(books, "by_author",
		index.Field("Author", func(b *Book) string { return b.Author }))

	// A book is found by each of its tags, and a book without tags is not in the index.
	byTag := index.Multi(books, "by_tag",
		index.Each("Tags", func(b *Book) []string { return b.Tags }))

	// One book at most can have an ISBN.
	byISBN := index.Unique(books, "by_isbn",
		index.Field("ISBN", func(b *Book) string { return b.ISBN }))

	// The title is stored with the entries, so it can be listed without reading the books.
	byYear := index.Multi(books, "by_year",
		index.Field("Year", func(b *Book) int { return b.Year }),
		index.Covering(func(b *Book) string { return b.Title }, nil, "Title"))

	// Several fields in one key: by author, and by year from the latest for the same author.
	indexer, err := concat.For[Book](concat.NewComponent("Author"), concat.NewComponent("Year").Desc())
	if err != nil {
		panic(err)
	}
	byAuthorYear := index.New(books, "by_author_year", indexer)

	db := memkv.New()
	if err := schema.Check(db); err != nil {
		panic(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		store := books.Instantiate(tx)
		for id, book := range map[int64]Book{
			1: {Title: "Dune", Author: "Herbert", Year: 1965, Tags: []string{"scifi", "classic"}, ISBN: "111"},
			2: {Title: "Emma", Author: "Austen", Year: 1815, Tags: []string{"classic"}, ISBN: "222"},
			3: {Title: "Persuasion", Author: "Austen", Year: 1817, ISBN: "333"},
			4: {Title: "Children of Dune", Author: "Herbert", Year: 1976, Tags: []string{"scifi"}, ISBN: "444"},
		} {
			if err := store.Set(id, &book); err != nil {
				return err
			}
		}

		// The indexes are kept up to date in the same transaction as the books.
		err := store.Set(5, &Book{Title: "A copy", ISBN: "111"})
		fmt.Println("same ISBN:", errors.Is(err, index.ErrUniqueViolation))
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		// Match: the books of an author, as an iterator of their keys and of the books themselves.
		it, err := byAuthor.Instantiate(tx).Match("Austen")
		if err != nil {
			return err
		}
		titles, err := iters.From(it).
			Map(func(_ int64, b *Book) (string, error) { return b.Title, nil }).
			Values()
		if err != nil {
			return err
		}
		fmt.Println("by Austen:", titles)

		// Each: the keys of the books with a tag.
		it, err = byTag.Instantiate(tx).Match("scifi")
		if err != nil {
			return err
		}
		ids, err := iters.From(it).Keys()
		if err != nil {
			return err
		}
		fmt.Println("scifi:", ids)

		// Unique: the one book with an ISBN.
		id, found, err := byISBN.Instantiate(tx).Find("333")
		if err != nil {
			return err
		}
		fmt.Println("ISBN 333:", id, found)

		// Range, from the latest year, with the titles read from the index alone.
		it, err = byYear.Instantiate(tx).ReverseRange(
			expr.NewRange(expr.NewBound(1800, false), expr.NewBound(1970, true)))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			covered, err := it.Covered()
			if err != nil {
				return err
			}
			title, err := codec.ValueFor[string](nil).Decode(covered)
			if err != nil {
				return err
			}
			fmt.Println("before 1970:", title)
		}

		// Conditions on the fields of a composite index. A condition that the index cannot use
		// comes back as the residual, to be checked on the books.
		it, residual, err := byAuthorYear.Instantiate(tx).Lookup(
			expr.NewAssigned("Author", expr.NewExact[any]("Herbert")),
			expr.NewAssigned("Year", expr.NewRange[any](expr.NewBound[any](1960, false), nil)),
		)
		if err != nil {
			return err
		}
		ids, err = iters.From(it).Keys()
		if err != nil {
			return err
		}
		fmt.Println("Herbert since 1960, latest first:", ids, "residual:", len(residual))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// same ISBN: true
	// by Austen: [Emma Persuasion]
	// scifi: [1 4]
	// ISBN 333: 3 true
	// before 1970: Dune
	// before 1970: Persuasion
	// before 1970: Emma
	// Herbert since 1960, latest first: [4 1] residual: 0
}
