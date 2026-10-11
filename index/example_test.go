package index_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iters"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/schema"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Book struct {
	Title   string
	Author  string
	Year    int
	Tags    []string
	ISBN    string
	InPrint bool
}

// The fields of a book that indexes are defined on and that conditions are written with.
var (
	// By the path of a field of the struct, which is read by reflection.
	Author  = schema.Path[Book, string]("Author")
	Year    = schema.Path[Book, int]("Year")
	ISBN    = schema.Path[Book, string]("ISBN")
	InPrint = schema.Path[Book, bool]("InPrint")
	// A slice is each of its elements: a book is found by each of its tags.
	Tags = schema.Path[Book, string]("Tags")
	// By a function, for a value that is not a field of the struct as it is.
	Decade = schema.FieldOf("Decade", func(b *Book) int { return b.Year / 10 * 10 })
)

// Example defines a store with one index of every kind and looks books up with each of them.
func Example() {
	sch := catalog.NewSchema()
	books := extstore.New[int64, Book](sch.Namespace("books"))

	// Any number of books can have the same author.
	byAuthor := index.Multi(books, "by_author", Author)
	// A book is found by each of its tags, and a book without tags is not in the index.
	byTag := index.Multi(books, "by_tag", Tags)
	// One book at most can have an ISBN.
	byISBN := index.Unique(books, "by_isbn", ISBN)
	// The title is stored with the entries, so it can be listed without reading the books.
	byYear := index.Multi(books, "by_year", Year,
		index.Covering(func(b *Book) string { return b.Title }, nil, "Title"))
	// Several fields in one key: by author, and from the latest year for the same author.
	byAuthorYear := index.Composite(books, "by_author_year", schema.Fields(Author, Year.Desc()))
	// Only the books that are in print are in the index.
	inPrintByDecade := index.Multi(books, "in_print_by_decade", Decade, index.Where(InPrint.Eq(true)))

	db := memkv.New()
	if err := sch.Check(db); err != nil {
		panic(err)
	}

	err := db.Update(func(tx kv.Tx) error {
		store := books.Instantiate(tx)
		for id, book := range map[int64]Book{
			1: {Title: "Dune", Author: "Herbert", Year: 1965, Tags: []string{"scifi", "classic"}, ISBN: "111", InPrint: true},
			2: {Title: "Emma", Author: "Austen", Year: 1815, Tags: []string{"classic"}, ISBN: "222", InPrint: true},
			3: {Title: "Persuasion", Author: "Austen", Year: 1817, ISBN: "333"},
			4: {Title: "Children of Dune", Author: "Herbert", Year: 1976, Tags: []string{"scifi"}, ISBN: "444"},
			5: {Title: "The Dosadi Experiment", Author: "Herbert", Year: 1977, ISBN: "555", InPrint: true},
		} {
			if err := store.Set(id, &book); err != nil {
				return err
			}
		}

		// The indexes are kept up to date in the same transaction as the books.
		err := store.Set(6, &Book{Title: "A copy", ISBN: "111"})
		fmt.Println("same ISBN:", errors.Is(err, index.ErrUniqueViolation))
		return nil
	})
	if err != nil {
		panic(err)
	}

	titles := func(it *index.Iterator[int64, Book], err error) []string {
		if err != nil {
			panic(err)
		}
		titles, err := iters.From(it).
			Map(func(_ int64, b *Book) (string, error) { return b.Title, nil }).
			Values()
		if err != nil {
			panic(err)
		}
		return titles
	}

	err = db.View(func(tx kv.Tx) error {
		// The books of an author.
		fmt.Println("by Austen:", titles(byAuthor.Instantiate(tx).Match("Austen")))

		// The books with a tag.
		fmt.Println("scifi:", titles(byTag.Instantiate(tx).Match("scifi")))

		// The one book with an ISBN, by its key.
		id, found, err := byISBN.Instantiate(tx).Find("333")
		if err != nil {
			return err
		}
		fmt.Println("ISBN 333:", id, found)

		// A range, from the latest year, with the titles read from the index alone.
		it, err := byYear.Instantiate(tx).ReverseRange(kvutil.From(1800).Until(1970))
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

		// Conditions on the fields of a composite index.
		fmt.Println("Herbert since 1970, latest first:",
			titles(byAuthorYear.Instantiate(tx).Lookup(Author.Eq("Herbert"), Year.Gte(1970))))

		// Conditions that the index cannot serve are checked on the books that it finds, so
		// what comes back is exactly what was asked for.
		fmt.Println("Herbert, in print, not scifi:",
			titles(byAuthorYear.Instantiate(tx).Lookup(
				Author.Eq("Herbert"), InPrint.Eq(true), schema.Not(Tags.Eq("scifi")))))

		// How an index would serve conditions, without running them.
		ranges, residual, err := byAuthorYear.Instantiate(tx).Plan(Author.In("Austen", "Herbert"), Tags.Eq("classic"))
		if err != nil {
			return err
		}
		fmt.Println("plan:", len(ranges), "ranges, then check", residual)

		// The index of the books in print does not have the others.
		fmt.Println("in print, 1970s:", titles(inPrintByDecade.Instantiate(tx).Match(1970)))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// same ISBN: true
	// by Austen: [Emma Persuasion]
	// scifi: [Dune Children of Dune]
	// ISBN 333: 3 true
	// before 1970: Dune
	// before 1970: Persuasion
	// before 1970: Emma
	// Herbert since 1970, latest first: [The Dosadi Experiment Children of Dune]
	// Herbert, in print, not scifi: [The Dosadi Experiment]
	// plan: 2 ranges, then check [Tags = "classic"]
	// in print, 1970s: [The Dosadi Experiment]
}
