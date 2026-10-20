package index_test

import (
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
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
	Author  = field.Path[Book, string]("Author")
	Year    = field.Path[Book, int]("Year")
	ISBN    = field.Path[Book, string]("ISBN")
	InPrint = field.Path[Book, bool]("InPrint")
	// A slice is each of its elements: a book is found by each of its tags.
	Tags = field.Path[Book, string]("Tags")
	// By a function, for a value that is not a field of the struct as it is.
	Decade = field.Of("Decade", func(b *Book) int { return b.Year / 10 * 10 })
)

// Example defines a store with one index of every kind and looks books up with each of them.
func Example() {
	books := extstore.New[int64, Book]("books")

	// Any number of books can have the same author.
	byAuthor := index.On(books, "by_author", Author)
	// A book is found by each of its tags, and a book without tags is not in the index.
	byTag := index.On(books, "by_tag", Tags)
	// One book at most can have an ISBN.
	byISBN := index.Unique(books, "by_isbn", ISBN)
	// The title is stored with the entries, so it can be listed without reading the books.
	byYear := index.On(books, "by_year", Year,
		index.Covering(func(b *Book) string { return b.Title }, nil, "Title"))
	// Several fields in one key: by author, and from the latest year for the same author.
	byAuthorYear := index.Composite(books, "by_author_year", field.List(Author, Year.Desc()))
	// Only the books that are in print are in the index.
	inPrintByDecade := index.On(books, "in_print_by_decade", Decade, index.Where(InPrint.Eq(true)))

	db := memkv.New()
	if err := books.Check(db); err != nil {
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

	titles := func(it index.Keyed[int64, Book], err error) []string {
		if err != nil {
			panic(err)
		}
		titles, err := iterutil.Of(it).
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
				Author.Eq("Herbert"), InPrint.Eq(true), cond.Not(Tags.Eq("scifi")))))

		// How an index would serve conditions, without running them.
		ranges, residual, err := byAuthorYear.Instantiate(tx).Plan(Author.In("Austen", "Herbert"), Tags.Eq("classic"))
		if err != nil {
			return err
		}
		fmt.Println("plan:", len(ranges), "ranges, then check", residual)

		// The index of the books in print does not have the others.
		fmt.Println("in print, 1970s:", titles(inPrintByDecade.Instantiate(tx).Match(1970)))

		// What several indexes find can be combined. Only the primary keys are read from the
		// indexes, and only the records of the result are read from the store.
		herbert := func() index.Keyed[int64, Book] { return must(byAuthor.Instantiate(tx).Match("Herbert")) }
		scifi := func() index.Keyed[int64, Book] { return must(byTag.Instantiate(tx).Match("scifi")) }
		classic := func() index.Keyed[int64, Book] { return must(byTag.Instantiate(tx).Match("classic")) }
		fmt.Println("Herbert and scifi:", titles(index.Intersect(herbert(), scifi())))
		fmt.Println("scifi or classic:", titles(index.Union(scifi(), classic())))
		fmt.Println("Herbert, not scifi:", titles(index.Except(herbert(), scifi())))

		// A combination is a source of another one: Herbert and (scifi or classic).
		tagged, err := index.Union(scifi(), classic())
		if err != nil {
			return err
		}
		fmt.Println("Herbert, scifi or classic:", titles(index.Intersect(herbert(), tagged)))

		// The primary keys of a lookup as a bitmap, to combine as bitmaps are.
		byHerbert, exact, err := index.Bitmap(herbert())
		if err != nil {
			return err
		}
		recent, _, err := index.Bitmap(must(byYear.Instantiate(tx).Lookup(Year.Gte(1970))))
		if err != nil {
			return err
		}
		byHerbert.And(recent)
		fmt.Println("bitmap:", byHerbert.ToArray(), "exact:", exact, titles(index.FromBitmap(books, tx, byHerbert)))
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
	// Herbert and scifi: [Dune Children of Dune]
	// scifi or classic: [Dune Children of Dune Emma]
	// Herbert, not scifi: [The Dosadi Experiment]
	// Herbert, scifi or classic: [Dune Children of Dune]
	// bitmap: [9223372036854775812 9223372036854775813] exact: true [Children of Dune The Dosadi Experiment]
}
