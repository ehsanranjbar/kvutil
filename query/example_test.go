package query_test

import (
	"fmt"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Book struct {
	Title  string
	Author string
	Year   int
}

// Example builds queries on a store, and shows how they are run.
func Example() {
	books := extstore.New[int64, Book]("books")

	author := field.Path[Book, string]("Author")
	year := field.Path[Book, int]("Year")
	title := field.Path[Book, string]("Title")
	index.Composite(books, "by_author_year", field.List(author, year.Desc()))

	db := memkv.New()
	if err := books.Check(db); err != nil {
		panic(err)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := books.Instantiate(tx)
		for id, book := range []Book{
			{Title: "Dune", Author: "Herbert", Year: 1965},
			{Title: "Emma", Author: "Austen", Year: 1815},
			{Title: "Children of Dune", Author: "Herbert", Year: 1976},
			{Title: "Persuasion", Author: "Austen", Year: 1817},
			{Title: "The Dosadi Experiment", Author: "Herbert", Year: 1977},
		} {
			if err := store.Set(int64(id+1), &book); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	run := func(name string, q *query.Query[int64, Book]) {
		plan, err := q.Explain()
		if err != nil {
			panic(err)
		}
		err = db.View(func(tx kv.Tx) error {
			titles, err := iterutil.Of(q.Run(tx).Iterator).
				Map(func(_ int64, b *Book) (string, error) { return b.Title, nil }).
				Values()
			fmt.Printf("%s\n%s\n%v\n\n", name, plan, titles)
			return err
		})
		if err != nil {
			panic(err)
		}
	}

	// A query is a definition: each method returns another one, and the base stays as it is.
	herbert := query.From(books).Where(author.Eq("Herbert"))

	// The index gives the order, so the records are not sorted.
	run("the latest of Herbert", herbert.OrderBy(year.Desc()).Limit(2))
	// Another order is sorted, and only the first two are kept.
	run("the first two titles of Herbert", herbert.OrderBy(title).Limit(2))
	// With no order the records come in the order of the plan, here that of the index.
	run("Herbert", herbert)
	// Nothing serves a condition on the year alone, so the store is scanned.
	run("books before 1900, page 2", query.From(books).Where(year.Lt(1900)).OrderBy(year).Offset(1).Limit(5))
	// A hint says where to read; this is the same query as the first one, from the store.
	run("the latest of Herbert, by a scan", herbert.OrderBy(year.Desc()).Limit(2).FullScan())
	// Conditions that contradict each other read nothing.
	run("nothing", herbert.Where(author.Eq("Austen")))

	// Output:
	// the latest of Herbert
	// Limit 2
	// └─ IndexLookup by_author_year (in order of Year desc): 1 range
	// [The Dosadi Experiment Children of Dune]
	//
	// the first two titles of Herbert
	// Limit 2
	// └─ TopN by Title
	//    └─ IndexLookup by_author_year: 1 range
	// [Children of Dune Dune]
	//
	// Herbert
	// IndexLookup by_author_year: 1 range
	// [The Dosadi Experiment Children of Dune Dune]
	//
	// books before 1900, page 2
	// Limit 5, offset 1
	// └─ TopN by Year
	//    └─ StoreScan, then check [Year < 1900]
	// [Persuasion]
	//
	// the latest of Herbert, by a scan
	// Limit 2
	// └─ TopN by Year desc
	//    └─ StoreScan, then check [Author = "Herbert"]
	// [The Dosadi Experiment Children of Dune]
	//
	// nothing
	// Empty
	// []
}
