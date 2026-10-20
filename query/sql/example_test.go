package sql_test

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

type Novel struct {
	Title   string
	Author  string
	Year    int
	Tags    []string
	InPrint bool
}

// Example parses the WHERE clauses of queries on a store, and runs them with an index and
// without one.
func Example() {
	novels := extstore.New[int64, Novel]("novels")
	index.Composite(novels, "by_author_year", field.List(
		field.Path[Novel, string]("Author"),
		field.Path[Novel, int]("Year"),
	))
	// A field that is computed is known to the parser by the index that is defined on it.
	index.On(novels, "by_decade", field.Of("Decade", func(n *Novel) int { return n.Year / 10 * 10 }))

	db := memkv.New()
	if err := novels.Check(db); err != nil {
		panic(err)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := novels.Instantiate(tx)
		for id, novel := range []Novel{
			{Title: "Dune", Author: "Herbert", Year: 1965, Tags: []string{"scifi", "classic"}, InPrint: true},
			{Title: "Emma", Author: "Austen", Year: 1815, Tags: []string{"classic"}, InPrint: true},
			{Title: "Children of Dune", Author: "Herbert", Year: 1976, Tags: []string{"scifi"}},
			{Title: "The Dosadi Experiment", Author: "Herbert", Year: 1977, InPrint: true},
		} {
			if err := store.Set(int64(id+1), &novel); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// The fields are the ones of the indexes of the store and the ones of the struct.
	parser := sql.For(novels)

	err = db.View(func(tx kv.Tx) error {
		query := func(where string) {
			explain, err := parser.Explain(where)
			if err != nil {
				panic(err)
			}
			it, err := parser.Query(tx, where)
			if err != nil {
				panic(err)
			}
			titles, err := iterutil.Of(it).
				Map(func(_ int64, n *Novel) (string, error) { return n.Title, nil }).
				Values()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s\n  %s\n  %v\n", where, strings.ReplaceAll(explain, "\n", "\n  "), titles)
		}

		// The comparisons of the fields of an index are served by it, and the rest of the
		// text is checked on the novels that it finds.
		query(`Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%dune'`)
		// Nothing serves a condition that is about a part of a field alone, so the store is scanned.
		query(`Decade = 1970 OR ('classic' IN Tags AND NOT Year >= 1900)`)
		query(`length(Title) > 10 AND InPrint`)

		// The condition itself is a filter of anything that has records.
		c, err := parser.Parse(`Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%dune'`)
		if err != nil {
			return err
		}
		fmt.Println("condition:", c)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// What cannot be right for any novel is an error of the text. Double quotes make a name.
	for _, where := range []string{
		`Athor = 'Herbert'`,
		`Author = "Herbert"`,
		`Year = 'soon'`,
		`contains(Title, 'Dune')`,
		`Year = 1965 AND`,
		`Year + 1`,
	} {
		_, err := parser.Parse(where)
		fmt.Printf("%s: %v %v\n", where,
			errors.Is(err, sql.ErrUnknownField) || errors.Is(err, sql.ErrType) ||
				errors.Is(err, sql.ErrUnknownFunction) || errors.Is(err, sql.ErrSyntax), err == nil)
	}
	_, err = parser.Parse(`Year = 'soon'`)
	fmt.Println(err)

	// Output:
	// Author = 'Herbert' AND Year >= 1970 AND Title LIKE '%dune'
	//   IndexLookup by_author_year: 1 range, then check ["Title" LIKE '%dune']
	//   [Children of Dune]
	// Decade = 1970 OR ('classic' IN Tags AND NOT Year >= 1900)
	//   StoreScan, then check [(Decade = 1970) or ((Tags = "classic") and (not (Year >= 1900)))]
	//   [Emma Children of Dune The Dosadi Experiment]
	// length(Title) > 10 AND InPrint
	//   StoreScan, then check [length("Title") > 10; InPrint = true]
	//   [The Dosadi Experiment]
	// condition: (Author = "Herbert") and (Year >= 1970) and ("Title" LIKE '%dune')
	// Athor = 'Herbert': true false
	// Author = "Herbert": true false
	// Year = 'soon': true false
	// contains(Title, 'Dune'): true false
	// Year = 1965 AND: true false
	// Year + 1: true false
	// sql: type mismatch: field Year: cannot use "soon" as a value of the type int: it is not a whole number that the type has
}

// Example_functions gives a parser functions of its own, which the texts of its queries call as
// they call the ones that there always are.
func Example_functions() {
	novels := extstore.New[int64, Novel]("novels")

	// A function is made from a Go function: its types are the ones of the Go function. It
	// must be deterministic. Its version says what it computes, and is a part of what an index
	// that calls it is identified by.
	functions := sql.Functions{}.
		Add(sql.Func1("initial", func(s string) string { return strings.ToUpper(s[:1]) }).WithVersion("1")).
		Add(sql.Func2("age", func(year, now int) int { return now - year })).
		// FuncN is for the types that Go has no type for, and for functions that fail.
		Add(sql.FuncN("century", []sql.Type{sql.Number}, false, sql.Integer, func(args []any) (any, error) {
			year, ok := args[0].(int64)
			if !ok {
				return nil, fmt.Errorf("%v is not a year", args[0])
			}
			return (year-1)/100 + 1, nil
		}))
	parser := sql.For(novels, sql.WithFunctions(functions))

	db := memkv.New()
	if err := novels.Check(db); err != nil {
		panic(err)
	}
	err := db.Update(func(tx kv.Tx) error {
		store := novels.Instantiate(tx)
		for id, novel := range []Novel{
			{Title: "Dune", Author: "Herbert", Year: 1965},
			{Title: "Emma", Author: "Austen", Year: 1815},
			{Title: "Persuasion", Author: "Austen", Year: 1817},
		} {
			if err := store.Set(int64(id+1), &novel); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	err = db.View(func(tx kv.Tx) error {
		for _, where := range []string{
			`initial(Author) = 'A' AND age(Year, 2000) > 180`,
			`INITIAL(Title) IN ('D', 'E') AND century(Year) = 20`,
			`century(Year) = 19 OR century(Year) = 21`,
		} {
			it, err := parser.Query(tx, where)
			if err != nil {
				return err
			}
			titles, err := iterutil.Of(it).
				Map(func(_ int64, n *Novel) (string, error) { return n.Title, nil }).
				Values()
			if err != nil {
				return err
			}
			fmt.Printf("%s\n  %v\n", where, titles)
		}

		// A function is checked when the text is parsed, as the ones that always are.
		for _, where := range []string{`initial(Year) = 'A'`, `age(Year) > 1`, `initial(Author)`, `shout(Author) = 'A'`} {
			_, err := parser.Parse(where)
			fmt.Printf("%s: %v\n", where, err)
		}

		// A function that fails fails the query, with its error.
		it, err := parser.Query(tx, `century(Year) = 1 OR century(NULL) = 1`)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
		}
		fmt.Println(it.Err())
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// initial(Author) = 'A' AND age(Year, 2000) > 180
	//   [Emma Persuasion]
	// INITIAL(Title) IN ('D', 'E') AND century(Year) = 20
	//   [Dune]
	// century(Year) = 19 OR century(Year) = 21
	//   [Emma Persuasion]
	// initial(Year) = 'A': sql: type mismatch: argument 1 of initial is a number, which it does not take: initial("Year")
	// age(Year) > 1: sql: type mismatch: age takes 2 arguments, not 1
	// initial(Author): sql: type mismatch: initial("Author") is a string, not true or false
	// shout(Author) = 'A': sql: unknown function "shout"
	// failed to check (century("Year") = 1) or (century(NULL) = 1): sql: evaluation failed: function century: <nil> is not a year
}

// Example_indexes defines indexes on an SQL expression and on an SQL condition. A query reads
// them when it compares the same expression with values, and when its conditions imply the
// condition of the index.
func Example_indexes() {
	novels := extstore.New[int64, Novel]("novels")
	year := field.Path[Novel, int]("Year")

	// The path of the field is the expression as the parser prints it.
	lowerAuthor := sql.Expr[Novel, string](`LOWER(Author)`)
	fmt.Println("field:", lowerAuthor.Path())
	index.On(novels, "by_author", lowerAuthor)

	// Only the novels that are in print and are not too old are in this index.
	recent := index.On(novels, "recent_by_year", year, sql.Where[Novel](`InPrint AND Year >= 1900`))
	fmt.Println("where:", recent.Descriptor().Where)

	parser := sql.For(novels)
	for _, where := range []string{
		`lower(Author) = 'herbert'`,
		`'austen' = LOWER( "Author" )`,
		`lower(Author) != 'herbert'`,
		`upper(Author) = 'HERBERT'`,
		`InPrint AND Year >= 1970`,
		`InPrint AND Year > 1800`,
		`Year >= 1970`,
		`InPrint OR Year >= 1970`,
	} {
		explain, err := parser.Explain(where)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s\n  %s\n", where, strings.ReplaceAll(explain, "\n", "\n  "))
	}

	// An index of some of the novels cannot be asked for by a query that is about others.
	q, err := parser.Where(`Year >= 1970`)
	if err != nil {
		panic(err)
	}
	_, err = q.Using("recent_by_year").Explain()
	fmt.Println(err)

	// Output:
	// field: lower("Author")
	// where: (InPrint = true) and (Year >= 1900)
	// lower(Author) = 'herbert'
	//   IndexLookup by_author: 1 range
	// 'austen' = LOWER( "Author" )
	//   IndexLookup by_author: 1 range
	// lower(Author) != 'herbert'
	//   StoreScan, then check [not (lower("Author") = "herbert")]
	// upper(Author) = 'HERBERT'
	//   StoreScan, then check [upper("Author") = 'HERBERT']
	// InPrint AND Year >= 1970
	//   IndexLookup recent_by_year: 1 range, then check [InPrint = true]
	// InPrint AND Year > 1800
	//   StoreScan, then check [InPrint = true; Year > 1800]
	// Year >= 1970
	//   StoreScan, then check [Year >= 1970]
	// InPrint OR Year >= 1970
	//   StoreScan, then check [(InPrint = true) or (Year >= 1970)]
	// query: the hint cannot be followed: the index "recent_by_year" cannot serve the query: it has the records that satisfy (InPrint = true) and (Year >= 1900) only, which the conditions do not imply
}
