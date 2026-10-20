// Package bookdb is the database of books that the query workloads run on, in both systems: the
// record store of kvutil with its indexes, and the table of PostgreSQL with the same ones. It
// loads the books, builds the indexes over them, and says how to read a book back from a row.
package bookdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	sqlq "github.com/ehsanranjbar/kvutil/query/sql"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// Book is the record.
type Book = dataset.Book

// Store is the record store of the books.
type Store = record.Store[int64, Book]

// The fields of a book, as the indexes and the queries of kvutil name them.
var (
	FCode       = field.Path[Book, string]("Code")
	FAuthor     = field.Path[Book, string]("Author")
	FYear       = field.Path[Book, int64]("Year")
	FPrice      = field.Path[Book, int64]("Price")
	FInPrint    = field.Path[Book, bool]("InPrint")
	FTags       = field.Path[Book, string]("Tags")
	FTitle      = field.Path[Book, string]("Title")
	FScore      = field.Path[Book, int64]("Score")
	FID         = field.Path[Book, int64]("ID")
	FTitleLower = sqlq.Expr[Book, string](`lower(Title)`)
)

// The names of the indexes of kvutil, and the indexes of PostgreSQL that correspond to them are in
// IndexDDL.
const (
	IdxCode        = "by_code"
	IdxAuthor      = "by_author"
	IdxAuthorYear  = "by_author_year"
	IdxYear        = "by_year"
	IdxTags        = "by_tags"
	IdxTitleLower  = "by_title_lower"
	IdxInPrint     = "in_print_by_price"
	IdxAuthorCover = "by_author_cover"
	IdxScore       = "by_score"
)

// NewStore returns the record store of the books, without indexes.
func NewStore() *Store { return record.New[int64, Book]("books") }

// AddQueryIndexes defines the indexes of the queries workload on the store and returns their
// names:
//
//	by_code            unique on Code
//	by_author          on Author
//	by_author_year     composite of Author and Year, descending
//	by_year            on Year
//	by_tags            on Tags, of which a book has up to four: an entry per tag
//	by_title_lower     on the expression lower(Title)
//	in_print_by_price  on Price, of the books that are in print only
//	by_author_cover    on Author, with Title and Price held in every entry
func AddQueryIndexes(st *Store) []string {
	index.Unique(st, IdxCode, FCode)
	index.On(st, IdxAuthor, FAuthor)
	index.Composite(st, IdxAuthorYear, field.List[Book](FAuthor, FYear.Desc()))
	index.On(st, IdxYear, FYear)
	index.On(st, IdxTags, FTags)
	index.On(st, IdxTitleLower, FTitleLower)
	index.On(st, IdxInPrint, FPrice, sqlq.Where[Book](`InPrint`))
	index.On(st, IdxAuthorCover, FAuthor, index.Including[Book](FTitle, FPrice))
	return []string{IdxCode, IdxAuthor, IdxAuthorYear, IdxYear, IdxTags, IdxTitleLower, IdxInPrint, IdxAuthorCover}
}

// AddScoreIndex defines the one index of the selectivity sweep: on Score.
func AddScoreIndex(st *Store) []string {
	index.On(st, IdxScore, FScore)
	return []string{IdxScore}
}

// KV is a database of Badger with the books in it.
type KV struct {
	DB *kvsys.DB
	// Base loads the records, and has no index. Store is the store that reads and writes them,
	// with the indexes that Build defined.
	Base, Store *Store
}

// OpenKV creates an empty database for a workload.
func OpenKV(ctx context.Context, sys *kvsys.System, name string) (*KV, error) {
	db, err := sys.Open(name)
	if err != nil {
		return nil, err
	}
	base := NewStore()
	if _, err := base.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return &KV{DB: db, Base: base, Store: base}, nil
}

// Load writes the books [1, n] with a bulk writer, without any index.
func (k *KV) Load(spec dataset.Spec, n int64) error {
	_, err := wl.BulkLoad(k.DB, 1, n+1, 1, func() func(kv.BulkWriter, int64) error {
		b := &Book{}
		prefix := bytes.Clone(k.Base.DataPrefix())
		return func(w kv.BulkWriter, id int64) error {
			spec.GenBookInto(b, id)
			key, err := k.Base.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			value, err := k.Base.ValueCodec().Encode(*b)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(prefix), key...), value)
		}
	})
	return err
}

// Build defines the indexes on a store with the same prefix as the one that loaded the books, and
// migrates the database to it, which builds them over the books that are there.
func (k *KV) Build(ctx context.Context, define func(*Store) []string) ([]string, error) {
	st := NewStore()
	names := define(st)
	report, err := st.Migrate(ctx, k.DB)
	if err != nil {
		return nil, err
	}
	built := 0
	for _, a := range report.Actions {
		if a.What == migrate.Built {
			built++
		}
	}
	if built != len(names) {
		return nil, fmt.Errorf("the migration built %d of %d indexes: %s", built, len(names), report)
	}
	k.Store = st
	return names, nil
}

// Settle makes what has been written durable, so that the size on disk is that of the data.
func (k *KV) Settle() error { return k.DB.Sync() }
