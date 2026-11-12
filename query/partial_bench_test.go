package query_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
)

const (
	// partialBenchRecords is the number of records of every store of the benchmarks.
	partialBenchRecords = 10_000
	// partialBenchLoad is the number of records that are written in a transaction when loading.
	partialBenchLoad = 1_000
	// partialBenchDims is the number of dimensions of the vectors, which is what a small
	// embedding model makes.
	partialBenchDims = 384
	// partialBenchQueries is the number of vectors that the nearest searches take turns with.
	partialBenchQueries = 50
	// partialBenchPayload is the size of the payload of the large books.
	partialBenchPayload = 4096
)

// benchBook is a record with a few small fields, and a payload that a query does not read.
type benchBook struct {
	Title, Author string
	Year, Score   int64
	Tags          []string
	InPrint       bool
	Payload       []byte
}

// benchDoc is a record with a vector.
type benchDoc struct {
	Lang      string
	Year      int64
	Embedding []float32
}

// partialSelectivities are the fractions of the records that the conditions of the benchmarks
// are satisfied by, with the bounds of the range of the values (0 to 9999, each once).
var partialSelectivities = []struct {
	name   string
	lo, hi int64
}{
	{"0.1%", 0, 9},
	{"10%", 0, 999},
	{"25%", 0, 2499},
	{"40%", 0, 3999},
	{"50%", 0, 4999},
	{"75%", 0, 7499},
	{"90%", 0, 8999},
	{"100%", 0, 9999},
}

// partialEnv is a store that is loaded, with the fields that the benchmarks read.
type partialEnv[V any] struct {
	db    kv.DB
	store *extstore.Store[int64, V]
	// num is the field that the conditions are on, whose values are 0 to 9999, each once.
	num field.Field[V, int64]
	// txt is the field that is selected.
	txt field.Field[V, string]
	// nearness, if it is not nil, makes the order of the records by the distance from a vector.
	nearness func(q []float32) query.Nearness[int64, V]
}

// partialLazy loads an environment once, for a codec.
type partialLazy[V any] struct {
	once sync.Once
	env  *partialEnv[V]
}

func (l *partialLazy[V]) get(load func() *partialEnv[V]) *partialEnv[V] {
	l.once.Do(func() { l.env = load() })
	return l.env
}

// partialLoad writes the records into a new memkv with the values of the field of the conditions
// in a random order.
func partialLoad[V any](
	store *extstore.Store[int64, V], newRecord func(rng *rand.Rand, num int64) *V,
) kv.DB {
	db := memkv.New()
	if err := store.Check(db); err != nil {
		panic(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	nums := rng.Perm(partialBenchRecords)
	for start := 0; start < partialBenchRecords; start += partialBenchLoad {
		err := db.Update(func(tx kv.Tx) error {
			s := store.Instantiate(tx)
			for i := start; i < start+partialBenchLoad; i++ {
				if err := s.Set(int64(i+1), newRecord(rng, int64(nums[i]))); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	return db
}

// partialBookEnv loads the books with a payload of the size, with the codec.
func partialBookEnv(payload int, c codec.ValueCodec[benchBook]) *partialEnv[benchBook] {
	store := extstore.NewWithCodecs[int64, benchBook]("books", nil, c)
	db := partialLoad(store, func(rng *rand.Rand, num int64) *benchBook {
		b := &benchBook{
			Title:   fmt.Sprintf("Title %d of the books", rng.IntN(1_000_000)),
			Author:  fmt.Sprintf("Author %d", rng.IntN(500)),
			Year:    1800 + rng.Int64N(220),
			Score:   num,
			Tags:    []string{"tag-a", "tag-b", fmt.Sprintf("tag-%d", rng.IntN(100))},
			InPrint: rng.IntN(2) == 0,
		}
		if payload > 0 {
			b.Payload = make([]byte, payload)
			for i := range b.Payload {
				b.Payload[i] = byte(rng.Uint32())
			}
		}
		return b
	})
	return &partialEnv[benchBook]{
		db:    db,
		store: store,
		num:   field.Path[benchBook, int64]("Score"),
		txt:   field.Path[benchBook, string]("Title"),
	}
}

// partialDocEnv loads the docs with a vector index, with the codec.
func partialDocEnv(c codec.ValueCodec[benchDoc]) *partialEnv[benchDoc] {
	store := extstore.NewWithCodecs[int64, benchDoc]("docs", nil, c)
	byEmbedding := vector.On(store, "by_embedding", partialBenchDims, vector.Cosine,
		func(d *benchDoc) []float32 { return d.Embedding })
	db := partialLoad(store, func(rng *rand.Rand, num int64) *benchDoc {
		return &benchDoc{
			Lang:      []string{"en", "fr", "de", "es"}[rng.IntN(4)],
			Year:      num,
			Embedding: partialVector(rng),
		}
	})
	return &partialEnv[benchDoc]{
		db:    db,
		store: store,
		num:   field.Path[benchDoc, int64]("Year"),
		txt:   field.Path[benchDoc, string]("Lang"),
		nearness: func(q []float32) query.Nearness[int64, benchDoc] {
			return byEmbedding.To(q)
		},
	}
}

func partialVector(rng *rand.Rand) []float32 {
	v := make([]float32, partialBenchDims)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

// partialQueries returns the vectors that the nearest searches take turns with.
func partialQueries() [][]float32 {
	rng := rand.New(rand.NewPCG(3, 4))
	qs := make([][]float32, partialBenchQueries)
	for i := range qs {
		qs[i] = partialVector(rng)
	}
	return qs
}

var (
	partialBooksSmall [2]partialLazy[benchBook]
	partialBooksLarge [2]partialLazy[benchBook]
	partialDocs       [2]partialLazy[benchDoc]
)

// partialCodecs are the codecs that the stores are compared with, in the order of the lazy
// environments: the partial decoding of the default codec, and the same without it.
var partialCodecs = []string{"partial", "full"}

func partialCodec[V any](i int) codec.ValueCodec[V] {
	c := codec.ValueFor[V](nil)
	if partialCodecs[i] == "full" {
		return codec.WithoutPartial(c)
	}
	return c
}

// partialOp is a query that a benchmark runs on an environment, with the range of the values of
// the field of the conditions and the index of the query vector, if it needs one. It returns
// the number of records (or rows) that it consumed, a number made of what it read from them so
// that the reading is not left out, and the keys of the records if keys is not nil.
type partialOp[V any] func(
	e *partialEnv[V], tx kv.Tx, lo, hi int64, n int, keys *[]int64,
) (int, int, error)

func partialRun[V any](use func(*V) int) partialOp[V] {
	return func(e *partialEnv[V], tx kv.Tx, lo, hi int64, _ int, keys *[]int64) (int, int, error) {
		it := query.From(e.store).Where(e.num.Between(lo, hi)).FullScan().Run(tx)
		defer it.Close()
		count, sum := 0, 0
		for ; it.Valid(); it.Next() {
			v, err := it.Value()
			if err != nil {
				return 0, 0, err
			}
			sum += use(v)
			count++
			if keys != nil {
				k, err := it.Key()
				if err != nil {
					return 0, 0, err
				}
				*keys = append(*keys, k)
			}
		}
		return count, sum, it.Err()
	}
}

func partialCount[V any]() partialOp[V] {
	return func(e *partialEnv[V], tx kv.Tx, lo, hi int64, _ int, _ *[]int64) (int, int, error) {
		n, err := query.From(e.store).Where(e.num.Between(lo, hi)).FullScan().Count(tx)
		return n, n, err
	}
}

func partialSelect[V any]() partialOp[V] {
	return func(e *partialEnv[V], tx kv.Tx, lo, hi int64, _ int, keys *[]int64) (int, int, error) {
		it := query.From(e.store).Where(e.num.Between(lo, hi)).FullScan().Select(e.txt).Run(tx)
		defer it.Close()
		count, sum := 0, 0
		for ; it.Valid(); it.Next() {
			row, err := it.Value()
			if err != nil {
				return 0, 0, err
			}
			s, _ := row.Get(e.txt)
			sum += len(s)
			count++
			if keys != nil {
				*keys = append(*keys, row.Key())
			}
		}
		return count, sum, it.Err()
	}
}

func partialNearest[V any](queries [][]float32, use func(*V) int) partialOp[V] {
	return func(e *partialEnv[V], tx kv.Tx, lo, hi int64, n int, keys *[]int64) (int, int, error) {
		q := query.From(e.store).
			Where(e.num.Between(lo, hi)).
			Nearest(e.nearness(queries[n%len(queries)])).
			Limit(10)
		it := q.Run(tx)
		defer it.Close()
		count, sum := 0, 0
		for ; it.Valid(); it.Next() {
			v, err := it.Value()
			if err != nil {
				return 0, 0, err
			}
			sum += use(v)
			count++
			if keys != nil {
				k, err := it.Key()
				if err != nil {
					return 0, 0, err
				}
				*keys = append(*keys, k)
			}
		}
		return count, sum, it.Err()
	}
}

// partialOpSpec is a benchmark of an operation: its name and what it runs, and how many of
// its queries (vectors) the check compares.
type partialOpSpec[V any] struct {
	name    string
	op      partialOp[V]
	queries int
}

// partialBench runs the benchmarks of the operations on a kind of records, for every selectivity
// and codec. Before the time is measured it checks that both codecs give the same results.
func partialBench[V any](
	b *testing.B,
	typ string,
	envs *[2]partialLazy[V],
	load func(codec.ValueCodec[V]) *partialEnv[V],
	ops []partialOpSpec[V],
) {
	// The loading is not measured: it is done when the first benchmark of the type needs it.
	env := func(i int) *partialEnv[V] {
		return envs[i].get(func() *partialEnv[V] { return load(partialCodec[V](i)) })
	}
	for _, spec := range ops {
		for _, sel := range partialSelectivities {
			b.Run(fmt.Sprintf("type=%s/op=%s/sel=%s", typ, spec.name, sel.name), func(b *testing.B) {
				partialCheck(b, env(0), env(1), spec, sel.lo, sel.hi)

				for i := range partialCodecs {
					b.Run("codec="+partialCodecs[i], func(b *testing.B) {
						e := env(i)
						var records int
						b.ReportAllocs()
						b.ResetTimer()
						for n := range b.N {
							err := e.db.View(func(tx kv.Tx) error {
								count, sum, err := spec.op(e, tx, sel.lo, sel.hi, n, nil)
								records += count
								partialSink += sum
								return err
							})
							if err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(records)/float64(b.N), "records/op")
					})
				}
			})
		}
	}
}

// partialSink keeps the benchmarks from leaving out what they read.
var partialSink int

// partialCheck fails the benchmark if the stores of the two codecs give other results for the
// operation: other numbers of records, other keys (in order), or other values.
func partialCheck[V any](b *testing.B, partial, full *partialEnv[V], spec partialOpSpec[V], lo, hi int64) {
	b.Helper()

	for n := range max(spec.queries, 1) {
		var results [2]struct {
			count, sum int
			keys       []int64
		}
		for i, e := range []*partialEnv[V]{partial, full} {
			err := e.db.View(func(tx kv.Tx) error {
				var err error
				r := &results[i]
				r.count, r.sum, err = spec.op(e, tx, lo, hi, n, &r.keys)
				return err
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		if results[0].count != results[1].count || results[0].sum != results[1].sum ||
			!slices.Equal(results[0].keys, results[1].keys) {
			b.Fatalf("partial and full decoding differ for %s on [%d, %d] (query %d): %d records and %d, sums %d and %d",
				spec.name, lo, hi, n, results[0].count, results[1].count, results[0].sum, results[1].sum)
		}
		if spec.queries == 0 && int64(results[0].count) != hi-lo+1 {
			b.Fatalf("%s on [%d, %d] gave %d records, not %d", spec.name, lo, hi, results[0].count, hi-lo+1)
		}
		if spec.queries > 0 && results[0].count != 10 {
			b.Fatalf("%s on [%d, %d] gave %d records, not 10", spec.name, lo, hi, results[0].count)
		}
	}
}

// BenchmarkPartialQuery measures the same queries on the same records, stored with the default
// value codec, which a query reads in part (codec=partial), and with the same codec without
// partial decoding (codec=full), so that a query decodes every record that it reads in full. The
// conditions are on a field that is not indexed, so a scan of the store checks every record. The
// selectivity is the fraction of the records that satisfy the condition.
//
// The nearest search is not served by an index of the condition: its plan is
//
//	Limit 10
//	└─ Nearest by_embedding (exact), then check [Year >= 0 and Year <= 999]
//
// so the search checks the condition on the records in the order of their distance, until it has
// 10 that satisfy it.
func BenchmarkPartialQuery(b *testing.B) {
	bookOps := func() []partialOpSpec[benchBook] {
		title := func(v *benchBook) int { return len(v.Title) }
		return []partialOpSpec[benchBook]{
			{name: "run", op: partialRun(title)},
			{name: "count", op: partialCount[benchBook]()},
			{name: "select", op: partialSelect[benchBook]()},
		}
	}
	partialBench(b, "book-small", &partialBooksSmall,
		func(c codec.ValueCodec[benchBook]) *partialEnv[benchBook] { return partialBookEnv(0, c) }, bookOps())
	partialBench(b, "book-large", &partialBooksLarge,
		func(c codec.ValueCodec[benchBook]) *partialEnv[benchBook] {
			return partialBookEnv(partialBenchPayload, c)
		}, bookOps())

	queries := partialQueries()
	lang := func(v *benchDoc) int { return len(v.Lang) + len(v.Embedding) }
	partialBench(b, "doc", &partialDocs, partialDocEnv, []partialOpSpec[benchDoc]{
		{name: "run", op: partialRun(lang)},
		{name: "count", op: partialCount[benchDoc]()},
		{name: "select", op: partialSelect[benchDoc]()},
		{name: "nearest", op: partialNearest(queries, lang), queries: partialBenchQueries},
	})
}
