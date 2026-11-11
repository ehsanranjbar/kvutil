package vector_test

import (
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// benchDims is the number of dimensions of the vectors of the benchmarks, which is what a small
// embedding model makes.
const benchDims = 384

// benchLoad is the number of docs that are written in a transaction when the benchmarks load.
const benchLoad = 500

// BenchmarkNearest measures finding the 10 nearest vectors of a query, which compares the
// query with every vector that is stored. The search of 100000 vectors is only run if
// VECTOR_BENCH_LARGE is 1, as loading them takes a while.
func BenchmarkNearest(b *testing.B) {
	sizes := []int{10_000}
	if os.Getenv("VECTOR_BENCH_LARGE") == "1" {
		sizes = append(sizes, 100_000)
	}

	engines := []struct {
		name string
		open func(testing.TB) kv.DB
	}{
		{"memkv", testkv.NewMem},
		{"badger", testkv.NewBadger},
	}
	for _, engine := range engines {
		for _, n := range sizes {
			b.Run(fmt.Sprintf("%s/%d", engine.name, n), func(b *testing.B) {
				benchNearest(b, engine.open(b), n)
			})
		}
	}
}

func benchNearest(b *testing.B, db kv.DB, n int) {
	docs := extstore.New[int64, doc]("docs")
	x := vector.On(docs, "by_embedding", benchDims, vector.Cosine, embedding)
	require.NoError(b, docs.Check(db))

	rng := rand.New(rand.NewPCG(1, 2))
	random := func() []float32 {
		v := make([]float32, benchDims)
		for i := range v {
			v[i] = float32(rng.NormFloat64())
		}
		return v
	}

	for start := 1; start <= n; start += benchLoad {
		require.NoError(b, db.Update(func(tx kv.Tx) error {
			store := docs.Instantiate(tx)
			for id := start; id < start+benchLoad && id <= n; id++ {
				if err := store.Set(int64(id), &doc{Embedding: random()}); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	query := random()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := db.View(func(tx kv.Tx) error {
			res, err := x.Instantiate(tx).Nearest(query, 10)
			if err != nil {
				return err
			}
			defer res.Close()
			if res.Len() != 10 {
				return fmt.Errorf("found %d vectors", res.Len())
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}
