package agg

import (
	"cmp"
	"time"

	"github.com/ehsanranjbar/kvutil/iters"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Number is a type that can be summed.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Count defines an aggregation that counts the records of each group. The function returns the
// group of a record, and whether the record is counted.
func Count[K, V, G any](
	store extstore.Extensible[K, V],
	name string,
	group func(v *V) (G, bool),
	opts ...Option,
) *Agg[K, V, G, int64, int64] {
	extract := func(v *V) (G, int64, bool) {
		g, ok := group(v)
		return g, 1, ok
	}
	return Sum(store, name, extract, append([]Option{Describe("count", "", "")}, opts...)...)
}

// Sum defines an aggregation that sums what the records of each group contribute.
// A sum of floating-point numbers may drift a little as records are changed and deleted.
func Sum[K, V, G any, N Number](
	store extstore.Extensible[K, V],
	name string,
	extract Extract[V, G, N],
	opts ...Option,
) *Agg[K, V, G, N, N] {
	fold := Fold[N, N]{
		Add:    func(sum, x N) N { return sum + x },
		Remove: func(sum, x N) N { return sum - x },
	}
	return Delta(store, name, extract, fold, append([]Option{Describe("sum", "", "")}, opts...)...)
}

// Average is the state of an Avg aggregation.
type Average[N Number] struct {
	Sum   N
	Count int64
}

// Value returns the average, which is zero if nothing was counted.
func (a Average[N]) Value() float64 {
	if a.Count == 0 {
		return 0
	}
	return float64(a.Sum) / float64(a.Count)
}

// Avg defines an aggregation that averages what the records of each group contribute.
func Avg[K, V, G any, N Number](
	store extstore.Extensible[K, V],
	name string,
	extract Extract[V, G, N],
	opts ...Option,
) *Agg[K, V, G, N, Average[N]] {
	fold := Fold[N, Average[N]]{
		Add:    func(a Average[N], x N) Average[N] { return Average[N]{Sum: a.Sum + x, Count: a.Count + 1} },
		Remove: func(a Average[N], x N) Average[N] { return Average[N]{Sum: a.Sum - x, Count: a.Count - 1} },
	}
	return Delta(store, name, extract, fold, append([]Option{Describe("avg", "", "")}, opts...)...)
}

// Min defines an aggregation of the least of what the records of each group contribute.
// It stays right when records are changed and deleted, as it is computed from the records.
// With several buckets, the spans of the first one must be inside the spans of the others.
func Min[K, V, G any, X cmp.Ordered](
	store extstore.Extensible[K, V],
	name string,
	extract Extract[V, G, X],
	opts ...Option,
) *Agg[K, V, G, X, X] {
	return extreme(store, name, "min", extract, func(a, b X) X { return min(a, b) }, opts)
}

// Max defines an aggregation of the greatest of what the records of each group contribute.
// It stays right when records are changed and deleted, as it is computed from the records.
// With several buckets, the spans of the first one must be inside the spans of the others.
func Max[K, V, G any, X cmp.Ordered](
	store extstore.Extensible[K, V],
	name string,
	extract Extract[V, G, X],
	opts ...Option,
) *Agg[K, V, G, X, X] {
	return extreme(store, name, "max", extract, func(a, b X) X { return max(a, b) }, opts)
}

func extreme[K, V, G any, X cmp.Ordered](
	store extstore.Extensible[K, V],
	name, function string,
	extract Extract[V, G, X],
	pick func(a, b X) X,
	opts []Option,
) *Agg[K, V, G, X, X] {
	compute := func(contributions iters.Seq[time.Time, X]) (X, error) {
		first := true
		return contributions.Aggregate(*new(X), func(best X, _ time.Time, x X) X {
			if first {
				first = false
				return x
			}
			return pick(best, x)
		})
	}
	opts = append([]Option{Describe(function, "", ""), WithMerge(pick)}, opts...)
	return Recompute(store, name, extract, compute, opts...)
}
