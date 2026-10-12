package iterutil_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/stretchr/testify/require"
)

// numbers iterates over the numbers from 0 to n-1, with their names as the values.
// It fails at the number failAt, and it counts how often it is closed.
type numbers struct {
	n, i   int
	failAt int
	closed int
}

func newNumbers(n int) *numbers {
	return &numbers{n: n, failAt: -1}
}

var errFailure = errors.New("failure")

func (it *numbers) Valid() bool { return it.i < it.n }
func (it *numbers) Next()       { it.i++ }
func (it *numbers) Close() error {
	it.closed++
	return nil
}

func (it *numbers) Key() (int, error) {
	if it.i == it.failAt {
		return 0, errFailure
	}
	return it.i, nil
}

func (it *numbers) Value() (string, error) {
	if it.i == it.failAt {
		return "", errFailure
	}
	return fmt.Sprint("n", it.i), nil
}

func even(k int, _ string) bool { return k%2 == 0 }

func TestAdvancedIterator(t *testing.T) {
	t.Run("KeysAndValues", func(t *testing.T) {
		base := newNumbers(3)
		keys, err := iterutil.Of(base).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2}, keys)
		require.Equal(t, 1, base.closed)

		values, err := iterutil.Of(newNumbers(3)).Values()
		require.NoError(t, err)
		require.Equal(t, []string{"n0", "n1", "n2"}, values)

		keys, err = iterutil.Of(newNumbers(0)).Keys()
		require.NoError(t, err)
		require.Empty(t, keys)
	})

	t.Run("Filter", func(t *testing.T) {
		keys, err := iterutil.Of(newNumbers(7)).Filter(even).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 2, 4, 6}, keys)

		values, err := iterutil.Of(newNumbers(7)).Filter(func(k int, _ string) bool { return k > 4 }).Values()
		require.NoError(t, err)
		require.Equal(t, []string{"n5", "n6"}, values)

		none := iterutil.Of(newNumbers(3)).Filter(func(int, string) bool { return false })
		require.False(t, none.Valid())
		none.Next()
		require.False(t, none.Valid())
	})

	t.Run("Limit", func(t *testing.T) {
		keys, err := iterutil.Of(newNumbers(7)).Limit(3).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2}, keys)

		keys, err = iterutil.Of(newNumbers(2)).Limit(5).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 1}, keys)

		keys, err = iterutil.Of(newNumbers(2)).Limit(0).Keys()
		require.NoError(t, err)
		require.Empty(t, keys)
	})

	t.Run("Map", func(t *testing.T) {
		lengths := iterutil.Of(newNumbers(11)).Map(func(_ int, v string) (int, error) { return len(v), nil })
		values, err := lengths.Filter(func(k, _ int) bool { return k >= 9 }).Values()
		require.NoError(t, err)
		require.Equal(t, []int{2, 3}, values)

		_, err = iterutil.Of(newNumbers(2)).Map(func(int, string) (int, error) { return 0, errFailure }).Values()
		require.ErrorIs(t, err, errFailure)
	})

	t.Run("Chain", func(t *testing.T) {
		values, err := iterutil.Of(newNumbers(100)).
			Filter(even).
			Map(func(k int, _ string) (int, error) { return k * k, nil }).
			Limit(4).
			Values()
		require.NoError(t, err)
		require.Equal(t, []int{0, 4, 16, 36}, values)
	})

	t.Run("Aggregate", func(t *testing.T) {
		base := newNumbers(5)
		sum, err := iterutil.Of(base).Filter(even).Aggregate(0, func(sum, k int, _ string) int { return sum + k })
		require.NoError(t, err)
		require.Equal(t, 6, sum)
		require.Equal(t, 1, base.closed)

		joined, err := iterutil.Of(newNumbers(3)).Aggregate("", func(s string, _ int, v string) string { return s + v })
		require.NoError(t, err)
		require.Equal(t, "n0n1n2", joined)
	})

	t.Run("All", func(t *testing.T) {
		base := newNumbers(4)
		var err error
		var keys []int
		for k, v := range iterutil.Of(base).All(&err) {
			require.Equal(t, fmt.Sprint("n", k), v)
			keys = append(keys, k)
		}
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2, 3}, keys)
		require.Equal(t, 1, base.closed)

		// A loop that is left early closes the iterator too.
		base = newNumbers(4)
		for k := range iterutil.Of(base).All(&err) {
			if k == 1 {
				break
			}
		}
		require.NoError(t, err)
		require.Equal(t, 1, base.closed)
	})

	t.Run("Errors", func(t *testing.T) {
		failing := func() *numbers {
			it := newNumbers(5)
			it.failAt = 2
			return it
		}

		_, err := iterutil.Of(failing()).Keys()
		require.ErrorIs(t, err, errFailure)
		_, err = iterutil.Of(failing()).Values()
		require.ErrorIs(t, err, errFailure)
		_, err = iterutil.Of(failing()).Aggregate(0, func(s, _ int, _ string) int { return s })
		require.ErrorIs(t, err, errFailure)

		// An error under a filter stops it, and is reported when it is consumed.
		filtered := iterutil.Of(failing()).Filter(func(k int, _ string) bool { return k >= 2 })
		require.False(t, filtered.Valid())
		require.ErrorIs(t, filtered.Err(), errFailure)

		_, err = iterutil.Of(failing()).Filter(even).Limit(10).Keys()
		require.ErrorIs(t, err, errFailure)
		_, err = iterutil.Of(failing()).Filter(even).
			Map(func(k int, _ string) (int, error) { return k, nil }).
			Aggregate(0, func(s, _, _ int) int { return s })
		require.ErrorIs(t, err, errFailure)

		base := failing()
		var keys []int
		for k := range iterutil.Of(base).All(&err) {
			keys = append(keys, k)
		}
		require.ErrorIs(t, err, errFailure)
		require.Equal(t, []int{0, 1}, keys)
		require.Equal(t, 1, base.closed)

		err = nil
		for range iterutil.Of(failing()).Filter(even).All(&err) {
		}
		require.ErrorIs(t, err, errFailure)
	})
}

func TestLookup(t *testing.T) {
	// The names of the even numbers only, as a store that has no record for the others.
	names := func(v string) (string, error) {
		var k int
		if _, err := fmt.Sscanf(v, "n%d", &k); err != nil {
			return "", err
		}
		if k%2 != 0 {
			return "", fmt.Errorf("number %d: %w", k, kv.ErrNotFound)
		}
		return fmt.Sprint("even", k), nil
	}

	base := newNumbers(6)
	found := iterutil.Of(base).Lookup(names)
	var keys []int
	var values []string
	for ; found.Valid(); found.Next() {
		key, err := found.Key()
		require.NoError(t, err)
		value, err := found.Value()
		require.NoError(t, err)
		keys, values = append(keys, key), append(values, value)
	}
	require.NoError(t, found.Err())
	require.NoError(t, found.Close())
	require.Equal(t, []int{0, 2, 4}, keys)
	require.Equal(t, []string{"even0", "even2", "even4"}, values)
	require.Equal(t, 1, base.closed)

	// Nothing is found, and nothing is left to find.
	none := iterutil.Of(newNumbers(4)).Lookup(func(string) (int, error) { return 0, kv.ErrNotFound })
	require.False(t, none.Valid())
	none.Next()
	require.False(t, none.Valid())
	require.NoError(t, none.Err())

	// An error other than a record that is not there stops the iterator.
	_, err := iterutil.Of(newNumbers(4)).Lookup(func(v string) (int, error) {
		if v == "n2" {
			return 0, errFailure
		}
		return len(v), nil
	}).Values()
	require.ErrorIs(t, err, errFailure)

	failing := newNumbers(4)
	failing.failAt = 1
	_, err = iterutil.Of(failing).Lookup(names).Keys()
	require.ErrorIs(t, err, errFailure)
}

func TestFlatMap(t *testing.T) {
	var inners []*numbers
	// Every number k has the numbers below it, and the odd ones have no iterator at all.
	below := func(k int, _ string) (iterutil.Iterator[int, string], error) {
		if k%2 != 0 {
			return nil, nil
		}
		inner := newNumbers(k)
		inners = append(inners, inner)
		return inner, nil
	}

	base := newNumbers(5)
	keys, err := iterutil.Of(base).FlatMap(below).Keys()
	require.NoError(t, err)
	// 0 has none, 2 has 0 and 1, 4 has 0 to 3.
	require.Equal(t, []int{0, 1, 0, 1, 2, 3}, keys)
	require.Equal(t, 1, base.closed)
	require.Len(t, inners, 3)
	for _, inner := range inners {
		require.Equal(t, 1, inner.closed)
	}

	values, err := iterutil.Of(newNumbers(3)).FlatMap(below).Limit(1).Values()
	require.NoError(t, err)
	require.Equal(t, []string{"n0"}, values)

	// The types of the keys and the values can change.
	pairs, err := iterutil.Of(newNumbers(3)).
		FlatMap(func(k int, v string) (iterutil.Iterator[string, int], error) {
			return iterutil.Of[int, string](newNumbers(k)).Map(func(int, string) (int, error) { return k, nil }).
				FlatMap(func(_ int, n int) (iterutil.Iterator[string, int], error) {
					return &one[string, int]{key: v, value: n}, nil
				}), nil
		}).
		Aggregate("", func(s string, k string, v int) string { return s + fmt.Sprint(k, "=", v, " ") })
	require.NoError(t, err)
	require.Equal(t, "n1=1 n2=2 n2=2 ", pairs)

	// An iterator that is closed early closes the one that it is in the middle of.
	inners = nil
	base = newNumbers(5)
	it := iterutil.Of(base).FlatMap(below)
	for ; it.Valid(); it.Next() {
		if k, _ := it.Key(); k == 1 {
			break
		}
	}
	require.NoError(t, it.Close())
	require.False(t, it.Valid())
	require.Equal(t, 1, base.closed)
	for _, inner := range inners {
		require.Equal(t, 1, inner.closed)
	}

	t.Run("Errors", func(t *testing.T) {
		// Of the function.
		_, err := iterutil.Of(newNumbers(3)).
			FlatMap(func(k int, _ string) (iterutil.Iterator[int, string], error) {
				if k == 1 {
					return nil, errFailure
				}
				return newNumbers(1), nil
			}).Keys()
		require.ErrorIs(t, err, errFailure)

		// Of the base iterator.
		failing := newNumbers(4)
		failing.failAt = 2
		_, err = iterutil.Of(failing).FlatMap(below).Keys()
		require.ErrorIs(t, err, errFailure)

		// Of an iterator that the function returns, which stops what comes after it too.
		var inner *numbers
		got, err := iterutil.Of(newNumbers(3)).
			FlatMap(func(k int, _ string) (iterutil.Iterator[int, string], error) {
				if k != 1 {
					return newNumbers(2), nil
				}
				inner = newNumbers(3)
				return iterutil.Of[int, string](inner).Lookup(func(v string) (string, error) {
					if v == "n1" {
						return "", errFailure
					}
					return v, nil
				}), nil
			}).Values()
		require.ErrorIs(t, err, errFailure)
		// What came before the failure, and nothing of the iterator of 2.
		require.Equal(t, []string{"n0", "n1", "n0"}, got)
		require.Equal(t, 1, inner.closed)
	})
}

// one iterates over one record.
type one[K, V any] struct {
	key   K
	value V
	done  bool
}

func (it *one[K, V]) Valid() bool       { return !it.done }
func (it *one[K, V]) Next()             { it.done = true }
func (it *one[K, V]) Key() (K, error)   { return it.key, nil }
func (it *one[K, V]) Value() (V, error) { return it.value, nil }
func (it *one[K, V]) Close() error      { return nil }
