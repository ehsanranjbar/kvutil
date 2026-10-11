package iters_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ehsanranjbar/kvutil/iters"
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

func TestSeq(t *testing.T) {
	t.Run("KeysAndValues", func(t *testing.T) {
		base := newNumbers(3)
		keys, err := iters.From(base).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2}, keys)
		require.Equal(t, 1, base.closed)

		values, err := iters.From(newNumbers(3)).Values()
		require.NoError(t, err)
		require.Equal(t, []string{"n0", "n1", "n2"}, values)

		keys, err = iters.From(newNumbers(0)).Keys()
		require.NoError(t, err)
		require.Empty(t, keys)
	})

	t.Run("Filter", func(t *testing.T) {
		keys, err := iters.From(newNumbers(7)).Filter(even).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 2, 4, 6}, keys)

		values, err := iters.From(newNumbers(7)).Filter(func(k int, _ string) bool { return k > 4 }).Values()
		require.NoError(t, err)
		require.Equal(t, []string{"n5", "n6"}, values)

		none := iters.From(newNumbers(3)).Filter(func(int, string) bool { return false })
		require.False(t, none.Valid())
		none.Next()
		require.False(t, none.Valid())
	})

	t.Run("Limit", func(t *testing.T) {
		keys, err := iters.From(newNumbers(7)).Limit(3).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2}, keys)

		keys, err = iters.From(newNumbers(2)).Limit(5).Keys()
		require.NoError(t, err)
		require.Equal(t, []int{0, 1}, keys)

		keys, err = iters.From(newNumbers(2)).Limit(0).Keys()
		require.NoError(t, err)
		require.Empty(t, keys)
	})

	t.Run("Map", func(t *testing.T) {
		lengths := iters.From(newNumbers(11)).Map(func(_ int, v string) (int, error) { return len(v), nil })
		values, err := lengths.Filter(func(k, _ int) bool { return k >= 9 }).Values()
		require.NoError(t, err)
		require.Equal(t, []int{2, 3}, values)

		_, err = iters.From(newNumbers(2)).Map(func(int, string) (int, error) { return 0, errFailure }).Values()
		require.ErrorIs(t, err, errFailure)
	})

	t.Run("Chain", func(t *testing.T) {
		values, err := iters.From(newNumbers(100)).
			Filter(even).
			Map(func(k int, _ string) (int, error) { return k * k, nil }).
			Limit(4).
			Values()
		require.NoError(t, err)
		require.Equal(t, []int{0, 4, 16, 36}, values)
	})

	t.Run("Aggregate", func(t *testing.T) {
		base := newNumbers(5)
		sum, err := iters.From(base).Filter(even).Aggregate(0, func(sum, k int, _ string) int { return sum + k })
		require.NoError(t, err)
		require.Equal(t, 6, sum)
		require.Equal(t, 1, base.closed)

		joined, err := iters.From(newNumbers(3)).Aggregate("", func(s string, _ int, v string) string { return s + v })
		require.NoError(t, err)
		require.Equal(t, "n0n1n2", joined)
	})

	t.Run("All", func(t *testing.T) {
		base := newNumbers(4)
		var err error
		var keys []int
		for k, v := range iters.From(base).All(&err) {
			require.Equal(t, fmt.Sprint("n", k), v)
			keys = append(keys, k)
		}
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2, 3}, keys)
		require.Equal(t, 1, base.closed)

		// A loop that is left early closes the iterator too.
		base = newNumbers(4)
		for k := range iters.From(base).All(&err) {
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

		_, err := iters.From(failing()).Keys()
		require.ErrorIs(t, err, errFailure)
		_, err = iters.From(failing()).Values()
		require.ErrorIs(t, err, errFailure)
		_, err = iters.From(failing()).Aggregate(0, func(s, _ int, _ string) int { return s })
		require.ErrorIs(t, err, errFailure)

		// An error under a filter stops it, and is reported when it is consumed.
		filtered := iters.From(failing()).Filter(func(k int, _ string) bool { return k >= 2 })
		require.False(t, filtered.Valid())
		require.ErrorIs(t, filtered.Err(), errFailure)

		_, err = iters.From(failing()).Filter(even).Limit(10).Keys()
		require.ErrorIs(t, err, errFailure)
		_, err = iters.From(failing()).Filter(even).
			Map(func(k int, _ string) (int, error) { return k, nil }).
			Aggregate(0, func(s, _, _ int) int { return s })
		require.ErrorIs(t, err, errFailure)

		base := failing()
		var keys []int
		for k := range iters.From(base).All(&err) {
			keys = append(keys, k)
		}
		require.ErrorIs(t, err, errFailure)
		require.Equal(t, []int{0, 1}, keys)
		require.Equal(t, 1, base.closed)

		err = nil
		for range iters.From(failing()).Filter(even).All(&err) {
		}
		require.ErrorIs(t, err, errFailure)
	})
}
