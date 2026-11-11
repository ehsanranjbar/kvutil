package cond_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
)

type rec struct{ A, B, C int }

// opaque is a condition that does not say what it reads.
type opaque struct{}

func (opaque) Match(*rec) (bool, error) { return true, nil }
func (opaque) String() string           { return "opaque" }

func compare(path string) *cond.Comparison[rec] {
	return cond.NewComparison[rec](path, "int", cond.OpEq, nil).Describe(path)
}

func TestReadsOfConditions(t *testing.T) {
	known := func(c cond.Cond[rec], want ...string) {
		t.Helper()
		paths, ok := cond.Reads(c)
		require.True(t, ok, c.String())
		if len(want) == 0 {
			require.Empty(t, paths, c.String())
			return
		}
		require.Equal(t, want, paths, c.String())
	}
	unknown := func(c cond.Cond[rec]) {
		t.Helper()
		paths, ok := cond.Reads(c)
		require.False(t, ok, c.String())
		require.Nil(t, paths)
	}

	// A comparison reads what it is told to, and nothing is known without it.
	known(compare("A").Reading("A"), "A")
	known(compare("A").Reading("A", "B.X"), "A", "B.X")
	known(compare("A").Reading())
	unknown(compare("A"))

	// A function does the same.
	f := func(*rec) bool { return true }
	known(cond.Func("f", f).Reading("A"), "A")
	known(cond.Func("f", f).Reading())
	unknown(cond.Func("f", f))

	// The reads that are returned are copies.
	c := compare("A").Reading("A")
	paths, _ := c.Reads()
	paths[0] = "Z"
	known(c, "A")
	given := []string{"B"}
	p := cond.Func("f", f).Reading(given...)
	given[0] = "Z"
	known(p, "B")

	// Nothing is read by no conditions, and by the ones that read nothing.
	paths, ok := cond.Reads[rec]()
	require.True(t, ok)
	require.Empty(t, paths)
	known(cond.And[rec]())
	known(cond.Or[rec]())

	// A conjunction, a disjunction and a negation read what their conditions do, sorted, each once.
	a, b, ab := compare("A").Reading("A"), compare("B").Reading("B"), compare("AB").Reading("B", "A")
	known(cond.And[rec](b, a), "A", "B")
	known(cond.Or[rec](b, a, ab), "A", "B")
	known(cond.Not[rec](b), "B")
	known(cond.And[rec](cond.Or[rec](b, cond.Not[rec](a)), ab, cond.Func("f", f).Reading("C")), "A", "B", "C")
	known(cond.Not[rec](cond.And[rec]()))

	// One condition that is not known makes the whole of them not.
	nothing := cond.Func("f", f)
	unknown(cond.And[rec](a, nothing))
	unknown(cond.Or[rec](nothing, a))
	unknown(cond.Not[rec](nothing))
	unknown(cond.And[rec](a, cond.Or[rec](b, cond.Not[rec](compare("C")))))
	paths, ok = cond.Reads[rec](a, b, nothing)
	require.False(t, ok)
	require.Nil(t, paths)
	paths, ok = cond.Reads[rec](b, a, ab)
	require.True(t, ok)
	require.Equal(t, []string{"A", "B"}, paths)

	// A condition that is not a Reader is not known, wherever it is.
	unknown(opaque{})
	unknown(cond.And[rec](a, opaque{}))
	unknown(cond.Not[rec](opaque{}))
	unknown(cond.Or[rec](a, cond.And[rec](opaque{})))
}
