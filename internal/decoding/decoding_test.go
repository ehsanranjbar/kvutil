package decoding

import (
	"testing"
	"time"
)

// feed runs n records through a choice, each decoded in part in partial and, if it satisfies
// the conditions, in full in whole, as a filter does, and returns how many were decoded in full
// at once.
func feed(c *Choice, n int, satisfied func(i int) bool, partial, whole time.Duration) (oneStep int) {
	ago := func(d time.Duration) time.Time { return time.Now().Add(-d) }
	for i := range n {
		ok := satisfied(i)
		if c.Full() {
			oneStep++
			c.StartWhole()
			c.EndWhole(ago(whole))
		} else {
			c.StartPartial()
			c.EndPartial(ago(partial))
			if ok {
				c.StartWhole()
				c.EndWhole(ago(whole))
			}
		}
		c.Checked(ok)
	}
	return oneStep
}

func TestChoice(t *testing.T) {
	all := func(int) bool { return true }
	none := func(int) bool { return false }
	// A fraction of the records, spread over them.
	share := func(percent int) func(int) bool { return func(i int) bool { return i%100 < percent } }

	const p, f = time.Microsecond, 2 * time.Microsecond // two steps lose past half of the records

	tests := []struct {
		name      string
		satisfied func(int) bool
		partial   time.Duration
		oneStep   bool
	}{
		{"all", all, p, true},
		{"none", none, p, false},
		{"most", share(70), p, true},
		{"few", share(30), p, false},
		{"near the even point", share(52), p, false},
		// A decode of the fields that costs nearly as much as the record makes one step the better
		// for all but a few records...
		{"costly fields", share(10), 1900 * time.Nanosecond, true},
		// ...and one that costs nothing makes two steps the better for nearly all of them.
		{"cheap fields", share(90), 10 * time.Nanosecond, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var c Choice
			feed(&c, firstWindow+3*window, test.satisfied, test.partial, f)
			if c.Full() != test.oneStep {
				t.Fatalf("decodes in full at once: %v, want %v", c.Full(), test.oneStep)
			}
		})
	}

	t.Run("the first window", func(t *testing.T) {
		var c Choice
		if n := feed(&c, firstWindow, all, p, f); n != 0 || !c.Full() {
			t.Fatalf("after the first window: %d in one step, full %v", n, c.Full())
		}
	})

	t.Run("data that changes", func(t *testing.T) {
		var c Choice
		feed(&c, firstWindow+window, all, p, f)
		if !c.Full() {
			t.Fatal("not in one step where all are satisfied")
		}
		feed(&c, 2*window, none, p, f)
		if c.Full() {
			t.Fatal("still in one step where none is satisfied")
		}
	})

	t.Run("not adaptive", func(t *testing.T) {
		restore := SetAdaptive(false)
		defer restore()
		var c Choice
		if n := feed(&c, firstWindow+window, all, p, f); n != 0 || c.Full() {
			t.Fatalf("%d in one step, full %v", n, c.Full())
		}
	})

	t.Run("no time yet", func(t *testing.T) {
		// A run where no record is decoded in full has nothing to compare, and stays in two steps.
		var c Choice
		for range firstWindow {
			c.StartPartial()
			c.Checked(true)
		}
		if c.Full() {
			t.Fatal("in one step without a time of a decode")
		}
	})
}
