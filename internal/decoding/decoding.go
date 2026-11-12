// Package decoding chooses how the records that are checked against conditions are decoded: in
// two steps, the fields that the conditions read of every record and then the records that
// satisfy them in full, or in one, every record in full.
//
// Two steps cost p + a·f for a record, where p is the cost of decoding the fields of the
// conditions, f the cost of decoding a record in full, and a the fraction of the records that
// are decoded in full after they are checked. One step costs f. Two steps are the better as long
// as a < 1 − p/f. That depends on the codec: with msgpack the fields of a book cost about two
// thirds of the book, and two steps lose once a third of the books are wanted (measured on an
// Apple M5 Pro: they cross between 25% and 40% for books of 200 bytes, at about 50% for books
// with a payload of 4 KB and for documents with a vector of 384 floats); with FlatBuffers a field
// costs a tenth of a record, and they cross at about 90%. So a Choice measures p, f and a as the
// records go by, and decides again every window of records.
package decoding

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// firstWindow is the number of records that the first decision is made after, so that a short
	// run decides early, and window the number of the records of every decision after it.
	firstWindow = 32
	window      = 256
	// sampleEvery is how often a decode is timed: one in so many.
	sampleEvery = 4
	// margin is how far the fraction of the records that are wanted in full must be past the
	// point where the two ways cost the same for the choice to change, so that it does not
	// change back and forth on data that is near it.
	margin = 0.05
)

// Choice is the choice of one run over records: it is not safe for concurrent use. The zero
// value starts with two steps.
//
// A decode takes about the same time every time, and what makes one take longer, like a cold
// cache, a collection of the garbage or the scheduler, only adds to it. So the cost of a kind of
// decode is the least time that it took in the last window that timed one, not an average.
type Choice struct {
	full bool

	// The records of the current window: checked, and decoded in full (or, in one step, that
	// satisfied the conditions, which two steps would have decoded in full).
	size, seen, wanted int

	// The costs of a decode of the fields and of a decode in full, in nanoseconds, 0 until one is
	// timed; the least times of the current window, 0 if it timed none; and the counts of the
	// decodes that the samples are taken from.
	partial, whole       float64
	leastPart, leastFull float64
	partials, wholes     uint32
}

// Full reports whether the records are to be decoded in full at once.
func (c *Choice) Full() bool { return c.full && adaptive.Load() }

// StartPartial returns the time that a decode of the fields starts at, if it is one that is
// timed, and the zero time otherwise. Its end is given to EndPartial.
func (c *Choice) StartPartial() time.Time {
	c.partials++
	if c.partials%sampleEvery != 0 {
		return time.Time{}
	}
	return time.Now()
}

// EndPartial ends a decode of the fields that StartPartial started.
func (c *Choice) EndPartial(start time.Time) {
	if !start.IsZero() {
		c.leastPart = least(c.leastPart, time.Since(start))
	}
}

// StartWhole returns the time that a decode in full starts at, if it is one that is timed, and
// the zero time otherwise. Its end is given to EndWhole. Every decode in full that the choice is
// about goes through them, the ones after a record was checked as the ones of one step: they are
// what a is counted from.
func (c *Choice) StartWhole() time.Time {
	c.wholes++
	if !c.full {
		c.wanted++
	}
	// The first one is not timed, as it is the one that warms up what the others find warm.
	if c.wholes%sampleEvery != 2%sampleEvery {
		return time.Time{}
	}
	return time.Now()
}

// EndWhole ends a decode in full that StartWhole started.
func (c *Choice) EndWhole(start time.Time) {
	if !start.IsZero() {
		c.leastFull = least(c.leastFull, time.Since(start))
	}
}

// Checked counts a record that was checked, and whether it satisfied the conditions, and makes
// the choice again at the end of a window.
func (c *Choice) Checked(satisfied bool) {
	if c.size == 0 {
		c.size = firstWindow
	}
	c.seen++
	if c.full && satisfied {
		// Two steps would have decoded it in full.
		c.wanted++
	}
	if c.seen < c.size {
		return
	}

	if c.leastPart > 0 {
		c.partial = c.leastPart
	}
	if c.leastFull > 0 {
		c.whole = c.leastFull
	}
	c.leastPart, c.leastFull = 0, 0
	if c.partial > 0 && c.whole > 0 {
		wanted := float64(c.wanted) / float64(c.seen)
		even := 1 - c.partial/c.whole
		switch {
		case !c.full && wanted > even+margin:
			c.full = true
		case c.full && wanted < even-margin:
			c.full = false
		}
	}
	c.size, c.seen, c.wanted = window, 0, 0
}

// least returns the lesser of a least time so far, which is 0 for none, and a time.
func least(so float64, d time.Duration) float64 {
	// A decode of a few fields takes about as long as reading the clock, which would make
	// every decode look as costly as the others.
	ns := max(float64(d.Nanoseconds())-clock(), 1)
	if so == 0 {
		return ns
	}
	return min(so, ns)
}

var (
	clockOnce sync.Once
	clockNs   float64
)

// clock returns how long timing nothing takes, in nanoseconds: the least of a few tries.
func clock() float64 {
	clockOnce.Do(func() {
		least := time.Duration(math.MaxInt64)
		for range 64 {
			least = min(least, time.Since(time.Now()))
		}
		clockNs = float64(least.Nanoseconds())
	})
	return clockNs
}

var adaptive atomic.Bool

func init() { adaptive.Store(true) }

// SetAdaptive turns the choice on or off for the whole program, and returns the function that
// sets it back. Off, the records are always decoded in two steps. It is for tests of what two
// steps do, whose counts of decodes would otherwise depend on how fast the machine decodes.
func SetAdaptive(on bool) (restore func()) {
	was := adaptive.Swap(on)
	return func() { adaptive.Store(was) }
}
