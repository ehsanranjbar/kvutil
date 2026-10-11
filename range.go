package kvutil

import (
	"fmt"
	"strings"
)

// Range is a range of values, which is bounded on either side, on both, or on none.
// The zero Range has every value in it.
//
// A range is made with a function that sets one of its sides, and its other side is set with
// the method of the same name:
//
//	kvutil.All[int]()               // every value
//	kvutil.From(10)                 // 10 and what is after it
//	kvutil.After(10).Until(20)      // after 10 and before 20
//	kvutil.From(10).Through(20)     // from 10 to 20, with both
//	kvutil.Between(10, 20)          // the same
//	kvutil.Only(10)                 // 10 alone
type Range[T any] struct {
	low  *bound[T]
	high *bound[T]
}

type bound[T any] struct {
	value     T
	exclusive bool
}

// All returns the range of every value.
func All[T any]() Range[T] {
	return Range[T]{}
}

// From returns the range of a value and of the values after it.
func From[T any](low T) Range[T] {
	return Range[T]{}.From(low)
}

// After returns the range of the values after a value.
func After[T any](low T) Range[T] {
	return Range[T]{}.After(low)
}

// Until returns the range of the values before a value.
func Until[T any](high T) Range[T] {
	return Range[T]{}.Until(high)
}

// Through returns the range of a value and of the values before it.
func Through[T any](high T) Range[T] {
	return Range[T]{}.Through(high)
}

// Between returns the range of two values and of the values between them.
func Between[T any](low, high T) Range[T] {
	return Range[T]{}.From(low).Through(high)
}

// Only returns the range of a single value.
func Only[T any](value T) Range[T] {
	return Between(value, value)
}

// From returns the range with a value as its start, which is in the range.
func (r Range[T]) From(low T) Range[T] {
	r.low = &bound[T]{value: low}
	return r
}

// After returns the range with a value as its start, which is not in the range.
func (r Range[T]) After(low T) Range[T] {
	r.low = &bound[T]{value: low, exclusive: true}
	return r
}

// Until returns the range with a value as its end, which is not in the range.
func (r Range[T]) Until(high T) Range[T] {
	r.high = &bound[T]{value: high, exclusive: true}
	return r
}

// Through returns the range with a value as its end, which is in the range.
func (r Range[T]) Through(high T) Range[T] {
	r.high = &bound[T]{value: high}
	return r
}

// Low returns the start of the range and whether the start itself is out of the range.
// It is not ok if the range has no start.
func (r Range[T]) Low() (value T, exclusive, ok bool) {
	if r.low == nil {
		return value, false, false
	}
	return r.low.value, r.low.exclusive, true
}

// High returns the end of the range and whether the end itself is out of the range.
// It is not ok if the range has no end.
func (r Range[T]) High() (value T, exclusive, ok bool) {
	if r.high == nil {
		return value, false, false
	}
	return r.high.value, r.high.exclusive, true
}

// String returns the range in the notation of intervals, like [10, 20) for the values from 10
// up to 20, with 10 and without 20.
func (r Range[T]) String() string {
	var sb strings.Builder
	if r.low == nil {
		sb.WriteString("(-∞")
	} else if r.low.exclusive {
		fmt.Fprintf(&sb, "(%v", r.low.value)
	} else {
		fmt.Fprintf(&sb, "[%v", r.low.value)
	}
	sb.WriteString(", ")
	if r.high == nil {
		sb.WriteString("∞)")
	} else if r.high.exclusive {
		fmt.Fprintf(&sb, "%v)", r.high.value)
	} else {
		fmt.Fprintf(&sb, "%v]", r.high.value)
	}
	return sb.String()
}
