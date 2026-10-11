package expr

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Range represents a range of values of a sequential type.
type Range[T any] struct {
	low  *Bound[T]
	high *Bound[T]
}

// NewRange creates a new range lookup expression.
func NewRange[T any](low, high *Bound[T]) Range[T] {
	return Range[T]{low: low, high: high}
}

// Low returns the low bound of the range.
func (r Range[T]) Low() *Bound[T] { return r.low }

// High returns the high bound of the range.
func (r Range[T]) High() *Bound[T] { return r.high }

// String returns the string representation of the range.
func (r Range[T]) String() string {
	var sb strings.Builder
	if !r.Low().IsEmpty() && r.Low().Exclusive() {
		sb.WriteString("(")
	} else {
		sb.WriteString("[")
	}
	sb.WriteString(r.encodeBound(r.Low(), true))
	sb.WriteString(", ")
	sb.WriteString(r.encodeBound(r.High(), false))
	if r.Low().IsEmpty() || r.High().Exclusive() {
		sb.WriteString(")")
	} else {
		sb.WriteString("]")
	}
	return sb.String()
}

func (r Range[T]) encodeBound(b *Bound[T], low bool) string {
	var t T
	switch any(t).(type) {
	case []byte:
		if b.IsEmpty() {
			if low {
				return "0x00"
			} else {
				return "∞"
			}
		}
		return "0x" + hex.EncodeToString(any(b.Value()).([]byte))
	default:
		if b.IsEmpty() {
			if low {
				return "-∞"
			} else {
				return "∞"
			}
		}
		return fmt.Sprint(b.Value())
	}
}

// Bound represents a bound. either exclusive or inclusive.
type Bound[T any] struct {
	value     T
	exclusive bool
}

// NewBound creates a new bound.
func NewBound[T any](value T, exclusive bool) *Bound[T] {
	return &Bound[T]{value: value, exclusive: exclusive}
}

// Value returns the value of the bound.
func (b Bound[T]) Value() T { return b.value }

// Exclusive returns true if the bound is exclusive.
func (b *Bound[T]) Exclusive() bool { return b != nil && b.exclusive }

// IsEmpty returns true if the bound is nil.
func (b *Bound[T]) IsEmpty() bool { return b == nil }
