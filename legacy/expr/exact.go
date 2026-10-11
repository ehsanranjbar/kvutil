package expr

// Exact represents an exact equality expression.
type Exact[T any] struct {
	value T
}

// NewExact creates a new equality expression.
func NewExact[T any](value T) Exact[T] {
	return Exact[T]{value: value}
}

// Value returns the value of the equality expression.
func (e Exact[T]) Value() T { return e.value }
