package expr

// Set represents a set that the value must be in.
type Set[T any] struct {
	values []T
}

// NewSet creates a new in expression.
func NewSet[T any](values ...T) Set[T] {
	return Set[T]{values: values}
}

// Values returns the values of the in expression.
func (i Set[T]) Values() []T { return i.values }
