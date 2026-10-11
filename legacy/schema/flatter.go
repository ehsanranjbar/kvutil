package schema

// Flatter is an interface for flattening a hierarchy of values to a map of paths -> values.
type Flatter[T any] interface {
	Flatten(t T) (map[string]any, error)
}
