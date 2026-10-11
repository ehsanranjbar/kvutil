package concat

import (
	"reflect"
)

const (
	// DefaultMaxComponentSize is the default size that the binary representation of a component will be padded or truncated to.
	DefaultMaxComponentSize = 256
)

var (
	float64Type = reflect.TypeOf(float64(0))
	int64Type   = reflect.TypeOf(int64(0))
)

// Component represents a component of concatenated keys in index
type Component struct {
	path       string
	typed      bool
	size       int
	descending bool
	convertTo  reflect.Type
}

// NewComponent creates a new component with the given path.
func NewComponent(path string) Component {
	return Component{path: path, size: DefaultMaxComponentSize}
}

// Typed cause the component to include the type of value as a prefix byte of index component.
func (comp Component) Typed() Component {
	comp.typed = true
	return comp
}

// Size modifies the size of the component.
func (comp Component) WithSize(size int) Component {
	if size <= 0 {
		panic("size must be positive")
	}

	comp.size = size
	return comp
}

// Desc sets the descending flag of the component.
func (comp Component) Desc() Component {
	comp.descending = true
	return comp
}

// UnifyNumbers sets the unified flag of the component which converts all numeric types to float64.
func (comp Component) AsFloat64() Component {
	comp.convertTo = float64Type
	return comp
}

func (comp Component) AsInt64() Component {
	comp.convertTo = int64Type
	return comp
}
