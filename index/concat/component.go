package concat

import (
	"reflect"
	"strings"
)

var (
	float64Type = reflect.TypeFor[float64]()
	int64Type   = reflect.TypeFor[int64]()
)

// Component represents a component of the concatenated keys of an index.
type Component struct {
	path       string
	typed      bool
	descending bool
	convertTo  reflect.Type
}

// NewComponent creates a new component with the given path.
func NewComponent(path string) Component {
	return Component{path: path}
}

// Typed cause the component to include the type of value as a prefix byte of index component.
func (comp Component) Typed() Component {
	comp.typed = true
	return comp
}

// Desc sets the descending flag of the component.
func (comp Component) Desc() Component {
	comp.descending = true
	return comp
}

// AsFloat64 converts the values of the component to float64, so that numbers of different
// types sort together. A value that is not a number becomes NaN.
func (comp Component) AsFloat64() Component {
	comp.convertTo = float64Type
	return comp
}

// AsInt64 converts the values of the component to int64. A value that is not a number becomes zero.
func (comp Component) AsInt64() Component {
	comp.convertTo = int64Type
	return comp
}

// String returns the component as it is written in the encoding of an index.
func (comp Component) String() string {
	var sb strings.Builder
	sb.WriteString(comp.path)
	if comp.descending {
		sb.WriteString(" desc")
	}
	if comp.typed {
		sb.WriteString(" typed")
	}
	if comp.convertTo != nil {
		sb.WriteString(" as " + comp.convertTo.String())
	}
	return sb.String()
}
