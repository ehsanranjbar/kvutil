package expr

// Assigned associates a name with an expression.
type Assigned struct {
	name       string
	expression any
}

// NewAssigned creates a new named expression.
func NewAssigned(name string, expr any) Assigned {
	return Assigned{name: name, expression: expr}
}

// Name returns the name of the expression.
func (n Assigned) Name() string { return n.name }

// Expr returns the expression.
func (n Assigned) Expression() any { return n.expression }
