package field

import (
	"fmt"
	"strings"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
)

func (f Field[V, F]) comparison(op cond.Op, text string) *cond.Comparison[V] {
	c := cond.NewComparison(f.path, f.codec.Name(), op, f.ascending).Describe(text)
	c.Multi = f.multi
	return c
}

// Eq returns the condition of the field being equal to a value.
func (f Field[V, F]) Eq(value F) *cond.Comparison[V] {
	c := f.comparison(cond.OpEq, fmt.Sprintf("%s = %#v", f.path, value))
	key, err := f.Encode(value)
	c.Values = [][]byte{key}
	c.Fail(err)
	return c
}

// In returns the condition of the field being equal to one of the values.
func (f Field[V, F]) In(values ...F) *cond.Comparison[V] {
	texts := make([]string, 0, len(values))
	for _, value := range values {
		texts = append(texts, fmt.Sprintf("%#v", value))
	}
	c := f.comparison(cond.OpIn, fmt.Sprintf("%s in (%s)", f.path, strings.Join(texts, ", ")))

	for _, value := range values {
		key, err := f.Encode(value)
		c.Fail(err)
		c.Values = append(c.Values, key)
	}
	return c
}

// Gt returns the condition of the field being greater than a value.
func (f Field[V, F]) Gt(value F) *cond.Comparison[V] {
	return f.Range(kvutil.After(value))
}

// Gte returns the condition of the field being greater than a value, or equal to it.
func (f Field[V, F]) Gte(value F) *cond.Comparison[V] {
	return f.Range(kvutil.From(value))
}

// Lt returns the condition of the field being less than a value.
func (f Field[V, F]) Lt(value F) *cond.Comparison[V] {
	return f.Range(kvutil.Until(value))
}

// Lte returns the condition of the field being less than a value, or equal to it.
func (f Field[V, F]) Lte(value F) *cond.Comparison[V] {
	return f.Range(kvutil.Through(value))
}

// Between returns the condition of the field being between two values, or equal to one of them.
func (f Field[V, F]) Between(low, high F) *cond.Comparison[V] {
	return f.Range(kvutil.Between(low, high))
}

// Range returns the condition of the field being in a range.
func (f Field[V, F]) Range(r kvutil.Range[F]) *cond.Comparison[V] {
	var texts []string
	c := f.comparison(cond.OpRange, "")

	if low, exclusive, ok := r.Low(); ok {
		key, err := f.Encode(low)
		c.Fail(err)
		c.Low = &cond.Bound{Value: key, Exclusive: exclusive}
		if exclusive {
			texts = append(texts, fmt.Sprintf("%s > %#v", f.path, low))
		} else {
			texts = append(texts, fmt.Sprintf("%s >= %#v", f.path, low))
		}
	}
	if high, exclusive, ok := r.High(); ok {
		key, err := f.Encode(high)
		c.Fail(err)
		c.High = &cond.Bound{Value: key, Exclusive: exclusive}
		if exclusive {
			texts = append(texts, fmt.Sprintf("%s < %#v", f.path, high))
		} else {
			texts = append(texts, fmt.Sprintf("%s <= %#v", f.path, high))
		}
	}

	if len(texts) == 0 {
		texts = []string{f.path + " is anything"}
	}
	return c.Describe(strings.Join(texts, " and "))
}
