package qlutil

import (
	"time"

	qlvalue "github.com/araddon/qlbridge/value"
	"github.com/ehsanranjbar/kvutil/schema"
)

// ContextWrapper is a wrapper around a type that implements the qlbridge.ContextReader interface.
type ContextWrapper[I, D any] struct {
	id        I
	data      D
	extractor schema.PathExtractor[D]
	flatter   schema.Flatter[D]
}

// NewContextWrapper creates a new ContextWrapper.
func NewContextWrapper[I, D any](
	id I,
	data D,
	extractor schema.PathExtractor[D],
	flatter schema.Flatter[D],
) *ContextWrapper[I, D] {
	return &ContextWrapper[I, D]{
		id:        id,
		data:      data,
		extractor: extractor,
		flatter:   flatter,
	}
}

// Get implements the qlbridge.ContextReader interface.
func (c *ContextWrapper[I, D]) Get(key string) (qlvalue.Value, bool) {
	switch {
	case key == "_id":
		return qlvalue.NewValue(c.id), true
	default:
		v, err := c.extractor.ExtractPath(c.data, key)
		if err != nil {
			return qlvalue.NewErrorValue(err), false
		}
		return qlvalue.NewValue(v), true
	}
}

// Row implements the qlbridge.ContextReader interface.
// I don't know what this is supposed to do.
func (c *ContextWrapper[I, D]) Row() map[string]qlvalue.Value {
	if c.flatter == nil {
		return nil
	}

	flat, err := c.flatter.Flatten(c.data)
	if err != nil {
		return nil
	}
	row := make(map[string]qlvalue.Value, len(flat))
	for k, v := range flat {
		row[k] = qlvalue.NewValue(v)
	}
	return row
}

// Ts implements the qlbridge.ContextReader interface.
// I don't know what this is supposed to do.
func (c *ContextWrapper[I, D]) Ts() time.Time { return time.Time{} }
