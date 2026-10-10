package schema

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// PathExtractor is an interface for extracting a value with the given path from a given value.
type PathExtractor[T any] interface {
	ExtractPath(t T, path string) (any, error)
}

// ReflectPathExtractor is a PathExtractor that uses reflection to extract the value.
// It is safe for concurrent use.
type ReflectPathExtractor[T any] struct {
	rt        reflect.Type
	returnAny bool
	cache     sync.Map // path -> []int
}

// NewReflectPathExtractor creates a new ReflectPathExtractor for the given type.
func NewReflectPathExtractor[T any](returnAny bool) *ReflectPathExtractor[T] {
	rt := reflect.TypeFor[T]()
	if unwrapPtr(rt).Kind() != reflect.Struct {
		panic(fmt.Sprintf("reflect path extractor only supports struct types but got %s", rt))
	}

	return &ReflectPathExtractor[T]{
		rt:        rt,
		returnAny: returnAny,
	}
}

// ExtractPath implements the PathExtractor interface.
func (pe *ReflectPathExtractor[T]) ExtractPath(v T, path string) (any, error) {
	indices, err := pe.indices(path)
	if err != nil {
		return nil, err
	}

	rv := reflect.ValueOf(v)
	for _, i := range indices {
		var err error
		rv = unwrapPtr(rv).Field(i)
		if !rv.IsValid() {
			return nil, fmt.Errorf("invalid field %d: %w", i, err)
		}
		if rv.Kind() == reflect.Ptr && rv.IsNil() {
			return rv, nil
		}
	}

	if pe.returnAny {
		return rv.Interface(), nil
	} else {
		return rv, nil
	}
}

// indices returns the field indices of the path, verifying and caching them on first use.
// Only valid paths are cached, so an invalid path fails on every call.
func (pe *ReflectPathExtractor[T]) indices(path string) ([]int, error) {
	if cached, ok := pe.cache.Load(path); ok {
		return cached.([]int), nil
	}

	indices, err := pe.verifyPath(path)
	if err != nil {
		return nil, err
	}
	pe.cache.Store(path, indices)
	return indices, nil
}

func (pe *ReflectPathExtractor[T]) verifyPath(path string) ([]int, error) {
	indices := make([]int, 0)
	t := pe.rt
	for _, part := range strings.Split(path, ".") {
		f, ok := unwrapPtr(t).FieldByName(part)
		if !ok {
			return nil, fmt.Errorf("field %s not found in %s", part, t)
		}

		indices = append(indices, f.Index...)
		t = f.Type
	}

	return indices, nil
}

type reflectPtr[T any] interface {
	Kind() reflect.Kind
	Elem() T
}

func unwrapPtr[T reflectPtr[T]](v T) T {
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	return v
}

// ExtractPathFromAny extracts the value from the given any value with the given path.
func ExtractPathFromAny(v any, path string) (any, error) {
	parts := strings.SplitN(path, ".", 2)

	if len(parts) == 0 || path == "" {
		return v, nil
	}
	if len(parts) > 1 && parts[0] == "" {
		return ExtractPathFromAny(v, parts[1])
	}
	if v == nil {
		return nil, fmt.Errorf("cannot extract path %q from nil", path)
	}

	switch vv := v.(type) {
	case map[string]any:
		var ok bool
		v, ok = vv[parts[0]]
		if !ok {
			return nil, fmt.Errorf("key %q not found", parts[0])
		}
	case []any:
		if parts[0] == "*" {
			result := make([]any, 0, len(vv))
			for _, iv := range vv {
				if len(parts) > 1 {
					var err error
					iv, err = ExtractPathFromAny(iv, parts[1])
					if err != nil {
						return nil, err
					}
				}
				switch iv := iv.(type) {
				case []any:
					result = append(result, iv...)
				default:
					result = append(result, iv)
				}
			}
			return result, nil
		} else {
			i, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, fmt.Errorf("invalid index %q: %w", parts[0], err)
			}
			if i < 0 || i >= len(vv) {
				return nil, fmt.Errorf("index %d out of range", i)
			}
			v = vv[i]
		}
	default:
		return nil, fmt.Errorf("cannot extract path %q from %T", path, v)
	}

	if len(parts) == 1 {
		return v, nil
	}

	return ExtractPathFromAny(v, parts[1])
}
