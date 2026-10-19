package field

import (
	"encoding"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
)

// Untyped is a field whatever the type of its values is, for whoever has the values that it
// is compared with as they come and not as values of its type, like the numbers and the
// strings of the text of a query. The values are converted as Convert does.
// It is implemented by Field.
type Untyped[V any] interface {
	Component[V]
	// Type returns the type of the values of the field.
	Type() reflect.Type
	// AnyValues returns the values that a record has for the field.
	AnyValues(v *V) []any
	// EqAny returns the condition of the field being equal to a value.
	EqAny(value any) (*cond.Comparison[V], error)
	// InAny returns the condition of the field being equal to one of the values.
	InAny(values ...any) (*cond.Comparison[V], error)
	// RangeAny returns the condition of the field being in a range.
	RangeAny(r kvutil.Range[any]) (*cond.Comparison[V], error)
}

// AnyValues implements the Untyped interface.
func (f Field[V, F]) AnyValues(v *V) []any {
	values := f.Values(v)
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

// convert converts a value to the type of the values of the field.
func (f Field[V, F]) convert(value any) (converted F, err error) {
	c, err := Convert(value, f.typ)
	if err != nil {
		return converted, fmt.Errorf("field %s: %w", f.path, err)
	}
	return c.(F), nil
}

// EqAny implements the Untyped interface.
func (f Field[V, F]) EqAny(value any) (*cond.Comparison[V], error) {
	converted, err := f.convert(value)
	if err != nil {
		return nil, err
	}
	c := f.Eq(converted)
	return c, c.Err()
}

// InAny implements the Untyped interface.
func (f Field[V, F]) InAny(values ...any) (*cond.Comparison[V], error) {
	converted := make([]F, 0, len(values))
	for _, value := range values {
		c, err := f.convert(value)
		if err != nil {
			return nil, err
		}
		converted = append(converted, c)
	}
	c := f.In(converted...)
	return c, c.Err()
}

// RangeAny implements the Untyped interface.
func (f Field[V, F]) RangeAny(r kvutil.Range[any]) (*cond.Comparison[V], error) {
	converted := kvutil.All[F]()
	if low, exclusive, ok := r.Low(); ok {
		c, err := f.convert(low)
		if err != nil {
			return nil, err
		}
		if exclusive {
			converted = converted.After(c)
		} else {
			converted = converted.From(c)
		}
	}
	if high, exclusive, ok := r.High(); ok {
		c, err := f.convert(high)
		if err != nil {
			return nil, err
		}
		if exclusive {
			converted = converted.Until(c)
		} else {
			converted = converted.Through(c)
		}
	}
	c := f.Range(converted)
	return c, c.Err()
}

// timeLayouts are the forms that a time can be written in as text.
var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
}

var textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()

// Convert converts a value to a type, as a value that is written in text is converted to the
// type of the field that it is compared with. Only what cannot change the value is converted:
//
//   - a value of the type itself, or of a type that is defined on the same type, as it is;
//   - a number to a type of integers that has it, so 1965.0 is an integer, and 1965.5 and
//     300 as an int8 are not;
//   - a number to a type of floating-point numbers, as the nearest one that the type has;
//   - a string that is a number to a number, and "true" or "false" to a boolean;
//   - a string to a time, as a date or as a date and a time, which is in UTC if it has no zone;
//   - a string to a slice of bytes with the same bytes, and to an array of bytes that it is
//     the hexadecimal form of;
//   - a string to a type that reads itself from text, like a UUID.
//
// Anything else is an error.
func Convert(value any, to reflect.Type) (any, error) {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, fmt.Errorf("cannot use nil as a value of the type %s", to)
	}
	if rv.Type() == to {
		return value, nil
	}

	fail := func(reason string) (any, error) {
		if reason != "" {
			reason = ": " + reason
		}
		return nil, fmt.Errorf("cannot use %#v as a value of the type %s%s", value, to, reason)
	}
	text, isText := "", rv.Kind() == reflect.String
	if isText {
		text = rv.String()
	}

	switch {
	case to == reflect.TypeFor[time.Time]():
		if !isText {
			return fail("")
		}
		for _, layout := range timeLayouts {
			if t, err := time.Parse(layout, text); err == nil {
				return t, nil
			}
		}
		return fail("it is not a date, or a date and a time")

	case isText && reflect.PointerTo(to).Implements(textUnmarshalerType):
		p := reflect.New(to)
		if err := p.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(text)); err != nil {
			return fail(err.Error())
		}
		return p.Elem().Interface(), nil
	}

	result := reflect.New(to).Elem()
	switch to.Kind() {
	case reflect.String:
		if !isText {
			return fail("")
		}
		result.SetString(text)

	case reflect.Bool:
		switch {
		case rv.Kind() == reflect.Bool:
			result.SetBool(rv.Bool())
		case text == "true" || text == "false":
			result.SetBool(text == "true")
		default:
			return fail("")
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, ok := toInt(rv)
		if !ok {
			return fail("it is not a whole number that the type has")
		}
		if result.OverflowInt(i) {
			return fail("it is out of the range of the type")
		}
		result.SetInt(i)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u, ok := toUint(rv)
		if !ok {
			return fail("it is not a whole number that the type has")
		}
		if result.OverflowUint(u) {
			return fail("it is out of the range of the type")
		}
		result.SetUint(u)

	case reflect.Float32, reflect.Float64:
		f, ok := toFloat(rv)
		if !ok {
			return fail("it is not a number")
		}
		if result.OverflowFloat(f) {
			return fail("it is out of the range of the type")
		}
		result.SetFloat(f)

	case reflect.Slice:
		switch {
		case to.Elem().Kind() != reflect.Uint8:
			return fail("")
		case isText:
			result.SetBytes([]byte(text))
		case rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8:
			result.SetBytes(rv.Bytes())
		default:
			return fail("")
		}

	case reflect.Array:
		if to.Elem().Kind() != reflect.Uint8 || !isText {
			return fail("")
		}
		bz, err := hex.DecodeString(text)
		if err != nil || len(bz) != to.Len() {
			return fail(fmt.Sprintf("it is not %d bytes in hexadecimal", to.Len()))
		}
		reflect.Copy(result, reflect.ValueOf(bz))

	default:
		return fail("")
	}
	return result.Interface(), nil
}

// toInt returns a value as an integer, if it is one: a number without a fraction, or the
// text of one.
func toInt(rv reflect.Value) (int64, bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		return int64(u), u <= math.MaxInt64
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		// The numbers from 2^63 on do not fit, and 2^63 itself is what the conversion of a
		// number that is too great may give.
		return int64(f), f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64
	case reflect.String:
		if i, err := strconv.ParseInt(rv.String(), 10, 64); err == nil {
			return i, true
		}
		if f, err := strconv.ParseFloat(rv.String(), 64); err == nil {
			return toInt(reflect.ValueOf(f))
		}
	}
	return 0, false
}

// toUint returns a value as an unsigned integer, if it is one.
func toUint(rv reflect.Value) (uint64, bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := rv.Int()
		return uint64(i), i >= 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return rv.Uint(), true
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		return uint64(f), f == math.Trunc(f) && f >= 0 && f < math.MaxUint64
	case reflect.String:
		if u, err := strconv.ParseUint(rv.String(), 10, 64); err == nil {
			return u, true
		}
		if f, err := strconv.ParseFloat(rv.String(), 64); err == nil {
			return toUint(reflect.ValueOf(f))
		}
	}
	return 0, false
}

// toFloat returns a value as a floating-point number, if it is a number or the text of one.
func toFloat(rv reflect.Value) (float64, bool) {
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	case reflect.String:
		f, err := strconv.ParseFloat(rv.String(), 64)
		return f, err == nil && !math.IsNaN(f)
	}
	return 0, false
}
