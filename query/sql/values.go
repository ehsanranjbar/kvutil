package sql

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// kind is the type that a part of a text is known to have before there is a record.
type kind uint8

const (
	// kindAny is the type of what can only be known on a record: a field of an interface type.
	kindAny kind = iota
	// kindNull is the type of NULL, which goes where any type does.
	kindNull
	kindBool
	kindNumber
	kindString
	kindBytes
	kindTime
)

func (k kind) String() string {
	switch k {
	case kindNull:
		return "NULL"
	case kindBool:
		return "a boolean"
	case kindNumber:
		return "a number"
	case kindString:
		return "a string"
	case kindBytes:
		return "bytes"
	case kindTime:
		return "a time"
	}
	return "a value of any type"
}

// open reports whether a kind is one that is not known to be any other.
func (k kind) open() bool {
	return k == kindAny || k == kindNull
}

// kindOfType returns the kind of the values of a Go type.
func kindOfType(t reflect.Type) kind {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeFor[time.Time]() {
		return kindTime
	}
	switch t.Kind() {
	case reflect.Bool:
		return kindBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return kindNumber
	case reflect.String:
		return kindString
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return kindBytes
		}
		return kindOfType(t.Elem())
	case reflect.Array:
		if t.Implements(reflect.TypeFor[fmt.Stringer]()) {
			return kindString
		}
		return kindOfType(t.Elem())
	}
	return kindAny
}

// The values that the evaluator works with are nil (no value), int64, float64, string, bool,
// []byte, time.Time, a list of them for a field of several values, and an opaque value for
// what has no other form.

// list is the value of a field of several values, or what is made of such a value.
type list []any

// opaque is the value of a field of a type that the evaluator knows nothing of, like a map or
// a struct. A record has it, and nothing can be done with it, but to count what it has.
type opaque struct {
	size int
}

// plain returns a Go value in a form that the evaluator knows: a value of a named type as one of
// the type that it is defined on, and a slice as a list of such values. It is nil for no value,
// which is the case behind a pointer that is nil.
func plain(rv reflect.Value) any {
	for rv.IsValid() && (rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface) {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil
	}
	if rv.Type() == reflect.TypeFor[time.Time]() {
		return rv.Interface()
	}

	switch rv.Kind() {
	case reflect.String:
		return rv.String()
	case reflect.Bool:
		return rv.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if u := rv.Uint(); u > math.MaxInt64 {
			return float64(u)
		}
		return int64(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return rv.Float()

	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return bytes.Clone(rv.Bytes())
		}
		return plainList(rv)

	case reflect.Array:
		// An array of bytes, like a UUID, is its text if it has one.
		if rv.CanInterface() {
			if s, ok := rv.Interface().(fmt.Stringer); ok {
				return s.String()
			}
		}
		return plainList(rv)

	case reflect.Map:
		if rv.IsNil() {
			return nil
		}
		return opaque{size: rv.Len()}
	}
	return opaque{size: -1}
}

// plainList returns the elements of a slice or of an array as a list of the values that plain
// returns, without the ones that are no value.
func plainList(rv reflect.Value) list {
	l := make(list, 0, rv.Len())
	for i := range rv.Len() {
		if x := plain(rv.Index(i)); x != nil {
			l = append(l, x)
		}
	}
	return l
}

// absent reports whether there is no value: none, or a list that has none.
func absent(v any) bool {
	if v == nil {
		return true
	}
	l, ok := v.(list)
	return ok && len(l) == 0
}

// truth returns the value of a condition: true if it is true, and false for anything else.
func truth(v any) bool {
	b, _ := v.(bool)
	return b
}

// compare compares two values. It reports false for two that cannot be compared: one that is
// no value, two of different types, and a number that is not one.
func compare(a, b any) (int, bool) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return cmpOrdered(x, y), true
		case float64:
			return compareFloats(float64(x), y)
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return compareFloats(x, float64(y))
		case float64:
			return compareFloats(x, y)
		}
	case string:
		switch y := b.(type) {
		case string:
			return strings.Compare(x, y), true
		case []byte:
			return bytes.Compare([]byte(x), y), true
		}
	case []byte:
		switch y := b.(type) {
		case []byte:
			return bytes.Compare(x, y), true
		case string:
			return bytes.Compare(x, []byte(y)), true
		}
	case bool:
		if y, ok := b.(bool); ok {
			switch {
			case x == y:
				return 0, true
			case !x:
				return -1, true
			}
			return 1, true
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y), true
		}
	}
	return 0, false
}

func cmpOrdered[T int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func compareFloats(a, b float64) (int, bool) {
	if math.IsNaN(a) || math.IsNaN(b) {
		return 0, false
	}
	return cmpOrdered(a, b), true
}

// holds reports whether one value of a and one of b are in the relation that test says of
// their comparison. A value that is no value is in none.
func holds(a, b any, test func(int) bool) bool {
	if l, ok := a.(list); ok {
		for _, x := range l {
			if holds(x, b, test) {
				return true
			}
		}
		return false
	}
	if l, ok := b.(list); ok {
		for _, y := range l {
			if holds(a, y, test) {
				return true
			}
		}
		return false
	}
	c, ok := compare(a, b)
	return ok && test(c)
}

// anyOf reports whether the function is true of a value, or of one of the values of a list.
func anyOf(v any, f func(any) bool) bool {
	if l, ok := v.(list); ok {
		for _, x := range l {
			if anyOf(x, f) {
				return true
			}
		}
		return false
	}
	return v != nil && f(v)
}

// lift applies a function to the values of its arguments. If one of them is a list the function
// is applied to each of its values, and the result is the list of what it returns; if several
// are, there is no result.
func lift(args []any, f func(args []any) any) any {
	at := -1
	for i, arg := range args {
		if _, ok := arg.(list); ok {
			if at >= 0 {
				return nil
			}
			at = i
		}
	}
	if at < 0 {
		return f(args)
	}

	values := args[at].(list)
	result := make(list, 0, len(values))
	call := make([]any, len(args))
	copy(call, args)
	for _, value := range values {
		call[at] = value
		if r := f(call); r != nil {
			result = append(result, r)
		}
	}
	return result
}

// arithmetic applies one of + - * / % to two values. It has no result for what is not numbers,
// and for a division by zero.
func arithmetic(op byte, a, b any) any {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return integerArithmetic(op, x, y)
		case float64:
			return floatArithmetic(op, float64(x), y)
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return floatArithmetic(op, x, float64(y))
		case float64:
			return floatArithmetic(op, x, y)
		}
	}
	return nil
}

// integerArithmetic is arithmetic on integers, which are floats where they would overflow.
func integerArithmetic(op byte, a, b int64) any {
	switch op {
	case '+':
		if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
			return float64(a) + float64(b)
		}
		return a + b
	case '-':
		if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
			return float64(a) - float64(b)
		}
		return a - b
	case '*':
		if a == 0 || b == 0 {
			return int64(0)
		}
		if p := a * b; p/b == a && !(a == -1 && b == math.MinInt64) && !(b == -1 && a == math.MinInt64) {
			return p
		}
		return float64(a) * float64(b)
	case '/':
		switch {
		case b == 0:
			return nil
		case a == math.MinInt64 && b == -1:
			return -float64(a)
		}
		return a / b
	case '%':
		switch {
		case b == 0:
			return nil
		case b == -1:
			return int64(0)
		}
		return a % b
	}
	return nil
}

func floatArithmetic(op byte, a, b float64) any {
	switch op {
	case '+':
		return a + b
	case '-':
		return a - b
	case '*':
		return a * b
	case '/':
		if b == 0 {
			return nil
		}
		return a / b
	case '%':
		if b == 0 {
			return nil
		}
		return math.Mod(a, b)
	}
	return nil
}

// text returns a value as text, if it is text or a number.
func text(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	}
	return "", false
}

// concat joins the texts of two values.
func concat(a, b any) any {
	x, ok := text(a)
	if !ok {
		return nil
	}
	y, ok := text(b)
	if !ok {
		return nil
	}
	return x + y
}

// An element of a pattern.
type patternKind uint8

const (
	patternLiteral patternKind = iota
	patternOne
	patternMany
)

type patternElement struct {
	kind patternKind
	r    rune
}

// pattern is the pattern of a LIKE or of a GLOB.
type pattern struct {
	elements []patternElement
	// fold is set if ASCII letters are the same in either case.
	fold bool
}

// newPattern makes the pattern of a text where many and one are the characters that stand for
// any characters and for any one character, and where the escape character, if there is one,
// makes the next character stand for itself.
func newPattern(s string, many, one rune, escape rune, hasEscape, fold bool) *pattern {
	p := &pattern{fold: fold}
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case hasEscape && r == escape && i+1 < len(runes):
			i++
			p.elements = append(p.elements, patternElement{patternLiteral, runes[i]})
		case r == many:
			// Several in a row are one.
			if n := len(p.elements); n == 0 || p.elements[n-1].kind != patternMany {
				p.elements = append(p.elements, patternElement{kind: patternMany})
			}
		case r == one:
			p.elements = append(p.elements, patternElement{kind: patternOne})
		default:
			p.elements = append(p.elements, patternElement{patternLiteral, r})
		}
	}
	return p
}

func (p *pattern) same(a, b rune) bool {
	if a == b {
		return true
	}
	return p.fold && foldASCII(a) == foldASCII(b)
}

func foldASCII(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + 'a' - 'A'
	}
	return r
}

// matches reports whether a text matches the pattern.
func (p *pattern) matches(s string) bool {
	runes := []rune(s)
	var (
		pi, si     int
		star, mark = -1, 0
	)
	for si < len(runes) {
		switch {
		case pi < len(p.elements) && (p.elements[pi].kind == patternOne ||
			(p.elements[pi].kind == patternLiteral && p.same(p.elements[pi].r, runes[si]))):
			pi++
			si++
		case pi < len(p.elements) && p.elements[pi].kind == patternMany:
			star, mark = pi, si
			pi++
		case star >= 0:
			mark++
			pi, si = star+1, mark
		default:
			return false
		}
	}
	for pi < len(p.elements) && p.elements[pi].kind == patternMany {
		pi++
	}
	return pi == len(p.elements)
}

// substr is the function of SQLite: the start is from 1, from the end if it is negative, and
// the length, if it is negative, counts the characters before the start.
func substr(s string, start int64, length int64, hasLength bool) string {
	runes := []rune(s)
	n := int64(len(runes))

	p1, p2 := start, int64(math.MaxInt32)
	negative := false
	if hasLength {
		p2 = length
		if p2 < 0 {
			p2, negative = -p2, true
		}
	}
	switch {
	case p1 < 0:
		p1 += n
		if p1 < 0 {
			p2 += p1
			if p2 < 0 {
				p2 = 0
			}
			p1 = 0
		}
	case p1 > 0:
		p1--
	case p2 > 0:
		p2--
	}
	if negative {
		p1 -= p2
		if p1 < 0 {
			p2 += p1
			p1 = 0
		}
	}
	p1 = max(0, min(p1, n))
	p2 = max(0, min(p2, n-p1))
	return string(runes[p1 : p1+p2])
}

// instr is the position of the first character of a text in another, from 1, or 0.
func instr(s, sub string) int64 {
	i := strings.Index(s, sub)
	if i < 0 {
		return 0
	}
	return int64(utf8.RuneCountInString(s[:i])) + 1
}

// round rounds a number to a number of digits, away from zero for a half, as SQLite does.
func round(x float64, digits int64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return x
	}
	digits = max(0, min(digits, 30))
	scale := math.Pow(10, float64(digits))
	if r := math.Round(x*scale) / scale; !math.IsInf(r, 0) && !math.IsNaN(r) {
		return r
	}
	return x
}
