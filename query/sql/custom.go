package sql

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/field"
)

// Type is a type of the values that a text is evaluated with, which a function is declared to
// take and to return.
type Type int

const (
	// Bool is true or false.
	Bool Type = iota + 1
	// Integer is a whole number.
	Integer
	// Real is a number that may have a fraction.
	Real
	// Number is an integer or a real: any number. A function can take it, and it is what the
	// evaluator calls the type of both.
	Number
	// Text is a string.
	Text
	// Bytes is a slice of bytes.
	Bytes
	// Time is a time.Time.
	Time
	// Any is a value of any type, which is only known when there is a record.
	Any
)

func (t Type) String() string {
	switch t {
	case Bool:
		return "Bool"
	case Integer:
		return "Integer"
	case Real:
		return "Real"
	case Number:
		return "Number"
	case Text:
		return "Text"
	case Bytes:
		return "Bytes"
	case Time:
		return "Time"
	case Any:
		return "Any"
	}
	return fmt.Sprintf("Type(%d)", int(t))
}

// kind returns the kind that the evaluator knows the type by. It is zero for a type that there
// is not.
func (t Type) kind() (k kind, ok bool) {
	switch t {
	case Bool:
		return kindBool, true
	case Integer, Real, Number:
		return kindNumber, true
	case Text:
		return kindString, true
	case Bytes:
		return kindBytes, true
	case Time:
		return kindTime, true
	case Any:
		return kindAny, true
	}
	return kindAny, false
}

// typeOf returns the Type of a Go type, if it has one: bool, the integers, the floats, string,
// []byte and time.Time, the types that are defined on them, and the empty interface for Any.
func typeOf(t reflect.Type) (Type, bool) {
	if t == reflect.TypeFor[time.Time]() {
		return Time, true
	}
	switch t.Kind() {
	case reflect.Bool:
		return Bool, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return Integer, true
	case reflect.Float32, reflect.Float64:
		return Real, true
	case reflect.String:
		return Text, true
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return Bytes, true
		}
	case reflect.Interface:
		if t.NumMethod() == 0 {
			return Any, true
		}
	}
	return 0, false
}

// Function is a function that a text can call, besides the ones that there always are: lower,
// upper, length, abs, substr, instr, trim, ltrim, rtrim, round, coalesce and ifnull. Functions
// are made with Func1, Func2, Func3 and FuncN, and given to a parser with WithFunctions.
//
// A function must be deterministic: it must return the same for the same arguments, whenever it
// is called. An index that is defined on an expression or on a condition that calls it keeps
// the results in its entries, and finds the records by them, and they are computed again to be
// removed. The results must not depend on the time, on random numbers or on anything that
// changes. If what a function computes changes, give it another version with WithVersion, so
// that the indexes that depend on it are built again.
//
// Like the functions that there always are, a function that is given a field of several values
// is applied to each of its values, and its results are the values of the result.
type Function struct {
	name     string
	args     []Type
	variadic bool
	result   Type
	version  string
	call     func(args []any) (any, error)
}

// Name returns the name of the function as it was given, which the text calls it by in any case.
func (f Function) Name() string {
	return f.name
}

// Version returns the version of the function, which is empty if it has none.
func (f Function) Version() string {
	return f.version
}

// WithVersion returns the function with a version, which says what it computes. The version is
// a part of what an index on an expression or on a condition that calls the function is
// identified by, as name@version: the index is built again when the version changes.
func (f Function) WithVersion(v string) Function {
	f.version = v
	return f
}

// Func1 makes a function of one argument. The types of the argument and of the result are those
// of A and of R: bool, the integers, the floats, string, []byte, time.Time, the types that are
// defined on them, and any (Any). It panics for a Go type that the evaluator has no Type for.
//
// The value of an argument is converted to A as field.Convert does, so that a whole number
// that is written as 5.0 can be given to an int; one that cannot be converted, like 5.5 for an
// int, is an ErrEvaluation. A function that is given no value for an argument is not called, and
// has no value as its result, like the functions that there always are.
func Func1[A, R any](name string, f func(A) R) Function {
	return typed(name, []Type{typeFor[A](name)}, typeFor[R](name), func(args []any) (any, error) {
		a, ok, err := argument[A](name, 0, args[0])
		if err != nil || !ok {
			return nil, err
		}
		return f(a), nil
	})
}

// Func2 makes a function of two arguments; see Func1.
func Func2[A, B, R any](name string, f func(A, B) R) Function {
	return typed(name, []Type{typeFor[A](name), typeFor[B](name)}, typeFor[R](name), func(args []any) (any, error) {
		a, ok, err := argument[A](name, 0, args[0])
		if err != nil || !ok {
			return nil, err
		}
		b, ok, err := argument[B](name, 1, args[1])
		if err != nil || !ok {
			return nil, err
		}
		return f(a, b), nil
	})
}

// Func3 makes a function of three arguments; see Func1.
func Func3[A, B, C, R any](name string, f func(A, B, C) R) Function {
	types := []Type{typeFor[A](name), typeFor[B](name), typeFor[C](name)}
	return typed(name, types, typeFor[R](name), func(args []any) (any, error) {
		a, ok, err := argument[A](name, 0, args[0])
		if err != nil || !ok {
			return nil, err
		}
		b, ok, err := argument[B](name, 1, args[1])
		if err != nil || !ok {
			return nil, err
		}
		c, ok, err := argument[C](name, 2, args[2])
		if err != nil || !ok {
			return nil, err
		}
		return f(a, b, c), nil
	})
}

// FuncN makes a function from the types of its arguments, for one that takes more than three or
// a different number of them, or of the types that the Go ones do not have, like Number. If
// variadic is set the last type is the type of the last argument and of any number of arguments
// after it, so the function takes the number of types that there are or more. The function is
// given the values as the evaluator has them: nil for no value, an int64 or a float64 for a
// number, a string, a bool, a []byte, a time.Time. It returns one of those (or nil for no
// value) of the type that it is declared to return, or an error, which makes the evaluation of
// the text fail with ErrEvaluation.
//
// The function is called when an argument has no value, unlike the ones that Func1, Func2 and
// Func3 make, so that it can tell what to do about it. It panics if there is no type for a
// variadic function, or if f is nil.
func FuncN(name string, args []Type, variadic bool, result Type, f func(args []any) (any, error)) Function {
	if f == nil {
		panic(fmt.Sprintf("sql: the function %s is nil", name))
	}
	if variadic && len(args) == 0 {
		panic(fmt.Sprintf("sql: the function %s is variadic and has no argument type to repeat", name))
	}
	for _, t := range append(append([]Type(nil), args...), result) {
		if _, ok := t.kind(); !ok {
			panic(fmt.Sprintf("sql: the function %s has the type %v, which there is not", name, t))
		}
	}
	return Function{name: name, args: append([]Type(nil), args...), variadic: variadic, result: result, call: f}
}

// typed makes a function of Go types, which is not called when an argument has no value.
func typed(name string, args []Type, result Type, call func(args []any) (any, error)) Function {
	return FuncN(name, args, false, result, call)
}

// typeFor returns the Type of a Go type, or panics.
func typeFor[T any](name string) Type {
	t := reflect.TypeFor[T]()
	typ, ok := typeOf(t)
	if !ok {
		panic(fmt.Sprintf("sql: the function %s takes or returns a %s, which the evaluator has no type for", name, t))
	}
	return typ
}

// argument converts the value of an argument to the Go type that the function takes. It reports
// false for no value.
func argument[T any](name string, at int, value any) (converted T, ok bool, err error) {
	if value == nil {
		return converted, false, nil
	}
	t := reflect.TypeFor[T]()
	if t.Kind() == reflect.Interface {
		return value.(T), true, nil
	}
	c, err := field.Convert(value, t)
	if err != nil {
		return converted, false, fmt.Errorf("%w: argument %d of %s: %v", ErrEvaluation, at+1, name, err)
	}
	return c.(T), true, nil
}

// Functions is a set of functions that a text can call. It is a value that is not changed by
// what is done with it: Add returns another set. The zero value is the empty set.
type Functions struct {
	byName map[string]Function
}

// Add returns the set with a function more. The names of functions are not case sensitive, like
// the names of the functions that there always are. It panics if the name is empty, if the set
// has a function of that name, or if it is the name of one of the functions that there always
// are: such mistakes are in the definition of the program.
func (fs Functions) Add(f Function) Functions {
	name := strings.ToLower(f.name)
	switch {
	case f.call == nil || name == "":
		panic("sql: a function must be made with Func1, Func2, Func3 or FuncN, and have a name")
	case functions[name] != nil:
		panic(fmt.Sprintf("sql: %s is the name of a function that there always is", name))
	}
	if _, ok := fs.byName[name]; ok {
		panic(fmt.Sprintf("sql: the set has a function %s already", name))
	}

	byName := make(map[string]Function, len(fs.byName)+1)
	for k, v := range fs.byName {
		byName[k] = v
	}
	byName[name] = f
	return Functions{byName: byName}
}

// WithFunctions gives the parser functions that its texts can call, besides the ones that there
// always are. The parser does not see the functions of any other parser. It replaces the
// functions that an option before it gave.
//
// It does not depend on the type of the records, so it is a plain value that every parser takes.
func WithFunctions(fs Functions) FunctionsOption {
	return FunctionsOption{fs: fs}
}

// FunctionsOption is the option that WithFunctions returns. It is an Option of any type of records.
type FunctionsOption struct{ fs Functions }

func (o FunctionsOption) apply(t optionTarget) { t.setFunctions(o.fs) }

// internal returns the function as the evaluator has the ones that there always are.
func (f Function) internal() *function {
	accepts := make([]kindSet, len(f.args))
	for i, t := range f.args {
		accepts[i] = acceptedBy(t)
	}
	result, _ := f.result.kind()

	fn := &function{min: len(f.args), max: len(f.args), accepts: accepts, result: fixed(result), lifted: true}
	if f.variadic {
		fn.max = -1
	}
	fn.call = func(args []any) any {
		value, err := f.call(args)
		if err != nil {
			panic(failure{fmt.Errorf("%w: function %s: %w", ErrEvaluation, f.name, err)})
		}
		return f.checked(value)
	}
	if len(accepts) == 0 {
		fn.accepts = []kindSet{^kindSet(0)}
	}
	return fn
}

// acceptedBy returns the kinds that an argument of a type can be.
func acceptedBy(t Type) kindSet {
	k, _ := t.kind()
	if k == kindAny {
		return ^kindSet(0)
	}
	return setOf(k)
}

// checked returns the result of a function in the form that the evaluator has, and fails if it
// is not of the type that the function is declared to return.
func (f Function) checked(value any) any {
	x := plain(reflect.ValueOf(value))
	if x == nil {
		return nil
	}
	want, _ := f.result.kind()
	var got kind
	switch x.(type) {
	case bool:
		got = kindBool
	case int64, float64:
		got = kindNumber
	case string:
		got = kindString
	case []byte:
		got = kindBytes
	case time.Time:
		got = kindTime
	default:
		// A list or a value of a type that the evaluator knows nothing of.
		got = kindNull
	}
	if got == kindNull || (want != kindAny && got != want) {
		panic(failure{fmt.Errorf("%w: function %s returned %T, which is not the %v that it is declared to return",
			ErrEvaluation, f.name, value, f.result)})
	}
	return x
}

// failure is the panic of an evaluation that fails, which the evaluator has no other way to
// return from the depth that the function is called at. It is always recovered by whoever
// started the evaluation, and turned into the error that it carries.
type failure struct {
	err error
}

// recoverFailure is deferred by whoever evaluates a text, to return the error of a function
// that fails. A panic of any other kind is not hidden.
func recoverFailure(err *error) {
	if r := recover(); r != nil {
		f, ok := r.(failure)
		if !ok {
			panic(r)
		}
		*err = f.err
	}
}
