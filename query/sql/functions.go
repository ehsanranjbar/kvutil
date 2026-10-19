package sql

import (
	"math"
	"strings"
	"unicode/utf8"
)

// kindSet is a set of kinds. What is not known to be of any kind is in every set.
type kindSet uint16

func setOf(kinds ...kind) kindSet {
	var s kindSet
	for _, k := range kinds {
		s |= 1 << k
	}
	return s
}

func (s kindSet) has(k kind) bool {
	return k.open() || s&(1<<k) != 0
}

// function is a function that a text can call.
type function struct {
	// min and max are the numbers of arguments that it takes. The last of them may be repeated
	// if max is -1.
	min, max int
	// accepts is the kinds that each argument may be, the last for all the ones after it.
	accepts []kindSet
	// result returns the kind of the result, from the kinds of the arguments.
	result func(args []kind) kind
	// call returns the result for the values of the arguments.
	call func(args []any) any
	// lifted is set if the function is applied to each value of an argument that is a list.
	lifted bool
}

func fixed(k kind) func([]kind) kind {
	return func([]kind) kind { return k }
}

// unify returns the kind that all of the kinds are, or any if they differ. NULL is all of them.
func unify(kinds ...kind) kind {
	result := kindNull
	for _, k := range kinds {
		switch {
		case k == kindNull:
		case result == kindNull:
			result = k
		case result != k:
			return kindAny
		}
	}
	return result
}

func unified(args []kind) kind {
	return unify(args...)
}

var (
	textOnly   = setOf(kindString)
	numberOnly = setOf(kindNumber)
)

// functions are the functions that a text can call, by their names in lower case.
var functions = map[string]*function{
	"lower": {
		min: 1, max: 1, accepts: []kindSet{textOnly}, lifted: true,
		result: fixed(kindString),
		call: func(args []any) any {
			if s, ok := args[0].(string); ok {
				return strings.ToLower(s)
			}
			return nil
		},
	},
	"upper": {
		min: 1, max: 1, accepts: []kindSet{textOnly}, lifted: true,
		result: fixed(kindString),
		call: func(args []any) any {
			if s, ok := args[0].(string); ok {
				return strings.ToUpper(s)
			}
			return nil
		},
	},
	// The length of a field of several values is the number of its values.
	"length": {
		min: 1, max: 1, accepts: []kindSet{setOf(kindString, kindBytes, kindNumber)},
		result: fixed(kindNumber),
		call: func(args []any) any {
			switch x := args[0].(type) {
			case list:
				return int64(len(x))
			case string:
				return int64(utf8.RuneCountInString(x))
			case []byte:
				return int64(len(x))
			case opaque:
				if x.size >= 0 {
					return int64(x.size)
				}
			case int64, float64:
				s, _ := text(x)
				return int64(len(s))
			}
			return nil
		},
	},
	"abs": {
		min: 1, max: 1, accepts: []kindSet{numberOnly}, lifted: true,
		result: fixed(kindNumber),
		call: func(args []any) any {
			switch x := args[0].(type) {
			case int64:
				switch {
				case x == math.MinInt64:
					return -float64(x)
				case x < 0:
					return -x
				}
				return x
			case float64:
				return math.Abs(x)
			}
			return nil
		},
	},
	"substr": {
		min: 2, max: 3, accepts: []kindSet{textOnly, numberOnly, numberOnly}, lifted: true,
		result: fixed(kindString),
		call: func(args []any) any {
			s, ok := args[0].(string)
			start, okStart := integer(args[1])
			if !ok || !okStart {
				return nil
			}
			if len(args) == 2 {
				return substr(s, start, 0, false)
			}
			length, okLength := integer(args[2])
			if !okLength {
				return nil
			}
			return substr(s, start, length, true)
		},
	},
	"instr": {
		min: 2, max: 2, accepts: []kindSet{textOnly}, lifted: true,
		result: fixed(kindNumber),
		call: func(args []any) any {
			s, ok := args[0].(string)
			sub, okSub := args[1].(string)
			if !ok || !okSub {
				return nil
			}
			return instr(s, sub)
		},
	},
	"trim":  trimmer(strings.Trim),
	"ltrim": trimmer(strings.TrimLeft),
	"rtrim": trimmer(strings.TrimRight),
	"round": {
		min: 1, max: 2, accepts: []kindSet{numberOnly}, lifted: true,
		result: fixed(kindNumber),
		call: func(args []any) any {
			var x float64
			switch n := args[0].(type) {
			case int64:
				x = float64(n)
			case float64:
				x = n
			default:
				return nil
			}
			digits := int64(0)
			if len(args) == 2 {
				d, ok := integer(args[1])
				if !ok {
					return nil
				}
				digits = d
			}
			return round(x, digits)
		},
	},
	"coalesce": {
		min: 2, max: -1, accepts: []kindSet{^kindSet(0)},
		result: unified,
		call:   coalesce,
	},
	"ifnull": {
		min: 2, max: 2, accepts: []kindSet{^kindSet(0)},
		result: unified,
		call:   coalesce,
	},
}

// trimmer returns trim, ltrim or rtrim, which take away the characters of a set (spaces by
// default) from the ends of a text.
func trimmer(trim func(s, cutset string) string) *function {
	return &function{
		min: 1, max: 2, accepts: []kindSet{textOnly}, lifted: true,
		result: fixed(kindString),
		call: func(args []any) any {
			s, ok := args[0].(string)
			if !ok {
				return nil
			}
			cutset := " "
			if len(args) == 2 {
				if cutset, ok = args[1].(string); !ok {
					return nil
				}
			}
			return trim(s, cutset)
		},
	}
}

// coalesce returns the first value that there is.
func coalesce(args []any) any {
	for _, arg := range args {
		if !absent(arg) {
			return arg
		}
	}
	return nil
}

// integer returns a number as an integer, if it is one that the function can take.
func integer(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, false
		}
		switch {
		case x >= 9.2e18:
			return math.MaxInt64, true
		case x <= -9.2e18:
			return math.MinInt64, true
		}
		return int64(x), true
	}
	return 0, false
}
