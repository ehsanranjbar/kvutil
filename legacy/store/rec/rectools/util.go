package rectools

func findAs[T any](anys []any) []T {
	var ts []T
	for _, a := range anys {
		if t, ok := a.(T); ok {
			ts = append(ts, t)
		}
	}

	return ts
}

func findOneAs[T any](anys []any) (t T, ok bool) {
	for _, a := range anys {
		if t, ok = a.(T); ok {
			return t, true
		}
	}

	return t, false
}
