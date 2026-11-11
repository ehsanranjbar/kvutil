//go:build race

package query_test

// raceEnabled says that the tests run with the race detector, which makes the oracles about ten
// times slower.
const raceEnabled = true
