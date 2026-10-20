package harness

// Levels returns the concurrency levels that a case runs at: one worker, and the run's
// concurrency if it is more.
func Levels(concurrency int) []int {
	if concurrency <= 1 {
		return []int{1}
	}
	return []int{1, concurrency}
}
