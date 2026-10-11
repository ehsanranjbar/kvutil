package agg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCalendarBuckets(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	date := func(loc *time.Location, y int, m time.Month, d, h, min int) time.Time {
		return time.Date(y, m, d, h, min, 0, 0, loc)
	}

	tests := []struct {
		name   string
		bucket Bucket
		at     time.Time
		start  time.Time
		next   time.Time
	}{
		{"Hourly", Hourly, date(time.UTC, 2026, 3, 8, 14, 37), date(time.UTC, 2026, 3, 8, 14, 0), date(time.UTC, 2026, 3, 8, 15, 0)},
		{"Daily", Daily, date(time.UTC, 2026, 3, 8, 14, 37), date(time.UTC, 2026, 3, 8, 0, 0), date(time.UTC, 2026, 3, 9, 0, 0)},
		{"DailyAtItsStart", Daily, date(time.UTC, 2026, 3, 8, 0, 0), date(time.UTC, 2026, 3, 8, 0, 0), date(time.UTC, 2026, 3, 9, 0, 0)},
		// 8 March 2026 is a Sunday.
		{"Weekly", Weekly, date(time.UTC, 2026, 3, 8, 14, 37), date(time.UTC, 2026, 3, 2, 0, 0), date(time.UTC, 2026, 3, 9, 0, 0)},
		{"WeeklyOnItsFirstDay", Weekly, date(time.UTC, 2026, 3, 9, 0, 0), date(time.UTC, 2026, 3, 9, 0, 0), date(time.UTC, 2026, 3, 16, 0, 0)},
		{"WeeklyFromSunday", Weekly.StartingOn(time.Sunday), date(time.UTC, 2026, 3, 8, 14, 37), date(time.UTC, 2026, 3, 8, 0, 0), date(time.UTC, 2026, 3, 15, 0, 0)},
		{"WeeklyOverAYear", Weekly, date(time.UTC, 2026, 1, 1, 12, 0), date(time.UTC, 2025, 12, 29, 0, 0), date(time.UTC, 2026, 1, 5, 0, 0)},
		{"Monthly", Monthly, date(time.UTC, 2026, 1, 31, 23, 59), date(time.UTC, 2026, 1, 1, 0, 0), date(time.UTC, 2026, 2, 1, 0, 0)},
		{"MonthlyInDecember", Monthly, date(time.UTC, 2026, 12, 31, 23, 59), date(time.UTC, 2026, 12, 1, 0, 0), date(time.UTC, 2027, 1, 1, 0, 0)},
		{"MonthlyInALeapYear", Monthly, date(time.UTC, 2028, 2, 29, 12, 0), date(time.UTC, 2028, 2, 1, 0, 0), date(time.UTC, 2028, 3, 1, 0, 0)},
		{"Yearly", Yearly, date(time.UTC, 2026, 12, 31, 23, 59), date(time.UTC, 2026, 1, 1, 0, 0), date(time.UTC, 2027, 1, 1, 0, 0)},

		// A time belongs to the day that it is in where the bucket is, not in UTC.
		{"DailyInAZone", Daily.In(ny), date(time.UTC, 2026, 6, 10, 2, 0), date(ny, 2026, 6, 9, 0, 0), date(ny, 2026, 6, 10, 0, 0)},
		{"MonthlyInAZone", Monthly.In(ny), date(time.UTC, 2026, 7, 1, 2, 0), date(ny, 2026, 6, 1, 0, 0), date(ny, 2026, 7, 1, 0, 0)},
		// The clocks go forward on 8 March 2026, which makes the day 23 hours long.
		{"DayOf23Hours", Daily.In(ny), date(ny, 2026, 3, 8, 12, 0), date(ny, 2026, 3, 8, 0, 0), date(ny, 2026, 3, 9, 0, 0)},
		// The clocks go back on 1 November 2026, which makes the day 25 hours long.
		{"DayOf25Hours", Daily.In(ny), date(ny, 2026, 11, 1, 12, 0), date(ny, 2026, 11, 1, 0, 0), date(ny, 2026, 11, 2, 0, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := test.bucket.Start(test.at)
			require.True(t, test.start.Equal(start), "start: want %s, got %s", test.start, start)
			next := test.bucket.Next(start)
			require.True(t, test.next.Equal(next), "next: want %s, got %s", test.next, next)

			// The start of a span is in the span, and the time right before the next one is too.
			require.True(t, start.Equal(test.bucket.Start(start)))
			require.True(t, start.Equal(test.bucket.Start(next.Add(-time.Nanosecond))))
			require.True(t, next.Equal(test.bucket.Start(next)))
		})
	}

	require.Equal(t, 23*time.Hour, Daily.In(ny).Next(date(ny, 2026, 3, 8, 0, 0)).Sub(date(ny, 2026, 3, 8, 0, 0)))
	require.Equal(t, 25*time.Hour, Daily.In(ny).Next(date(ny, 2026, 11, 1, 0, 0)).Sub(date(ny, 2026, 11, 1, 0, 0)))

	// The hour that the day has twice is two spans of an hour.
	first := date(ny, 2026, 11, 1, 1, 30)
	second := first.Add(time.Hour)
	require.Equal(t, first.Hour(), second.Hour())
	hourly := Hourly.In(ny)
	require.Equal(t, time.Hour, hourly.Start(second).Sub(hourly.Start(first)))
	require.True(t, hourly.Next(hourly.Start(first)).Equal(hourly.Start(second)))
}

func TestOtherBuckets(t *testing.T) {
	at := time.Date(2026, 3, 8, 14, 37, 12, 0, time.UTC)

	quarter := Every(15 * time.Minute)
	require.True(t, time.Date(2026, 3, 8, 14, 30, 0, 0, time.UTC).Equal(quarter.Start(at)))
	require.True(t, time.Date(2026, 3, 8, 14, 45, 0, 0, time.UTC).Equal(quarter.Next(quarter.Start(at))))
	require.Panics(t, func() { Every(0) })

	require.True(t, AllTime.Start(at).IsZero())
	require.True(t, AllTime.Start(at.AddDate(100, 0, 0)).IsZero())

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	names := map[Bucket]string{
		AllTime:                        "all",
		Hourly:                         "hourly",
		Daily:                          "daily",
		Weekly:                         "weekly(monday)",
		Weekly.StartingOn(time.Sunday): "weekly(sunday)",
		Monthly:                        "monthly",
		Yearly:                         "yearly",
		Daily.In(ny):                   "daily@America/New_York",
		quarter:                        "every(15m0s)",
	}
	ids := map[byte]bool{}
	for b, name := range names {
		require.Equal(t, name, b.Name())
		ids[b.ID()] = true
	}
	// The buckets of different sizes have different ids.
	require.Len(t, ids, 7)
}

func TestNests(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	tests := []struct {
		fine, coarse Bucket
		want         bool
	}{
		{Hourly, Daily, true},
		{Hourly, Weekly, true},
		{Hourly, Yearly, true},
		{Daily, Weekly, true},
		{Daily, Monthly, true},
		{Daily, Yearly, true},
		{Monthly, Yearly, true},
		{Daily, AllTime, true},
		{Every(time.Minute), AllTime, true},
		{Every(time.Minute), Every(15 * time.Minute), true},

		// A week is not inside a month or a year.
		{Weekly, Monthly, false},
		{Weekly, Yearly, false},
		{Daily, Hourly, false},
		{Daily, Daily, false},
		{Monthly, Weekly, false},
		{Daily.In(ny), Monthly, false},
		{Daily.In(ny), Monthly.In(ny), true},
		{Every(10 * time.Minute), Every(15 * time.Minute), false},
		{Every(time.Hour), Daily, false},
		{Daily, Every(48 * time.Hour), false},
		{AllTime, Daily, false},
	}
	for _, test := range tests {
		require.Equal(t, test.want, nests(test.fine, test.coarse), "%s in %s", test.fine.Name(), test.coarse.Name())
	}
}
