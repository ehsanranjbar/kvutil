package aggs

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

var placeholder = regexp.MustCompile(`\$(\d+)`)

// placeholders returns the highest $n of a statement, and fails if one of $1..$n is missing.
func placeholders(t *testing.T, stmt string) int {
	t.Helper()
	seen := map[int]bool{}
	highest := 0
	for _, m := range placeholder.FindAllStringSubmatch(stmt, -1) {
		var n int
		fmt.Sscan(m[1], &n)
		seen[n] = true
		highest = max(highest, n)
	}
	for i := 1; i <= highest; i++ {
		if !seen[i] {
			t.Errorf("$%d is missing in %s", i, stmt)
		}
	}
	return highest
}

func TestStatementsTakeTheArgumentsOfTheirQueries(t *testing.T) {
	n := namesIn("s")
	from, to := time.Unix(0, 0), time.Unix(86400, 0)
	queries := []query{
		{Kind: qSeries, Unit: day, Customer: "c", From: from, To: to},
		{Kind: qSeries, Unit: week, Customer: "c", From: from, To: to},
		{Kind: qSeries, Unit: month, Customer: "c", From: from, To: to},
		{Kind: qTotal, Customer: "c"},
		{Kind: qTop, Unit: month, From: from, To: to},
	}
	for _, src := range []source{srcRaw, srcSummary, srcCagg} {
		for _, q := range queries {
			stmt := readSQL(n, src, q)
			if got, want := placeholders(t, stmt), len(readArgs(src, q)); got != want {
				t.Errorf("source %d, query %+v: the statement takes %d arguments, the query gives %d\n%s", src, q, got, want, stmt)
			}
		}
	}
	if got := placeholders(t, insertSQL(n)); got != 5 {
		t.Errorf("insert takes %d", got)
	}
	if got := placeholders(t, updateSQL(n, false)); got != 5 {
		t.Errorf("update takes %d", got)
	}
	if got := placeholders(t, updateSQL(n, true)); got != 6 {
		t.Errorf("update on a hypertable takes %d: it needs the time of the row", got)
	}
	if got, want := placeholders(t, deleteSQL(n, false)), 1; got != want {
		t.Errorf("delete takes %d", got)
	}
	if got, want := placeholders(t, deleteSQL(n, true)), 2; got != want {
		t.Errorf("delete on a hypertable takes %d", got)
	}
}

func TestBucketsOfPostgreSQLAreInUTC(t *testing.T) {
	n := namesIn("s")
	for _, src := range []source{srcRaw, srcSummary} {
		for _, u := range []unit{day, week, month} {
			stmt := seriesSQL(n, src, u)
			if src == srcSummary && u == day {
				continue // the summary has the days already
			}
			if !strings.Contains(stmt, fmt.Sprintf("date_trunc('%s', ", u)) || !strings.Contains(stmt, ", 'UTC')") {
				t.Errorf("source %d, %s is not truncated in UTC:\n%s", src, u, stmt)
			}
		}
	}
	for _, stmt := range ddlSummary(n) {
		if strings.Contains(stmt, "date_trunc(") && !strings.Contains(stmt, "'UTC')") {
			t.Errorf("the trigger truncates without a time zone:\n%s", stmt)
		}
	}
	// A day of the summary is 24 hours, whatever the time zone of the session.
	if !strings.Contains(strings.Join(ddlSummary(n), ""), "interval '24 hours'") {
		t.Error("the trigger does not add 24 hours")
	}
	if strings.Contains(buildSummary(n), "AT TIME ZONE") || !strings.Contains(buildSummary(n), "'UTC'") {
		t.Errorf("summary build: %s", buildSummary(n))
	}
}

func TestContinuousAggregatesAreHierarchical(t *testing.T) {
	n := namesIn("s")
	daily := ddlCagg(n, day, false)
	if !strings.Contains(daily, "time_bucket('1 day', at)") || !strings.Contains(daily, "FROM s.events") || !strings.HasSuffix(daily, "WITH NO DATA") {
		t.Errorf("daily:\n%s", daily)
	}
	weekly := ddlCagg(n, week, true)
	if !strings.Contains(weekly, "time_bucket('1 week', bucket)") || !strings.Contains(weekly, "FROM s.daily") || !strings.HasSuffix(weekly, "WITH DATA") {
		t.Errorf("weekly:\n%s", weekly)
	}
	if m := ddlCagg(n, month, false); !strings.Contains(m, "time_bucket('1 month', bucket)") || !strings.Contains(m, "sum(sum_amount)") || !strings.Contains(m, "max(max_amount)") {
		t.Errorf("monthly:\n%s", m)
	}
	if got := ddlRealtime(n, day, true); !strings.HasSuffix(got, "materialized_only = false)") {
		t.Errorf("realtime: %s", got)
	}
	ev := strings.Join(ddlEvents(n, true), ";")
	for _, want := range []string{"PRIMARY KEY (at, id)", "by_range('at', INTERVAL '7 days')", "create_default_indexes => false", "(customer, at)"} {
		if !strings.Contains(ev, want) {
			t.Errorf("the hypertable lacks %q:\n%s", want, ev)
		}
	}
	if pg := strings.Join(ddlEvents(n, false), ";"); strings.Contains(pg, "create_hypertable") || !strings.Contains(pg, "id       bigint PRIMARY KEY") || !strings.Contains(pg, "(customer, at)") {
		t.Errorf("the table of PostgreSQL:\n%s", pg)
	}
}

func TestPoliciesAndRefreshes(t *testing.T) {
	n := namesIn("s")
	p := policy(n, week, "INTERVAL '1 hour'", 1500*time.Millisecond)
	for _, want := range []string{"add_continuous_aggregate_policy('s.weekly'", "start_offset => NULL", "end_offset => INTERVAL '1 hour'", "INTERVAL '1500 milliseconds'"} {
		if !strings.Contains(p, want) {
			t.Errorf("policy lacks %q: %s", want, p)
		}
	}
	if got := refreshAll(n, month, "NULL"); got != "CALL refresh_continuous_aggregate('s.monthly', NULL, NULL)" {
		t.Errorf("refresh: %s", got)
	}
	if got := refreshAll(n, day, "'2026-06-01T00:00:00Z'::timestamptz"); !strings.Contains(got, "'s.daily', NULL, '2026-06-01") {
		t.Errorf("refresh: %s", got)
	}
	if fmt.Sprint(caggUnits) != "[day week month]" {
		t.Errorf("the units refresh in the order of their dependencies: %v", caggUnits)
	}
	if !strings.Contains(hypertableSizes, "hypertable_size") || placeholders(t, hypertableSizes) != 1 {
		t.Error("hypertableSizes")
	}
}
