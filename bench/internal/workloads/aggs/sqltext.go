package aggs

import (
	"fmt"
	"time"
)

// The SQL of the workloads of aggregations. Everything that can be written without a database is
// here, so that the tests can look at it and docs/workloads.md quotes it.

// names are the relations a schema has, as they are written in a statement.
type names struct {
	events  string // the table of the events; a hypertable on TimescaleDB
	summary string // the summary table of PostgreSQL: a row for each customer and day
	daily   string // the continuous aggregates of TimescaleDB
	weekly  string
	monthly string
	fn      string // the trigger function of the summary table
}

func namesIn(schema string) names {
	return names{
		events:  schema + ".events",
		summary: schema + ".daily_summary",
		daily:   schema + ".daily",
		weekly:  schema + ".weekly",
		monthly: schema + ".monthly",
		fn:      schema + ".events_summary",
	}
}

func (n names) cagg(u unit) string {
	switch u {
	case week:
		return n.weekly
	case month:
		return n.monthly
	}
	return n.daily
}

// source says where a query reads from.
type source int

const (
	// srcRaw reads the events and groups them at read time.
	srcRaw source = iota
	// srcSummary reads the summary table of PostgreSQL.
	srcSummary
	// srcCagg reads the continuous aggregates of TimescaleDB.
	srcCagg
)

// ddlEvents creates the table of the events. On TimescaleDB the primary key has the time in it,
// as a unique index of a hypertable must have the column that it is partitioned by.
func ddlEvents(n names, timescale bool) []string {
	if timescale {
		return []string{
			fmt.Sprintf(`CREATE TABLE %s (
	id       bigint NOT NULL,
	customer text NOT NULL,
	amount   bigint NOT NULL,
	at       timestamptz NOT NULL,
	kind     text NOT NULL,
	PRIMARY KEY (at, id)
)`, n.events),
			fmt.Sprintf("SELECT create_hypertable('%s', by_range('at', INTERVAL '7 days'), create_default_indexes => false)", n.events),
			fmt.Sprintf("CREATE INDEX events_customer_at ON %s (customer, at)", n.events),
		}
	}
	return []string{
		fmt.Sprintf(`CREATE TABLE %s (
	id       bigint PRIMARY KEY,
	customer text NOT NULL,
	amount   bigint NOT NULL,
	at       timestamptz NOT NULL,
	kind     text NOT NULL
)`, n.events),
		fmt.Sprintf("CREATE INDEX events_customer_at ON %s (customer, at)", n.events),
	}
}

// ddlSummary creates the summary table of PostgreSQL, a row for each customer and day (UTC), and
// the trigger that keeps it exact for every insert, update and delete of an event. The maximum
// of a day that loses its greatest event is computed again from the events of the day.
func ddlSummary(n names) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE %s (
	customer   text NOT NULL,
	day        timestamptz NOT NULL,
	sum_amount bigint NOT NULL,
	cnt        bigint NOT NULL,
	max_amount bigint NOT NULL,
	PRIMARY KEY (customer, day)
)`, n.summary),
		fmt.Sprintf(`CREATE FUNCTION %[1]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF TG_OP IN ('UPDATE', 'DELETE') THEN
		UPDATE %[2]s SET sum_amount = sum_amount - OLD.amount, cnt = cnt - 1
		WHERE customer = OLD.customer AND day = date_trunc('day', OLD.at, 'UTC');
		UPDATE %[2]s SET max_amount = coalesce((SELECT max(amount) FROM %[3]s
				WHERE customer = OLD.customer AND at >= day AND at < day + interval '24 hours'), 0)
		WHERE customer = OLD.customer AND day = date_trunc('day', OLD.at, 'UTC')
			AND cnt > 0 AND max_amount <= OLD.amount;
		DELETE FROM %[2]s WHERE customer = OLD.customer AND day = date_trunc('day', OLD.at, 'UTC') AND cnt = 0;
	END IF;
	IF TG_OP IN ('INSERT', 'UPDATE') THEN
		INSERT INTO %[2]s AS s (customer, day, sum_amount, cnt, max_amount)
		VALUES (NEW.customer, date_trunc('day', NEW.at, 'UTC'), NEW.amount, 1, NEW.amount)
		ON CONFLICT (customer, day) DO UPDATE SET
			sum_amount = s.sum_amount + EXCLUDED.sum_amount,
			cnt = s.cnt + 1,
			max_amount = greatest(s.max_amount, EXCLUDED.max_amount);
	END IF;
	RETURN NULL;
END
$$`, n.fn, n.summary, n.events),
		fmt.Sprintf("CREATE TRIGGER events_summary AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %s()", n.events, n.fn),
	}
}

// buildSummary builds the summary table from the events that are there.
func buildSummary(n names) string {
	return fmt.Sprintf(`INSERT INTO %s (customer, day, sum_amount, cnt, max_amount)
SELECT customer, date_trunc('day', at, 'UTC'), sum(amount)::bigint, count(*), max(amount)
FROM %s GROUP BY 1, 2`, n.summary, n.events)
}

// The continuous aggregates: the days from the hypertable, and the weeks and months from the days
// (hierarchical aggregates), which TimescaleDB 2.31 supports. time_bucket of a timestamptz is in
// UTC unless it is given a time zone.
func ddlCagg(n names, u unit, withData bool) string {
	var q string
	switch u {
	case day:
		q = fmt.Sprintf(`SELECT time_bucket('1 day', at) AS bucket, customer,
	sum(amount)::bigint AS sum_amount, count(*) AS cnt, max(amount) AS max_amount
FROM %s GROUP BY 1, 2`, n.events)
	default:
		width := "1 week"
		if u == month {
			width = "1 month"
		}
		q = fmt.Sprintf(`SELECT time_bucket('%s', bucket) AS bucket, customer,
	sum(sum_amount)::bigint AS sum_amount, sum(cnt)::bigint AS cnt, max(max_amount) AS max_amount
FROM %s GROUP BY 1, 2`, width, n.daily)
	}
	with := "WITH NO DATA"
	if withData {
		with = "WITH DATA"
	}
	return fmt.Sprintf("CREATE MATERIALIZED VIEW %s WITH (timescaledb.continuous) AS\n%s\n%s", n.cagg(u), q, with)
}

// cagg units in the order that they are refreshed: a unit is built from the one before it.
var caggUnits = []unit{day, week, month}

// ddlRealtime makes a continuous aggregate a real-time aggregate (materialized_only is false).
func ddlRealtime(n names, u unit, realtime bool) string {
	return fmt.Sprintf("ALTER MATERIALIZED VIEW %s SET (timescaledb.materialized_only = %t)", n.cagg(u), !realtime)
}

// refreshAll refreshes a continuous aggregate from the beginning of time: every region that was
// invalidated, and the buckets that were never materialized, up to the end. A NULL end is the end
// of the data, partial bucket included, which moves the watermark of the aggregate past it, so that
// what is written to that bucket afterwards is not seen by a real-time aggregate until the next
// refresh. A real-time aggregate is refreshed up to a time instead (the end of the events that
// are loaded): the buckets that are complete before it are materialized, and the ones that are
// not are left to the real-time part, which reads them exactly.
func refreshAll(n names, u unit, end string) string {
	return fmt.Sprintf("CALL refresh_continuous_aggregate('%s', NULL, %s)", n.cagg(u), end)
}

// policy adds the refresh policy of a continuous aggregate. The window starts at the beginning
// of time, so that changes to old events are refreshed too; it ends at the end of the data (NULL)
// for the policy that keeps the materialized state current, and an hour ago for the one that
// leaves the recent buckets to the real-time part of the aggregate.
func policy(n names, u unit, endOffset string, every time.Duration) string {
	return fmt.Sprintf("SELECT add_continuous_aggregate_policy('%s', start_offset => NULL, end_offset => %s, schedule_interval => INTERVAL '%d milliseconds')",
		n.cagg(u), endOffset, every.Milliseconds())
}

// The statements of the writes. TimescaleDB finds a row by its time too, which its primary key
// needs, and PostgreSQL by the id alone.
func insertSQL(n names) string {
	return fmt.Sprintf("INSERT INTO %s (id, customer, amount, at, kind) VALUES ($1, $2, $3, $4, $5)", n.events)
}

func updateSQL(n names, timescale bool) string {
	if timescale {
		return fmt.Sprintf("UPDATE %s SET customer = $2, amount = $3, at = $4, kind = $5 WHERE id = $1 AND at = $6", n.events)
	}
	return fmt.Sprintf("UPDATE %s SET customer = $2, amount = $3, at = $4, kind = $5 WHERE id = $1", n.events)
}

func deleteSQL(n names, timescale bool) string {
	if timescale {
		return fmt.Sprintf("DELETE FROM %s WHERE id = $1 AND at = $2", n.events)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE id = $1", n.events)
}

// readSQL is the statement of a query on a source. The arguments are, for a series, the customer
// and the window; for the total, the customer; for the top, the month (the start and the end of
// it, or for the continuous aggregate only its start).
func readSQL(n names, src source, q query) string {
	switch q.Kind {
	case qSeries:
		return seriesSQL(n, src, q.Unit)
	case qTotal:
		return totalSQL(n, src)
	}
	return topSQL(n, src)
}

func seriesSQL(n names, src source, u unit) string {
	switch src {
	case srcCagg:
		return fmt.Sprintf(`SELECT bucket, sum_amount, cnt, max_amount FROM %s
WHERE customer = $1 AND bucket >= $2 AND bucket < $3 ORDER BY bucket`, n.cagg(u))
	case srcSummary:
		if u == day {
			return fmt.Sprintf(`SELECT day, sum_amount, cnt, max_amount FROM %s
WHERE customer = $1 AND day >= $2 AND day < $3 ORDER BY day`, n.summary)
		}
		return fmt.Sprintf(`SELECT date_trunc('%s', day, 'UTC') AS bucket, sum(sum_amount)::bigint, sum(cnt)::bigint, max(max_amount)
FROM %s WHERE customer = $1 AND day >= $2 AND day < $3 GROUP BY 1 ORDER BY 1`, u, n.summary)
	}
	return fmt.Sprintf(`SELECT date_trunc('%s', at, 'UTC') AS bucket, sum(amount)::bigint, count(*), max(amount)
FROM %s WHERE customer = $1 AND at >= $2 AND at < $3 GROUP BY 1 ORDER BY 1`, u, n.events)
}

func totalSQL(n names, src source) string {
	switch src {
	case srcCagg:
		// The months are the coarsest aggregate there is.
		return fmt.Sprintf("SELECT coalesce(sum(sum_amount), 0)::bigint, coalesce(sum(cnt), 0)::bigint FROM %s WHERE customer = $1", n.monthly)
	case srcSummary:
		return fmt.Sprintf("SELECT coalesce(sum(sum_amount), 0)::bigint, coalesce(sum(cnt), 0)::bigint FROM %s WHERE customer = $1", n.summary)
	}
	return fmt.Sprintf("SELECT coalesce(sum(amount), 0)::bigint, count(*) FROM %s WHERE customer = $1", n.events)
}

func topSQL(n names, src source) string {
	switch src {
	case srcCagg:
		return fmt.Sprintf(`SELECT customer, sum_amount FROM %s WHERE bucket = $1
ORDER BY sum_amount DESC, customer LIMIT 10`, n.monthly)
	case srcSummary:
		return fmt.Sprintf(`SELECT customer, sum(sum_amount)::bigint AS s FROM %s WHERE day >= $1 AND day < $2
GROUP BY customer ORDER BY s DESC, customer LIMIT 10`, n.summary)
	}
	return fmt.Sprintf(`SELECT customer, sum(amount)::bigint AS s FROM %s WHERE at >= $1 AND at < $2
GROUP BY customer ORDER BY s DESC, customer LIMIT 10`, n.events)
}

// readArgs are the arguments of the statement of a query on a source.
func readArgs(src source, q query) []any {
	switch q.Kind {
	case qSeries:
		return []any{q.Customer, q.From, q.To}
	case qTotal:
		return []any{q.Customer}
	}
	if src == srcCagg {
		return []any{q.From}
	}
	return []any{q.From, q.To}
}

// hypertableSizes is the statement that returns the size of the hypertables of a schema (the
// events), and of the materialization hypertables of its continuous aggregates, in this order.
const hypertableSizes = `SELECT
	coalesce((SELECT sum(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass))
		FROM timescaledb_information.hypertables WHERE hypertable_schema = $1), 0)::bigint,
	coalesce((SELECT sum(hypertable_size(format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name)::regclass))
		FROM timescaledb_information.continuous_aggregates WHERE view_schema = $1), 0)::bigint`
