// Package plans records the plan that a system chose for a query, in a form that the report can
// show: the text of the plan, a short summary of the operators (plan_nodes), and, when the plans
// of the different arguments of a query differ, how often each was seen (plans_seen).
//
// For kvutil the plan is the text of Explain; for PostgreSQL it is EXPLAIN (FORMAT JSON) of the
// statement, with the operators in the order of the tree. The summary has the same shape for both:
//
//	Limit > Sort > Index Scan[books_year]
//	Bitmap Heap Scan > BitmapOr > (Bitmap Index Scan[books_author] + Bitmap Index Scan[books_year])
//	Limit > TopN > IndexLookup[by_author]
//	Union > (IndexLookup[by_author] + IndexLookup[by_year])
//
// a node, then its child (">"), or its children in parentheses joined with "+".
package plans

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Plan is the plan of one query.
type Plan struct {
	// Text is the plan as lines of an indented tree.
	Text string
	// Nodes is the summary of its operators.
	Nodes string
}

// node is an operator of a plan, for the summary.
type node struct {
	name     string
	children []*node
}

func (n *node) String() string {
	s := n.name
	switch len(n.children) {
	case 0:
	case 1:
		s += " > " + n.children[0].String()
	default:
		parts := make([]string, len(n.children))
		for i, c := range n.children {
			parts[i] = c.String()
		}
		s += " > (" + strings.Join(parts, " + ") + ")"
	}
	return s
}

// --- kvutil

// KV parses the text of Explain of kvutil, a tree like
//
//	Limit 10, offset 20
//	└─ TopN by Year desc
//	   └─ IndexLookup by_author: 1 range, then check [Year >= 1970]
func KV(explain string) Plan {
	var roots []*node
	type level struct {
		depth int
		n     *node
	}
	var stack []level
	for _, line := range strings.Split(strings.TrimRight(explain, "\n"), "\n") {
		// The branches are 3 runes each: "├─ ", "└─ ", "│  " and "   ".
		runes := []rune(line)
		i := 0
		for i < len(runes) && !unicode.IsLetter(runes[i]) {
			i++
		}
		depth := i / 3
		n := &node{name: kvName(string(runes[i:]))}
		for len(stack) > 0 && stack[len(stack)-1].depth >= depth {
			stack = stack[:len(stack)-1]
		}
		if len(stack) == 0 {
			roots = append(roots, n)
		} else {
			parent := stack[len(stack)-1].n
			parent.children = append(parent.children, n)
		}
		stack = append(stack, level{depth, n})
	}
	p := Plan{Text: strings.TrimRight(explain, "\n")}
	parts := make([]string, len(roots))
	for i, r := range roots {
		parts[i] = r.String()
	}
	p.Nodes = strings.Join(parts, " + ")
	return p
}

// kvName is the operator of a line of Explain, with the index it reads.
func kvName(line string) string {
	word, rest, _ := strings.Cut(line, " ")
	switch word {
	case "IndexLookup", "IndexOnlyScan", "IndexKeys":
		name, _, _ := strings.Cut(rest, ":")
		name = strings.TrimSpace(name)
		// "IndexLookup by_year (reversed, in order of Year desc): 1 range"
		name, _, _ = strings.Cut(name, " ")
		return word + "[" + name + "]"
	}
	return strings.TrimSuffix(word, ",")
}

// --- PostgreSQL

type pgNode struct {
	NodeType     string    `json:"Node Type"`
	IndexName    string    `json:"Index Name"`
	RelationName string    `json:"Relation Name"`
	PlanRows     float64   `json:"Plan Rows"`
	Plans        []*pgNode `json:"Plans"`
	ScanDir      string    `json:"Scan Direction"`
}

func (n *pgNode) label() string {
	name := n.NodeType
	if n.IndexName != "" {
		name += "[" + n.IndexName + "]"
	}
	return name
}

func (n *pgNode) tree() *node {
	t := &node{name: n.label()}
	for _, c := range n.Plans {
		t.children = append(t.children, c.tree())
	}
	return t
}

func (n *pgNode) render(b *strings.Builder, depth int) {
	b.WriteString(strings.Repeat("  ", depth))
	b.WriteString(n.NodeType)
	switch {
	case n.IndexName != "" && n.RelationName != "":
		fmt.Fprintf(b, " using %s on %s", n.IndexName, n.RelationName)
	case n.IndexName != "":
		fmt.Fprintf(b, " on %s", n.IndexName)
	case n.RelationName != "":
		fmt.Fprintf(b, " on %s", n.RelationName)
	}
	fmt.Fprintf(b, " (rows=%.0f)\n", n.PlanRows)
	for _, c := range n.Plans {
		c.render(b, depth+1)
	}
}

// PG parses the result of EXPLAIN (FORMAT JSON).
func PG(explainJSON string) (Plan, error) {
	var top []struct {
		Plan *pgNode `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(explainJSON), &top); err != nil {
		return Plan{}, err
	}
	if len(top) == 0 || top[0].Plan == nil {
		return Plan{}, fmt.Errorf("plans: no plan in %q", explainJSON)
	}
	var b strings.Builder
	top[0].Plan.render(&b, 0)
	return Plan{Text: strings.TrimRight(b.String(), "\n"), Nodes: top[0].Plan.tree().String()}, nil
}

// Explain runs EXPLAIN (FORMAT JSON) of a statement with no parameters, which the server plans for
// the values that are in it, as it does for a statement that is not prepared.
func Explain(ctx context.Context, pool *pgxpool.Pool, statement string) (Plan, error) {
	var js string
	err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+statement, pgx.QueryExecModeSimpleProtocol).Scan(&js)
	if err != nil {
		return Plan{}, err
	}
	return PG(js)
}

// ExplainPrepared returns the plans that a prepared statement is run with, for the sets of
// arguments, in the order that they are run: the statement is prepared on a connection and run
// five times, so that the server has the plans of the first five runs, which are made for the
// arguments, and has decided from then on whether to use a generic plan (plan_cache_mode auto).
// Then it asks for the plan of each set. The statement uses $1, $2, ... for the arguments.
func ExplainPrepared(ctx context.Context, pool *pgxpool.Pool, statement string, sets [][]any) ([]Plan, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	const name = "kvb_plan"
	simple := pgx.QueryExecModeSimpleProtocol
	if _, err := conn.Exec(ctx, "PREPARE "+name+" AS "+statement, simple); err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), "DEALLOCATE "+name, simple)

	execute := func(set []any) string {
		lits := make([]string, len(set))
		for i, v := range set {
			lits[i] = Literal(v)
		}
		return "EXECUTE " + name + "(" + strings.Join(lits, ", ") + ")"
	}
	for i := 0; i < 5; i++ {
		if _, err := conn.Exec(ctx, execute(sets[i%len(sets)]), simple); err != nil {
			return nil, fmt.Errorf("execute: %w", err)
		}
	}
	var out []Plan
	for _, set := range sets {
		var js string
		if err := conn.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+execute(set), simple).Scan(&js); err != nil {
			return nil, fmt.Errorf("explain execute: %w", err)
		}
		p, err := PG(js)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// Literal writes a value as a literal of SQL, for a statement that is sent as text.
func Literal(v any) string {
	switch x := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		return strconv.FormatBool(x)
	case []string:
		parts := make([]string, len(x))
		for i, s := range x {
			parts[i] = Literal(s)
		}
		return "ARRAY[" + strings.Join(parts, ",") + "]::text[]"
	}
	panic(fmt.Sprintf("plans: no literal for %T", v))
}

// Inline replaces $1, $2, ... in a statement with the literals of the arguments, which makes of a
// prepared statement the text that a client that does not prepare sends.
func Inline(statement string, args []any) string {
	var b strings.Builder
	for i := 0; i < len(statement); i++ {
		c := statement[i]
		if c != '$' {
			b.WriteByte(c)
			continue
		}
		j := i + 1
		for j < len(statement) && statement[j] >= '0' && statement[j] <= '9' {
			j++
		}
		n, err := strconv.Atoi(statement[i+1 : j])
		if err != nil || n < 1 || n > len(args) {
			b.WriteByte(c)
			continue
		}
		b.WriteString(Literal(args[n-1]))
		i = j - 1
	}
	return b.String()
}

// --- the plans of the arguments of a query

// Survey collects the plans that were seen for the arguments of a query.
type Survey struct {
	seen  map[string]*seen
	order []string
	total int
}

type seen struct {
	plan  Plan
	count int
}

// Add records a plan.
func (s *Survey) Add(p Plan) {
	if s.seen == nil {
		s.seen = map[string]*seen{}
	}
	e, ok := s.seen[p.Nodes]
	if !ok {
		e = &seen{plan: p}
		s.seen[p.Nodes] = e
		s.order = append(s.order, p.Nodes)
	}
	e.count++
	s.total++
}

// Info returns what a result says of the plans: the plan that was seen most often, its summary,
// and, when more than one was seen, all of them with how often.
func (s *Survey) Info() map[string]string {
	if s.total == 0 {
		return nil
	}
	keys := append([]string(nil), s.order...)
	sort.SliceStable(keys, func(i, j int) bool { return s.seen[keys[i]].count > s.seen[keys[j]].count })
	top := s.seen[keys[0]]
	info := map[string]string{"plan": top.plan.Text, "plan_nodes": top.plan.Nodes}
	if len(keys) > 1 {
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = fmt.Sprintf("%s x%d", k, s.seen[k].count)
		}
		info["plans_seen"] = strings.Join(parts, "; ")
	}
	return info
}
