package plans

import "testing"

func TestKVPlan(t *testing.T) {
	text := "Limit 10, offset 20\n└─ TopN by Year desc\n   └─ IndexLookup by_author: 1 range, then check [Year >= 1970]"
	p := KV(text)
	if p.Nodes != "Limit > TopN > IndexLookup[by_author]" {
		t.Errorf("nodes %q", p.Nodes)
	}
	if p.Text != text {
		t.Errorf("text %q", p.Text)
	}

	p = KV("Union\n├─ IndexLookup by_author: 1 range\n└─ IndexLookup by_year (reversed, in order of Year desc): 1 range")
	if p.Nodes != "Union > (IndexLookup[by_author] + IndexLookup[by_year])" {
		t.Errorf("union: %q", p.Nodes)
	}

	p = KV("Limit 10\n└─ Intersect\n   ├─ IndexLookup a: 1 range, then check [X = 1]  <- streamed\n   │  └─ Odd\n   └─ IndexKeys b: 1 range")
	if p.Nodes != "Limit > Intersect > (IndexLookup[a] > Odd + IndexKeys[b])" {
		t.Errorf("intersect: %q", p.Nodes)
	}

	if p := KV("StoreScan, then check [Title LIKE 'a%']"); p.Nodes != "StoreScan" {
		t.Errorf("scan: %q", p.Nodes)
	}
}

func TestPGPlan(t *testing.T) {
	js := `[{"Plan": {"Node Type": "Limit", "Plan Rows": 10, "Plans": [
	  {"Node Type": "Bitmap Heap Scan", "Relation Name": "books", "Plan Rows": 50, "Plans": [
	    {"Node Type": "BitmapOr", "Plan Rows": 50, "Plans": [
	      {"Node Type": "Bitmap Index Scan", "Index Name": "books_author", "Plan Rows": 5},
	      {"Node Type": "Bitmap Index Scan", "Index Name": "books_year", "Plan Rows": 45}]}]}]}}]`
	p, err := PG(js)
	if err != nil {
		t.Fatal(err)
	}
	want := "Limit > Bitmap Heap Scan > BitmapOr > (Bitmap Index Scan[books_author] + Bitmap Index Scan[books_year])"
	if p.Nodes != want {
		t.Errorf("nodes %q", p.Nodes)
	}
	if p.Text == "" || p.Text[:5] != "Limit" {
		t.Errorf("text %q", p.Text)
	}
	if _, err := PG(`[]`); err == nil {
		t.Error("an empty plan is accepted")
	}
}

func TestLiteralsAndInline(t *testing.T) {
	got := Inline("SELECT 1 WHERE a = $1 AND b BETWEEN $2 AND $3 AND c = ANY($4::text[]) AND d = $10", []any{"o'neil", int64(5), 7, []string{"x", "y"}})
	want := "SELECT 1 WHERE a = 'o''neil' AND b BETWEEN 5 AND 7 AND c = ANY(ARRAY['x','y']::text[]::text[]) AND d = $10"
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestSurvey(t *testing.T) {
	var s Survey
	if s.Info() != nil {
		t.Error("an empty survey has info")
	}
	a, b := Plan{Text: "a", Nodes: "A"}, Plan{Text: "b", Nodes: "B"}
	s.Add(b)
	s.Add(a)
	s.Add(a)
	info := s.Info()
	if info["plan_nodes"] != "A" || info["plan"] != "a" || info["plans_seen"] != "A x2; B x1" {
		t.Errorf("%v", info)
	}
}
