package report

import (
	"encoding/csv"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// The report is a list of blocks, which are written as Markdown and as HTML.

// Block is a Heading, a Para, a *Table, a Charts or a *Details.
type Block any

// Heading is a heading of level 1 to 6.
type Heading struct {
	Level int
	Text  string
}

// Para is a paragraph. Kind is "" for text, "warn" for a warning and "note" for a note.
type Para struct {
	Text string
	Kind string
}

// Table is a table of text. Name is the name of its CSV file without the extension; a table
// without a name has none (its data is in a CSV that holds more).
type Table struct {
	Name    string
	Caption string
	Header  []string
	Rows    [][]Cell
}

// Cell is a cell of a table: text, or a number with the way to show it. The page shows a number
// that is not available as n/a, and the CSV file leaves it empty; it is never zero.
type Cell struct {
	Text string
	Num  float64
	Fmt  func(float64) string
	Bold bool
}

// T is a cell of text.
func T(s string) Cell { return Cell{Text: s} }

// N is a cell of a number.
func N(v float64, f func(float64) string) Cell { return Cell{Num: v, Fmt: f} }

// B is the cell in bold.
func (c Cell) B() Cell { c.Bold = true; return c }

// Display is the cell as the page shows it.
func (c Cell) Display() string {
	if c.Fmt != nil {
		return c.Fmt(c.Num)
	}
	return c.Text
}

// Raw is the cell as the CSV file has it.
func (c Cell) Raw() string {
	if c.Fmt == nil {
		return c.Text
	}
	if math.IsNaN(c.Num) {
		return ""
	}
	return strconv.FormatFloat(c.Num, 'g', -1, 64)
}

// cellRow makes a row of cells from text, numbers and cells.
func cellRow(cells ...any) []Cell {
	out := make([]Cell, len(cells))
	for i, c := range cells {
		switch c := c.(type) {
		case Cell:
			out[i] = c
		case string:
			out[i] = T(c)
		default:
			out[i] = T(fmt.Sprint(c))
		}
	}
	return out
}

// Charts are charts that are shown side by side.
type Charts struct{ Items []*Chart }

// Details is a part of the report that a page can fold.
type Details struct {
	Summary string
	Open    bool
	Blocks  []Block
}

// Doc is a report.
type Doc struct {
	Title  string
	Blocks []Block
	// Data are tables that only have a CSV file: the data behind the tables of the page.
	Data []*Table
}

// Add appends blocks.
func (d *Doc) Add(bs ...Block) { d.Blocks = append(d.Blocks, bs...) }

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

func slug(s string) string {
	s = strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(s), "-"), "-")
	if s == "" {
		return "x"
	}
	return s
}

// walk calls f for each block, in order, going into the details.
func walk(bs []Block, f func(Block)) {
	for _, b := range bs {
		f(b)
		if d, ok := b.(*Details); ok {
			walk(d.Blocks, f)
		}
	}
}

// --- Markdown

func mdCell(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	return strings.ReplaceAll(s, "\n", " ")
}

// Markdown writes the report as Markdown. The charts are files in the directory charts/.
func (d *Doc) Markdown() string {
	var sb strings.Builder
	sb.WriteString("# " + d.Title + "\n\n")
	// The contents.
	for _, b := range d.Blocks {
		if h, ok := b.(*Heading); ok && h.Level == 2 {
			fmt.Fprintf(&sb, "- [%s](#%s)\n", h.Text, slug(h.Text))
		}
	}
	sb.WriteString("\n")
	mdBlocks(&sb, d.Blocks)
	return sb.String()
}

func mdBlocks(sb *strings.Builder, bs []Block) {
	for _, b := range bs {
		switch b := b.(type) {
		case *Heading:
			fmt.Fprintf(sb, "%s %s\n\n", strings.Repeat("#", b.Level), b.Text)
		case *Para:
			switch b.Kind {
			case "warn":
				fmt.Fprintf(sb, "> **Warning:** %s\n\n", b.Text)
			case "note":
				fmt.Fprintf(sb, "> %s\n\n", b.Text)
			default:
				sb.WriteString(b.Text + "\n\n")
			}
		case *Table:
			if b.Caption != "" {
				fmt.Fprintf(sb, "*%s*\n\n", b.Caption)
			}
			sb.WriteString("| " + strings.Join(mapStrings(b.Header, mdCell), " | ") + " |\n")
			sb.WriteString("|" + strings.Repeat(" --- |", len(b.Header)) + "\n")
			for _, r := range b.Rows {
				cells := make([]string, len(r))
				for i, c := range r {
					cells[i] = mdCell(c.Display())
					if c.Bold {
						cells[i] = "**" + cells[i] + "**"
					}
				}
				sb.WriteString("| " + strings.Join(cells, " | ") + " |\n")
			}
			sb.WriteString("\n")
		case *Charts:
			for _, c := range b.Items {
				fmt.Fprintf(sb, "![%s](charts/%s.svg)\n\n", strings.ReplaceAll(c.Title, "]", ""), c.Name)
			}
		case *Details:
			mdBlocks(sb, b.Blocks)
		}
	}
}

func mapCells(in []Cell) []string {
	out := make([]string, len(in))
	for i, c := range in {
		out[i] = c.Raw()
	}
	return out
}

func mapStrings(in []string, f func(string) string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = f(s)
	}
	return out
}

// --- HTML

const pageCSS = `
body{background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,sans-serif;margin:0 auto;max-width:1100px;padding:0 16px 48px}
h1{font-size:1.7rem;margin:1.2em 0 .3em}h2{font-size:1.3rem;margin:2em 0 .4em;border-bottom:1px solid var(--grid);padding-bottom:.2em}
h3{font-size:1.1rem;margin:1.6em 0 .3em}h4,h5,h6{font-size:1rem;margin:1.2em 0 .3em}
a{color:var(--c-kvutil)}
table{border-collapse:collapse;margin:.4em 0 1em;font-size:13px;display:block;overflow-x:auto;max-width:100%}
th,td{border:1px solid var(--grid);padding:3px 8px;text-align:left;vertical-align:top}
td.n{text-align:right;font-variant-numeric:tabular-nums}th{background:color-mix(in srgb,var(--grid) 35%,transparent)}
caption{caption-side:top;text-align:left;color:var(--muted);padding-bottom:2px}
.warn{border-left:4px solid #d1242f;background:color-mix(in srgb,#d1242f 12%,transparent);padding:6px 12px;margin:.6em 0}
.note{border-left:4px solid var(--muted);padding:2px 12px;margin:.6em 0;color:var(--muted)}
.charts{display:flex;flex-wrap:wrap;gap:8px 20px;margin:.4em 0 1em}.charts svg{max-width:100%;height:auto}
details{margin:.4em 0}summary{cursor:pointer;font-weight:600}
nav ul{columns:2;padding-left:1.2em}
`

// HTML writes the report as one self-contained page: inline CSS and inline SVG, no scripts.
func (d *Doc) HTML() string {
	var sb strings.Builder
	sb.WriteString("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n")
	sb.WriteString("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n")
	sb.WriteString("<meta name=\"color-scheme\" content=\"light dark\">\n")
	fmt.Fprintf(&sb, "<title>%s</title>\n<style>%s%s</style></head><body>\n", html.EscapeString(d.Title), chartCSS(), pageCSS)
	fmt.Fprintf(&sb, "<h1>%s</h1>\n<nav><ul>\n", html.EscapeString(d.Title))
	for _, b := range d.Blocks {
		if h, ok := b.(*Heading); ok && h.Level == 2 {
			fmt.Fprintf(&sb, "<li><a href=\"#%s\">%s</a></li>\n", slug(h.Text), html.EscapeString(h.Text))
		}
	}
	sb.WriteString("</ul></nav>\n")
	ids := map[string]int{}
	htmlBlocks(&sb, d.Blocks, ids)
	sb.WriteString("</body></html>\n")
	return sb.String()
}

func htmlBlocks(sb *strings.Builder, bs []Block, ids map[string]int) {
	for _, b := range bs {
		switch b := b.(type) {
		case *Heading:
			id := slug(b.Text)
			if n := ids[id]; n > 0 {
				id = fmt.Sprintf("%s-%d", id, n)
			}
			ids[slug(b.Text)]++
			fmt.Fprintf(sb, "<h%d id=\"%s\">%s</h%d>\n", b.Level, id, html.EscapeString(b.Text), b.Level)
		case *Para:
			cls := ""
			if b.Kind != "" {
				cls = ` class="` + b.Kind + `"`
			}
			text := html.EscapeString(b.Text)
			if b.Kind == "warn" {
				text = "<strong>Warning:</strong> " + text
			}
			fmt.Fprintf(sb, "<p%s>%s</p>\n", cls, text)
		case *Table:
			sb.WriteString("<table>")
			if b.Caption != "" {
				fmt.Fprintf(sb, "<caption>%s</caption>", html.EscapeString(b.Caption))
			}
			sb.WriteString("<thead><tr>")
			for _, h := range b.Header {
				fmt.Fprintf(sb, "<th>%s</th>", html.EscapeString(h))
			}
			sb.WriteString("</tr></thead><tbody>\n")
			for _, r := range b.Rows {
				sb.WriteString("<tr>")
				for _, c := range r {
					open, shut := "<td>", "</td>"
					if c.Fmt != nil {
						open = `<td class="n">`
					}
					if c.Bold {
						open, shut = open+"<strong>", "</strong></td>"
					}
					sb.WriteString(open + html.EscapeString(c.Display()) + shut)
				}
				sb.WriteString("</tr>\n")
			}
			sb.WriteString("</tbody></table>\n")
		case *Charts:
			sb.WriteString("<div class=\"charts\">")
			for _, c := range b.Items {
				sb.WriteString(c.Inline())
			}
			sb.WriteString("</div>\n")
		case *Details:
			open := ""
			if b.Open {
				open = " open"
			}
			fmt.Fprintf(sb, "<details%s><summary>%s</summary>\n", open, html.EscapeString(b.Summary))
			htmlBlocks(sb, b.Blocks, ids)
			sb.WriteString("</details>\n")
		}
	}
}

// --- files

// WriteFiles writes report.md, report.html, the charts and the CSV file of each named table.
func (d *Doc) WriteFiles(dir string) error {
	if err := os.MkdirAll(filepath.Join(dir, "charts"), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(d.Markdown()), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.html"), []byte(d.HTML()), 0o644); err != nil {
		return err
	}
	var err error
	walk(d.Blocks, func(b Block) {
		if err != nil {
			return
		}
		switch b := b.(type) {
		case *Charts:
			for _, c := range b.Items {
				if e := os.WriteFile(filepath.Join(dir, "charts", c.Name+".svg"), []byte(c.File()), 0o644); e != nil {
					err = e
				}
			}
		case *Table:
			if b.Name != "" {
				err = writeCSV(filepath.Join(dir, b.Name+".csv"), b.Header, b.Rows)
			}
		}
	})
	for _, t := range d.Data {
		if err == nil {
			err = writeCSV(filepath.Join(dir, t.Name+".csv"), t.Header, t.Rows)
		}
	}
	return err
}

func writeCSV(path string, header []string, rows [][]Cell) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.Write(header)
	for _, r := range rows {
		w.Write(mapCells(r))
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
