package report

import (
	"fmt"
	"html"
	"math"
	"strings"
)

// systemColors are the colours of the systems, defined here and nowhere else: a system has the
// same colour in every chart of the report, in the light and in the dark scheme. Several variants
// of a system differ by the style of their line, not by colour.
var systemColors = []struct{ name, light, dark string }{
	{"kvutil", "#2b6cb0", "#6aa6e8"},
	{"postgres", "#c2641a", "#eda15c"},
	{"timescale", "#2a8f6b", "#5cc7a0"},
	{"other", "#7b5ab5", "#b49ae8"},
}

var dashes = []string{"", "7 4", "2 3", "9 3 2 3"}

// chartCSS defines the colours and the classes of the charts. The page has it, and each SVG file
// has it too, so that the files look the same on their own.
func chartCSS() string {
	var light, dark strings.Builder
	for _, c := range systemColors {
		fmt.Fprintf(&light, "--c-%s:%s;", c.name, c.light)
		fmt.Fprintf(&dark, "--c-%s:%s;", c.name, c.dark)
	}
	var cls strings.Builder
	for _, c := range systemColors {
		fmt.Fprintf(&cls, ".c-%s{color:var(--c-%s)}", c.name, c.name)
	}
	return `:root{--fg:#1f2328;--muted:#656d76;--grid:#d0d7de;--bg:#ffffff;` + light.String() + `}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--fg:#e6edf3;--muted:#9198a1;--grid:#3d444d;--bg:#0d1117;` + dark.String() + `}}
` + cls.String() + `
.tx{fill:var(--fg);font:11px system-ui,sans-serif}.tm{fill:var(--muted);font:10px system-ui,sans-serif}
.tt{fill:var(--fg);font:600 12px system-ui,sans-serif}.gr{stroke:var(--grid);stroke-width:1;fill:none}
.ln{fill:none;stroke:currentColor;stroke-width:2}.bar{fill:currentColor}.ax{stroke:var(--muted);stroke-width:1}`
}

func sysClass(system string) string {
	for _, c := range systemColors {
		if c.name == system {
			return "c-" + system
		}
	}
	return "c-other"
}

// Chart is an SVG chart: its body, to be inlined in the page or wrapped in a file.
type Chart struct {
	Name  string // the file name without the extension
	Title string
	W, H  int
	Body  string
}

// Inline is the chart as the page has it.
func (c *Chart) Inline() string {
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" role="img" aria-label="%s">%s</svg>`,
		c.W, c.H, c.W, html.EscapeString(c.Title), c.Body)
}

// File is the chart as a file of its own.
func (c *Chart) File() string {
	return fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img"><title>%s</title><style>%s</style><rect width="100%%" height="100%%" fill="var(--bg)"/>%s</svg>`,
		c.W, c.H, c.W, c.H, html.EscapeString(c.Title), chartCSS(), c.Body)
}

// Bar is a bar of a bar chart. A NaN value is a figure that is not available: it has no bar.
type Bar struct {
	Label  string
	System string
	Value  float64
	Text   string
}

// BarChart draws horizontal bars from zero.
func BarChart(name, title, unit string, bars []Bar) *Chart {
	const rowH, padT, barMax = 20, 26, 300
	labelW := 40
	for _, b := range bars {
		labelW = max(labelW, int(float64(len([]rune(b.Label)))*6.0)+12)
	}
	labelW = min(labelW, 280)
	maxV := 0.0
	for _, b := range bars {
		if !math.IsNaN(b.Value) && b.Value > maxV {
			maxV = b.Value
		}
	}
	w := labelW + barMax + 110
	h := padT + len(bars)*rowH + 6
	var sb strings.Builder
	fmt.Fprintf(&sb, `<text class="tt" x="6" y="15">%s</text>`, html.EscapeString(title+" ("+unit+")"))
	for i, b := range bars {
		y := padT + i*rowH
		label := b.Label
		if r := []rune(label); len(r) > 46 {
			label = string(r[:45]) + "…"
		}
		fmt.Fprintf(&sb, `<text class="tx" x="%d" y="%d" text-anchor="end">%s</text>`, labelW-6, y+13, html.EscapeString(label))
		text := b.Text
		bw := 0.0
		if math.IsNaN(b.Value) {
			text = "n/a"
		} else if maxV > 0 {
			bw = b.Value / maxV * barMax
		}
		if bw > 0 {
			bw = math.Max(bw, 1)
			fmt.Fprintf(&sb, `<g class="%s"><rect class="bar" x="%d" y="%d" width="%.1f" height="%d"/></g>`, sysClass(b.System), labelW, y+3, bw, rowH-6)
		}
		fmt.Fprintf(&sb, `<text class="tm" x="%.1f" y="%d">%s</text>`, float64(labelW)+bw+5, y+13, html.EscapeString(text))
	}
	fmt.Fprintf(&sb, `<line class="ax" x1="%d" y1="%d" x2="%d" y2="%d"/>`, labelW, padT, labelW, h-4)
	return &Chart{Name: name, Title: title, W: w, H: h, Body: sb.String()}
}

// Line is a line of a line chart.
type Line struct {
	Label  string
	System string
	Style  int // the style of the line among the lines of the system
	X, Y   []float64
}

// LineOpts are the options of a line chart.
type LineOpts struct {
	Title, XLabel, YLabel string
	LogX, LogY            bool
	// XTicks names the points of a categorical axis: X of the lines are then their positions.
	XTicks []string
	YFmt   func(float64) string
	// YBytes puts the ticks of the y axis at round numbers of KiB, MiB or GiB.
	YBytes bool
}

// LineChart draws lines over a linear, logarithmic or categorical x axis.
func LineChart(name string, lines []Line, o LineOpts) *Chart {
	const w, padL, padR, padT = 640, 64, 16, 42
	plotW := float64(w - padL - padR)
	padB := 40
	if len(o.XTicks) > 0 {
		longest := 0
		for _, t := range o.XTicks {
			longest = max(longest, len([]rune(t)))
		}
		if longest*len(o.XTicks)*6 > int(plotW) {
			padB = 30 + min(longest, 24)*4
		}
	}
	plotH := 220.0
	yfmt := o.YFmt
	if yfmt == nil {
		yfmt = fmtSI
	}

	// The extent of the data.
	xmin, xmax, ymin, ymax := math.Inf(1), math.Inf(-1), math.Inf(1), math.Inf(-1)
	for _, l := range lines {
		for i := range l.X {
			if math.IsNaN(l.Y[i]) || (o.LogY && l.Y[i] <= 0) || (o.LogX && l.X[i] <= 0) {
				continue
			}
			xmin, xmax = math.Min(xmin, l.X[i]), math.Max(xmax, l.X[i])
			ymin, ymax = math.Min(ymin, l.Y[i]), math.Max(ymax, l.Y[i])
		}
	}
	if math.IsInf(xmin, 0) {
		xmin, xmax, ymin, ymax = 1, 10, 1, 10
	}
	if len(o.XTicks) > 0 {
		xmin, xmax = 0, float64(len(o.XTicks)-1)
	}

	var xt, yt []tick
	var tx, ty func(float64) float64
	switch {
	case len(o.XTicks) > 0:
		for i, t := range o.XTicks {
			xt = append(xt, tick{float64(i), t})
		}
		span := math.Max(xmax-xmin, 1)
		tx = func(v float64) float64 { return float64(padL) + (v-xmin)/span*plotW }
		if len(o.XTicks) == 1 {
			tx = func(float64) float64 { return float64(padL) + plotW/2 }
		}
	case o.LogX:
		xt, xmin, xmax = logTicks(xmin, xmax)
		tx = func(v float64) float64 {
			return float64(padL) + (math.Log10(v)-math.Log10(xmin))/(math.Log10(xmax)-math.Log10(xmin))*plotW
		}
	default:
		xt, xmin, xmax = linTicks(xmin, xmax, false, fmtSI)
		tx = func(v float64) float64 { return float64(padL) + (v-xmin)/(xmax-xmin)*plotW }
	}
	if o.LogY {
		yt, ymin, ymax = logTicks(ymin, ymax)
		for i := range yt {
			yt[i].label = yfmt(yt[i].v)
		}
		ty = func(v float64) float64 {
			return padT + plotH - (math.Log10(v)-math.Log10(ymin))/(math.Log10(ymax)-math.Log10(ymin))*plotH
		}
	} else {
		// Sizes get ticks that are round in their unit (4 MiB, 8 MiB), not in bytes.
		scale := 1.0
		for o.YBytes && scale*1024 <= ymax {
			scale *= 1024
		}
		yt, ymin, ymax = linTicks(0, ymax/scale, true, func(v float64) string { return yfmt(v * scale) })
		for i := range yt {
			yt[i].v *= scale
		}
		ymin, ymax = ymin*scale, ymax*scale
		ty = func(v float64) float64 { return padT + plotH - (v-ymin)/(ymax-ymin)*plotH }
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, `<text class="tt" x="6" y="15">%s</text>`, html.EscapeString(o.Title))
	for _, t := range yt {
		y := ty(t.v)
		fmt.Fprintf(&sb, `<line class="gr" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/><text class="tm" x="%d" y="%.1f" text-anchor="end">%s</text>`,
			padL, y, w-padR, y, padL-6, y+3, html.EscapeString(t.label))
	}
	rotate := padB > 40
	for _, t := range xt {
		x := tx(t.v)
		fmt.Fprintf(&sb, `<line class="gr" x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f"/>`, x, float64(padT), x, padT+plotH)
		if rotate {
			fmt.Fprintf(&sb, `<text class="tm" x="%.1f" y="%.1f" text-anchor="end" transform="rotate(-35 %.1f %.1f)">%s</text>`,
				x+4, padT+plotH+14, x+4, padT+plotH+14, html.EscapeString(t.label))
		} else {
			fmt.Fprintf(&sb, `<text class="tm" x="%.1f" y="%.1f" text-anchor="middle">%s</text>`, x, padT+plotH+14, html.EscapeString(t.label))
		}
	}
	fmt.Fprintf(&sb, `<line class="ax" x1="%d" y1="%.1f" x2="%d" y2="%.1f"/>`, padL, padT+plotH, w-padR, padT+plotH)
	if o.XLabel != "" {
		fmt.Fprintf(&sb, `<text class="tm" x="%.1f" y="%.1f" text-anchor="middle">%s</text>`, float64(padL)+plotW/2, padT+plotH+float64(padB)-6, html.EscapeString(o.XLabel))
	}
	if o.YLabel != "" {
		fmt.Fprintf(&sb, `<text class="tm" x="6" y="%d">%s</text>`, padT-12, html.EscapeString(o.YLabel))
	}

	for _, l := range lines {
		dash := ""
		if d := dashes[l.Style%len(dashes)]; d != "" {
			dash = ` stroke-dasharray="` + d + `"`
		}
		var path strings.Builder
		var dots strings.Builder
		pen := "M"
		for i := range l.X {
			if math.IsNaN(l.Y[i]) || (o.LogY && l.Y[i] <= 0) || (o.LogX && l.X[i] <= 0) {
				pen = "M"
				continue
			}
			x, y := tx(l.X[i]), ty(l.Y[i])
			fmt.Fprintf(&path, "%s%.1f %.1f", pen, x, y)
			pen = "L"
			fmt.Fprintf(&dots, `<circle cx="%.1f" cy="%.1f" r="3"><title>%s</title></circle>`, x, y, html.EscapeString(fmt.Sprintf("%s: %s", l.Label, yfmt(l.Y[i]))))
		}
		if path.Len() == 0 {
			continue
		}
		fmt.Fprintf(&sb, `<g class="%s" fill="currentColor"><path class="ln" d="%s"%s/>%s</g>`, sysClass(l.System), path.String(), dash, dots.String())
	}

	// The legend, in two columns under the plot.
	top := padT + int(plotH) + padB
	for i, l := range lines {
		col, row := i%2, i/2
		x, y := padL+col*(int(plotW)/2), top+row*16
		dash := ""
		if d := dashes[l.Style%len(dashes)]; d != "" {
			dash = ` stroke-dasharray="` + d + `"`
		}
		fmt.Fprintf(&sb, `<g class="%s"><line class="ln" x1="%d" y1="%d" x2="%d" y2="%d"%s/></g><text class="tx" x="%d" y="%d">%s</text>`,
			sysClass(l.System), x, y, x+22, y, dash, x+28, y+4, html.EscapeString(l.Label))
	}
	h := top + (len(lines)+1)/2*16 + 6
	return &Chart{Name: name, Title: o.Title, W: w, H: h, Body: sb.String()}
}

type tick struct {
	v     float64
	label string
}

// linTicks are round ticks from lo (or lower) to above hi; zero starts the axis when fromZero.
func linTicks(lo, hi float64, fromZero bool, f func(float64) string) ([]tick, float64, float64) {
	if fromZero {
		lo = 0
	}
	if hi <= lo {
		hi = lo + 1
	}
	step := niceStep((hi - lo) / 5)
	start := math.Floor(lo/step) * step
	end := math.Ceil(hi/step) * step
	var ts []tick
	for v := start; v <= end+step/2; v += step {
		ts = append(ts, tick{v, f(v)})
	}
	return ts, start, end
}

func niceStep(raw float64) float64 {
	mag := math.Pow(10, math.Floor(math.Log10(raw)))
	switch f := raw / mag; {
	case f <= 1:
		return mag
	case f <= 2:
		return 2 * mag
	case f <= 5:
		return 5 * mag
	}
	return 10 * mag
}

// logTicks are the powers of ten around lo..hi, which are also the new ends of the axis.
func logTicks(lo, hi float64) ([]tick, float64, float64) {
	if lo <= 0 || math.IsInf(lo, 0) {
		lo = 1
	}
	if hi < lo {
		hi = lo
	}
	a, b := math.Floor(math.Log10(lo)), math.Ceil(math.Log10(hi))
	if b <= a {
		b = a + 1
	}
	var ts []tick
	for e := a; e <= b; e++ {
		v := math.Pow(10, e)
		ts = append(ts, tick{v, fmtSI(v)})
	}
	return ts, math.Pow(10, a), math.Pow(10, b)
}
