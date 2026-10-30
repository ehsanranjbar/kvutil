package report

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// na is what a figure that is not available is shown as; it is never shown as zero.
const na = "n/a"

// fmtSI is a short number for an axis: 1.5k, 20M.
func fmtSI(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	a := math.Abs(v)
	switch {
	case a >= 1e9:
		return trim(v/1e9) + "G"
	case a >= 1e6:
		return trim(v/1e6) + "M"
	case a >= 1e3:
		return trim(v/1e3) + "k"
	case a >= 1 || a == 0:
		return trim(v)
	}
	return strconv.FormatFloat(v, 'g', 3, 64)
}

func trim(v float64) string {
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', -1, 64)
}

// fmtNum is a number for a table: grouped thousands and the digits that carry information.
func fmtNum(v float64) string {
	switch {
	case math.IsNaN(v):
		return na
	case math.IsInf(v, 0):
		return "inf"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return group(strconv.FormatInt(int64(v), 10))
	case math.Abs(v) >= 100:
		return group(strconv.FormatInt(int64(math.Round(v)), 10))
	case math.Abs(v) >= 1:
		return strconv.FormatFloat(v, 'f', 2, 64)
	}
	return strconv.FormatFloat(v, 'g', 3, 64)
}

func group(s string) string {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// fmtUs is a latency given in microseconds.
func fmtUs(v float64) string {
	switch {
	case math.IsNaN(v):
		return na
	case v >= 1e6:
		return fmt.Sprintf("%.2f s", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.2f ms", v/1e3)
	}
	return fmt.Sprintf("%.1f µs", v)
}

// fmtMs is a time given in milliseconds.
func fmtMs(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	return fmtUs(v * 1000)
}

// fmtSec is a time given in seconds.
func fmtSec(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	return fmtUs(v * 1e6)
}

// fmtNs is a time given in nanoseconds.
func fmtNs(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	if v < 1000 {
		return fmt.Sprintf("%.1f ns", v)
	}
	return fmtUs(v / 1000)
}

// fmtBytes is a size in bytes, in binary units.
func fmtBytes(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	a := math.Abs(v)
	if a < 1024 {
		return fmt.Sprintf("%.0f B", v)
	}
	div, exp := 1024.0, 0
	for a/div >= 1024 && exp < 5 {
		div *= 1024
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", v/div, "KMGTPE"[exp])
}

// fmtRatio is a ratio, like 0.42x.
func fmtRatio(v float64) string {
	switch {
	case math.IsNaN(v) || math.IsInf(v, 0):
		return na
	case v >= 100:
		return fmt.Sprintf("%.0fx", v)
	case v >= 10:
		return fmt.Sprintf("%.1fx", v)
	}
	return fmt.Sprintf("%.2fx", v)
}

func fmtCores(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	return fmt.Sprintf("%.2f", v)
}

func fmtPct(v float64) string {
	if math.IsNaN(v) {
		return na
	}
	return fmt.Sprintf("%+.1f%%", v)
}

// ratio is a over b, NaN if either is not available or b is zero.
func ratio(a, b float64) float64 {
	if math.IsNaN(a) || math.IsNaN(b) || b == 0 {
		return math.NaN()
	}
	return a / b
}

// formatFor picks how to show a number of an extra by the name that the workload gave it.
func formatFor(name string) func(float64) string {
	n := strings.ToLower(name)
	switch {
	case strings.HasSuffix(n, "_bytes") || strings.Contains(n, "_bytes_") || strings.HasPrefix(n, "bytes_"):
		return fmtBytes
	case strings.HasSuffix(n, "_ms") || strings.Contains(n, "_ms_"):
		return fmtMs
	case strings.HasSuffix(n, "_us") || strings.Contains(n, "_us_"):
		return fmtUs
	case strings.HasSuffix(n, "_ns") || strings.Contains(n, "_ns_") || strings.HasPrefix(n, "ns_per"):
		return fmtNs
	case strings.HasSuffix(n, "_sec") || strings.Contains(n, "_sec_"):
		return fmtSec
	}
	return fmtNum
}
