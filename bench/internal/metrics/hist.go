// Package metrics measures: latency histograms, the resources of the process, and the results
// that are written as JSON lines.
package metrics

import (
	"math"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	histMin = 1                // 1 ns
	histMax = int64(120 * 1e9) // 120 s; a longer operation is recorded as this long
	histSig = 3                // significant digits
	micro   = float64(time.Microsecond)
)

// Hist is a latency histogram of one worker: it is not safe for concurrent use, and the
// histograms of the workers are merged when the measurement is over.
type Hist struct {
	h *hdrhistogram.Histogram
	// saturated is the number of latencies that were longer than the histogram goes up to.
	saturated int64
}

// NewHist returns an empty histogram with a resolution of 3 significant digits, from 1 ns up to
// 120 s.
func NewHist() *Hist {
	return &Hist{h: hdrhistogram.New(histMin, histMax, histSig)}
}

// Record records the latency of an operation.
func (h *Hist) Record(d time.Duration) {
	v := int64(d)
	if v < histMin {
		v = histMin
	}
	if v > histMax {
		v = histMax
		h.saturated++
	}
	_ = h.h.RecordValue(v)
}

// Merge adds what another histogram has recorded.
func (h *Hist) Merge(o *Hist) {
	h.h.Merge(o.h)
	h.saturated += o.saturated
}

// Count is the number of latencies recorded.
func (h *Hist) Count() int64 { return h.h.TotalCount() }

// Latency is a summary of a histogram, in microseconds.
type Latency struct {
	Count  int64   `json:"count"`
	MinUs  float64 `json:"min_us"`
	MeanUs float64 `json:"mean_us"`
	P50Us  float64 `json:"p50_us"`
	P95Us  float64 `json:"p95_us"`
	P99Us  float64 `json:"p99_us"`
	P999Us float64 `json:"p999_us"`
	MaxUs  float64 `json:"max_us"`
	// Saturated is the number of operations that took longer than the histogram goes up to
	// and are counted as that long.
	Saturated int64 `json:"saturated,omitempty"`
}

// Summary returns the percentiles of the histogram.
func (h *Hist) Summary() Latency {
	if h.h.TotalCount() == 0 {
		return Latency{}
	}
	us := func(v int64) float64 { return round3(float64(v) / micro) }
	return Latency{
		Count:     h.h.TotalCount(),
		MinUs:     us(h.h.Min()),
		MeanUs:    round3(h.h.Mean() / micro),
		P50Us:     us(h.h.ValueAtQuantile(50)),
		P95Us:     us(h.h.ValueAtQuantile(95)),
		P99Us:     us(h.h.ValueAtQuantile(99)),
		P999Us:    us(h.h.ValueAtQuantile(99.9)),
		MaxUs:     us(h.h.Max()),
		Saturated: h.saturated,
	}
}

func round3(f float64) float64 {
	return math.Round(f*1000) / 1000
}
