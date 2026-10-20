package metrics

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func near(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	if math.Abs(got-want) > want*tolerance {
		t.Errorf("%s = %v, want %v (+-%.1f%%)", name, got, want, tolerance*100)
	}
}

func TestHistPercentiles(t *testing.T) {
	h := NewHist()
	for i := 1; i <= 10000; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}
	s := h.Summary()
	if s.Count != 10000 {
		t.Fatalf("count = %d", s.Count)
	}
	near(t, "min", s.MinUs, 1, 0.01)
	near(t, "mean", s.MeanUs, 5000.5, 0.01)
	near(t, "p50", s.P50Us, 5000, 0.01)
	near(t, "p95", s.P95Us, 9500, 0.01)
	near(t, "p99", s.P99Us, 9900, 0.01)
	near(t, "p999", s.P999Us, 9990, 0.01)
	near(t, "max", s.MaxUs, 10000, 0.01)
}

func TestHistMerge(t *testing.T) {
	a, b := NewHist(), NewHist()
	for i := 0; i < 900; i++ {
		a.Record(10 * time.Microsecond)
	}
	for i := 0; i < 100; i++ {
		b.Record(1000 * time.Microsecond)
	}
	a.Merge(b)
	s := a.Summary()
	if s.Count != 1000 {
		t.Fatalf("count = %d", s.Count)
	}
	near(t, "p50", s.P50Us, 10, 0.01)
	near(t, "p95", s.P95Us, 1000, 0.01)
	near(t, "max", s.MaxUs, 1000, 0.01)
}

func TestHistSaturatesAndClamps(t *testing.T) {
	h := NewHist()
	h.Record(0)
	h.Record(10 * time.Minute)
	s := h.Summary()
	if s.Count != 2 || s.Saturated != 1 {
		t.Fatalf("count = %d, saturated = %d", s.Count, s.Saturated)
	}
	near(t, "max", s.MaxUs, 120e6, 0.01)
}

func TestEmptyHist(t *testing.T) {
	if s := NewHist().Summary(); s != (Latency{}) {
		t.Errorf("an empty histogram summarises to %+v", s)
	}
}

func TestSinkWritesJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.jsonl")
	var log strings.Builder
	sink, err := NewSink(path, &log)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHist()
	h.Record(250 * time.Microsecond)
	lat := h.Summary()
	r := &Result{
		Start: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), End: time.Date(2026, 1, 2, 3, 4, 15, 0, time.UTC),
		System: "kvutil", SystemVersion: "kvutil abc", Workload: "kv-basic", Variant: "typed", Case: "get",
		Params:  Params{Scale: 1000, Values: "small", Durability: "durable", Concurrency: 4, DurationSec: 10, Seed: 1, Case: map[string]any{"rows": 100}},
		Host:    Host{Label: "box", OS: "linux", NumCPU: 4},
		Metrics: Metrics{Ops: 1000, OpsPerSec: 100, ElapsedSec: 10, Latency: &lat, Retries: 2, DiskBytes: 4096},
	}
	for i := 0; i < 2; i++ {
		if err := sink.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		lines++
		var got Result
		if err := json.Unmarshal(sc.Bytes(), &got); err != nil {
			t.Fatalf("line %d is not JSON: %v", lines, err)
		}
		if got.Schema != SchemaVersion || got.System != "kvutil" || got.Case != "get" || got.Params.Concurrency != 4 ||
			got.Metrics.Latency == nil || got.Metrics.Latency.P50Us != lat.P50Us || got.Metrics.Retries != 2 ||
			got.Host.Label != "box" || got.Params.Case["rows"] != float64(100) || !got.Start.Equal(r.Start) {
			t.Fatalf("line %d does not round trip: %+v", lines, got)
		}
	}
	if lines != 2 {
		t.Fatalf("%d lines written, want 2", lines)
	}
	if n := strings.Count(log.String(), "\n"); n != 2 || !strings.Contains(log.String(), "ops/s") {
		t.Errorf("the log has %d lines: %q", n, log.String())
	}
}

func TestSinkWithoutFile(t *testing.T) {
	sink, err := NewSink("", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(&Result{}); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSummaryOfEachKind(t *testing.T) {
	lat := Latency{Count: 1, P50Us: 1500, P99Us: 2, P999Us: 3, MaxUs: 4}
	timed := Summary(&Result{System: "x", Metrics: Metrics{OpsPerSec: 10, Latency: &lat, Units: 5, UnitName: "rows", UnitsPerSec: 7}})
	if !strings.Contains(timed, "1.50ms") || !strings.Contains(timed, "7 rows/s") {
		t.Errorf("timed: %q", timed)
	}
	load := Summary(&Result{System: "x", Metrics: Metrics{Records: 100, RecordsPerSec: 50, ElapsedSec: 2, DiskBytes: 3 << 20}})
	if !strings.Contains(load, "50 rec/s") || !strings.Contains(load, "3.0MiB") {
		t.Errorf("load: %q", load)
	}
}

func TestProbeMeasuresCPU(t *testing.T) {
	p := StartProbe()
	deadline := time.Now().Add(50 * time.Millisecond)
	for x := 0; time.Now().Before(deadline); x++ {
	}
	r := p.Stop()
	if r.WallSec < 0.05 || r.CPUUserSec+r.CPUSysSec < 0.02 || r.CPUCores <= 0.3 {
		t.Errorf("a busy loop of 50 ms is measured as %+v", r)
	}
	if r.MaxRSSBytes < 1<<20 {
		t.Errorf("max RSS = %d", r.MaxRSSBytes)
	}
}
