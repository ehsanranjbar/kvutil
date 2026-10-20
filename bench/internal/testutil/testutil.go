// Package testutil has what the tests of the workloads share: a kvutil system on a temporary
// directory, and a way to run a workload and read its results back.
package testutil

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
)

// Params returns small parameters, for a run that takes a moment.
func Params(scale int) harness.Params {
	return harness.Params{
		Scale: scale, Values: dataset.Small, Durability: harness.Relaxed, Concurrency: 2,
		Duration: 60 * time.Millisecond, Warmup: 20 * time.Millisecond, Seed: 1,
	}
}

// Kvutil returns the system of kvutil on a directory that the test removes.
func Kvutil(t *testing.T) *kvsys.System {
	t.Helper()
	sys, err := kvsys.New(kvsys.Config{DataDir: t.TempDir(), Durability: harness.Relaxed, BlockCacheBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return sys
}

// Run runs a workload on a system and returns what it emitted.
func Run(t *testing.T, w harness.Workload, sys harness.System, p harness.Params) []metrics.Result {
	t.Helper()
	path := filepath.Join(t.TempDir(), "results.jsonl")
	sink, err := metrics.NewSink(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = harness.Execute(context.Background(), w, sys, p, sink, metrics.ReadHost(), nil)
	sink.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rs []metrics.Result
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r metrics.Result
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	return rs
}

// Verify verifies a workload on the system and returns the outputs and the differences among them.
func Verify(t *testing.T, w harness.Workload, sys harness.System, p harness.Params) ([]harness.Labeled, []string) {
	t.Helper()
	outs, diffs, err := harness.Verify(context.Background(), w, []harness.System{sys}, p)
	if err != nil {
		t.Fatal(err)
	}
	return outs, diffs
}
