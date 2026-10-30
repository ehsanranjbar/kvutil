// Package report turns the result directories of run.sh into a report: Markdown, a self-contained
// HTML page with SVG charts, and CSV files with the data behind the tables. It presents numbers; it
// draws no conclusions.
package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
)

// Meta is meta.json, which run.sh writes.
type Meta struct {
	Host         string `json:"host"`
	Started      string `json:"started"`
	KvutilCommit string `json:"kvutil_commit"`
	Args         struct {
		Workload    string `json:"workload"`
		Scale       int    `json:"scale"`
		Values      string `json:"values"`
		Durability  string `json:"durability"`
		Concurrency int    `json:"concurrency"`
		Duration    string `json:"duration"`
		Warmup      string `json:"warmup"`
		Systems     string `json:"systems"`
		Seed        int    `json:"seed"`
		PGService   string `json:"pg_service"`
	} `json:"args"`
	Limits map[string]string `json:"limits"`
	Docker struct {
		Server   string  `json:"server"`
		OS       string  `json:"os"`
		NCPU     float64 `json:"ncpu"`
		MemTotal float64 `json:"mem_total"`
		Kernel   string  `json:"kernel"`
	} `json:"docker"`
}

// Sample is a row of docker-stats-*.csv. A figure that docker did not give is NaN.
type Sample struct {
	At       time.Time
	Name     string
	CPUCores float64 // docker's CPU % of a core, over 100
	MemBytes float64
}

// Verify is what kvbench verify said.
type Verify struct {
	Present bool
	Passed  bool
	Lines   []string
}

// Run is a result directory.
type Run struct {
	Name    string // the unique label of the run in the report
	Dir     string
	Meta    Meta
	HasMeta bool
	Results []*metrics.Result
	Stats   []Sample
	Images  []string
	Verify  Verify
	// Skipped counts the lines of the results files that could not be read.
	Skipped int
	// Replaced counts the results that a later result of the same case replaced: the results
	// files are appended to, so a case that was run again is there twice.
	Replaced int
}

// LoadRun reads a result directory. A directory without results is an error.
func LoadRun(dir string) (*Run, error) {
	run := &Run{Dir: dir, Name: filepath.Base(filepath.Clean(dir))}

	files, _ := filepath.Glob(filepath.Join(dir, "results-*.jsonl"))
	sort.Strings(files)
	for _, f := range files {
		rs, skipped, err := readResults(f)
		if err != nil {
			return nil, err
		}
		run.Results = append(run.Results, rs...)
		run.Skipped += skipped
	}
	if len(run.Results) == 0 {
		return nil, fmt.Errorf("%s: no results (results-*.jsonl)", dir)
	}
	run.Results, run.Replaced = keepLast(run.Results)

	if bz, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		run.HasMeta = json.Unmarshal(bz, &run.Meta) == nil
	}

	stats, _ := filepath.Glob(filepath.Join(dir, "docker-stats-*.csv"))
	sort.Strings(stats)
	for _, f := range stats {
		ss, err := readStats(f)
		if err != nil {
			return nil, err
		}
		run.Stats = append(run.Stats, ss...)
	}

	if bz, err := os.ReadFile(filepath.Join(dir, "images.txt")); err == nil {
		for _, l := range strings.Split(string(bz), "\n") {
			if l = strings.TrimSpace(l); l != "" {
				run.Images = append(run.Images, l)
			}
		}
	}
	if bz, err := os.ReadFile(filepath.Join(dir, "verify.txt")); err == nil {
		run.Verify = parseVerify(string(bz))
	}
	return run, nil
}

// keepLast drops each result that a later one of the same durability, system, workload, variant,
// case and workers replaces, and says how many it dropped. The parameters of the case are no part
// of the identity: in the results of a full run, those six make a result unique.
func keepLast(rs []*metrics.Result) ([]*metrics.Result, int) {
	key := func(r *metrics.Result) compKey {
		return compKey{r.Workload, r.System, r.Variant, r.Case, r.Params.Durability, r.Params.Concurrency}
	}
	last := map[compKey]int{}
	for i, r := range rs {
		last[key(r)] = i
	}
	if len(last) == len(rs) {
		return rs, 0
	}
	out := make([]*metrics.Result, 0, len(last))
	for i, r := range rs {
		if last[key(r)] == i {
			out = append(out, r)
		}
	}
	return out, len(rs) - len(out)
}

func readResults(path string) ([]*metrics.Result, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	var out []*metrics.Result
	skipped := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var r metrics.Result
		if err := json.Unmarshal([]byte(line), &r); err != nil || r.Workload == "" {
			skipped++
			continue
		}
		out = append(out, &r)
	}
	return out, skipped, sc.Err()
}

func readStats(path string) ([]Sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Sample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if s, ok := parseSample(sc.Text()); ok {
			out = append(out, s)
		}
	}
	return out, sc.Err()
}

// parseSample reads a line of docker-stats-*.csv: ts,name,cpu_perc,mem_usage,mem_perc,... The
// header and the lines that are cut off are not samples.
func parseSample(line string) (Sample, bool) {
	f := strings.Split(strings.TrimSpace(line), ",")
	if len(f) < 4 {
		return Sample{}, false
	}
	at, err := time.Parse(time.RFC3339, f[0])
	if err != nil {
		return Sample{}, false
	}
	s := Sample{At: at, Name: f[1], CPUCores: math.NaN(), MemBytes: math.NaN()}
	if p, err := strconv.ParseFloat(strings.TrimSuffix(f[2], "%"), 64); err == nil {
		s.CPUCores = p / 100
	}
	used, _, _ := strings.Cut(f[3], "/")
	if b, ok := parseSize(strings.TrimSpace(used)); ok {
		s.MemBytes = b
	}
	return s, true
}

var sizeUnits = []struct {
	suffix string
	mult   float64
}{
	{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
	{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"kB", 1e3}, {"KB", 1e3}, {"B", 1},
}

// parseSize reads a size of docker stats, like 132.6MiB or 1.93MB.
func parseSize(s string) (float64, bool) {
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), 64)
			return v * u.mult, err == nil
		}
	}
	return 0, false
}

func parseVerify(text string) Verify {
	v := Verify{Present: true}
	ok := 0
	bad := false
	for _, l := range strings.Split(text, "\n") {
		l = strings.TrimRight(l, " \t\r")
		switch {
		case strings.HasPrefix(l, "OK "):
			ok++
		case strings.HasPrefix(l, "DIFFER"), strings.HasPrefix(l, "ERROR"), strings.Contains(l, "verification failed"):
			bad = true
		}
		if l != "" {
			v.Lines = append(v.Lines, l)
		}
	}
	v.Passed = ok > 0 && !bad
	return v
}
