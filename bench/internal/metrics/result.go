package metrics

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// SchemaVersion is the version of the format of a Result. It is raised when a field changes its
// meaning, so that whoever reads the files (the report) knows what it has.
const SchemaVersion = 1

// Params are the parameters that a measurement was made with.
type Params struct {
	Scale       int     `json:"scale"`
	Values      string  `json:"values"`
	Durability  string  `json:"durability"`
	Concurrency int     `json:"concurrency"`
	DurationSec float64 `json:"duration_sec,omitempty"`
	WarmupSec   float64 `json:"warmup_sec,omitempty"`
	Seed        uint64  `json:"seed"`
	// Case holds the parameters that only the case has, like a batch size.
	Case map[string]any `json:"case,omitempty"`
}

// Metrics are what was measured. A field that is zero is left out when it does not apply to the
// kind of measurement: Ops and Latency are of the operations of a timed run, Records and
// RecordsPerSec of a phase that loads or builds.
type Metrics struct {
	// Ops is the number of operations that finished without an error in the measured interval.
	Ops int64 `json:"ops,omitempty"`
	// Errors is the number of operations that failed. Retries is the number of times that a
	// conflicting transaction was run again (kvutil), which is not an error.
	Errors      int64    `json:"errors"`
	Retries     int64    `json:"retries"`
	ErrorSample []string `json:"error_sample,omitempty"`
	// ElapsedSec is the length of the measured interval, without the warm-up.
	ElapsedSec float64 `json:"elapsed_sec"`
	OpsPerSec  float64 `json:"ops_per_sec,omitempty"`
	// Units is what the operations did in units other than themselves, like the rows that a
	// scan read, and UnitsPerSec the rate of that.
	Units       int64   `json:"units,omitempty"`
	UnitName    string  `json:"unit_name,omitempty"`
	UnitsPerSec float64 `json:"units_per_sec,omitempty"`

	Latency *Latency `json:"latency,omitempty"`

	Records       int64   `json:"records,omitempty"`
	RecordsPerSec float64 `json:"records_per_sec,omitempty"`

	Resources Resources `json:"resources"`

	// DiskBytes is the size of the data of the database after the measurement: the blocks of
	// the directory of Badger, the relations of PostgreSQL with their indexes and TOAST tables.
	// WALBytes is the write-ahead log of PostgreSQL, which Badger keeps in its own directory.
	DiskBytes int64 `json:"disk_bytes,omitempty"`
	WALBytes  int64 `json:"wal_bytes,omitempty"`

	// Extra is for the numbers that only some measurements have.
	Extra map[string]float64 `json:"extra,omitempty"`
	// Info is for the text that only some measurements have, like the plan that a database
	// chose for a query (plan, plan_nodes, plans_seen).
	Info map[string]string `json:"info,omitempty"`
}

// Result is one line of a results file: a measurement of a case of a workload.
type Result struct {
	Schema        int       `json:"schema"`
	Start         time.Time `json:"start"`
	End           time.Time `json:"end"`
	System        string    `json:"system"`
	SystemVersion string    `json:"system_version"`
	// Settings are the settings of the system that matter for the numbers.
	Settings map[string]string `json:"settings,omitempty"`
	Workload string            `json:"workload"`
	Variant  string            `json:"variant"`
	Case     string            `json:"case"`
	Params   Params            `json:"params"`
	Host     Host              `json:"host"`
	Metrics  Metrics           `json:"metrics"`
}

// Sink writes results as JSON lines, and says what it wrote on a log.
type Sink struct {
	mu   sync.Mutex
	file io.WriteCloser
	log  io.Writer
}

// NewSink opens a file that results are appended to; an empty path writes none. The log gets a
// line for every result; a nil log gets none.
func NewSink(path string, log io.Writer) (*Sink, error) {
	s := &Sink{log: log}
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		s.file = f
	}
	return s, nil
}

// Write writes a result.
func (s *Sink) Write(r *Result) error {
	r.Schema = SchemaVersion
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		bz, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := s.file.Write(append(bz, '\n')); err != nil {
			return err
		}
	}
	if s.log != nil {
		fmt.Fprintln(s.log, Summary(r))
	}
	return nil
}

// Close closes the file.
func (s *Sink) Close() error {
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

// Summary is a result in one line for a person to read.
func Summary(r *Result) string {
	m := r.Metrics
	head := fmt.Sprintf("%-8s %-17s %-13s %-12s c=%-2d", r.System, r.Workload, r.Variant, r.Case, r.Params.Concurrency)
	switch {
	case m.Latency != nil:
		s := fmt.Sprintf("%s %10.0f ops/s  p50=%s p99=%s p999=%s max=%s  err=%d retry=%d",
			head, m.OpsPerSec, us(m.Latency.P50Us), us(m.Latency.P99Us), us(m.Latency.P999Us), us(m.Latency.MaxUs),
			m.Errors, m.Retries)
		if m.UnitsPerSec > 0 {
			s += fmt.Sprintf("  %.0f %s/s", m.UnitsPerSec, m.UnitName)
		}
		if plan := m.Info["plan_nodes"]; plan != "" {
			s += "  plan=" + plan
		}
		return s
	case m.Records > 0:
		return fmt.Sprintf("%s %10.0f rec/s   %d records in %.2fs  cpu=%.2f cores  disk=%s",
			head, m.RecordsPerSec, m.Records, m.ElapsedSec, m.Resources.CPUCores, bytes(m.DiskBytes))
	case len(m.Extra) > 0:
		keys := make([]string, 0, len(m.Extra))
		for k := range m.Extra {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		s := head
		for _, k := range keys {
			s += fmt.Sprintf(" %s=%.1f", k, m.Extra[k])
		}
		return s
	default:
		return fmt.Sprintf("%s %.2fs  err=%d", head, m.ElapsedSec, m.Errors)
	}
}

func us(v float64) string {
	switch {
	case v >= 1000000:
		return fmt.Sprintf("%.2fs", v/1e6)
	case v >= 1000:
		return fmt.Sprintf("%.2fms", v/1e3)
	}
	return fmt.Sprintf("%.1fus", v)
}

func bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
