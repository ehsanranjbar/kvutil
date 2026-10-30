// Package tssys is TimescaleDB as a system of the benchmarks: PostgreSQL with the extension, reached
// by the same driver and pool as pgsys. It is a different type, so that a workload can have variants
// for it and not for PostgreSQL, and the results name it "timescale" and say which version of the
// extension ran.
package tssys

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

// System is a PostgreSQL server that has the timescaledb extension.
type System struct {
	*pgsys.System
	extVersion string
	settings   map[string]string
}

// timescaleSettings are the settings of the server that matter for the jobs of the extension.
var timescaleSettings = []string{"timescaledb.max_background_workers", "max_worker_processes", "timescaledb.license"}

// Open connects, creates the extension in the database if it is not there, and reads its version.
func Open(ctx context.Context, cfg pgsys.Config) (*System, error) {
	base, err := pgsys.Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &System{System: base, settings: map[string]string{}}
	for k, v := range base.Settings() {
		s.settings[k] = v
	}
	pool := base.Pool()
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb"); err != nil {
		base.Close()
		return nil, fmt.Errorf("timescale: CREATE EXTENSION timescaledb: %w", err)
	}
	if err := pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&s.extVersion); err != nil {
		base.Close()
		return nil, fmt.Errorf("timescale: the version of the extension: %w", err)
	}
	s.settings["timescaledb"] = s.extVersion
	for _, name := range timescaleSettings {
		var v string
		if err := pool.QueryRow(ctx, "SELECT current_setting($1, true)", name).Scan(&v); err == nil {
			s.settings[name] = v
		}
	}
	return s, nil
}

// Name implements harness.System.
func (s *System) Name() string { return "timescale" }

// Version implements harness.System: the server, and the version of the extension.
func (s *System) Version() string {
	return s.System.Version() + "; timescaledb " + s.extVersion
}

// Settings implements harness.System.
func (s *System) Settings() map[string]string { return s.settings }
