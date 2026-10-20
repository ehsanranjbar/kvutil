package metrics

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Host describes where a run was made.
type Host struct {
	// Label names the machine: BENCH_HOST, which the scripts set to the name of the host.
	Label     string `json:"label"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Kernel    string `json:"kernel,omitempty"`
	NumCPU    int    `json:"num_cpu"`
	GoVersion string `json:"go_version"`
	GoMaxProc int    `json:"gomaxprocs"`
	// CPULimit and MemLimitBytes are the limits of the container that the harness runs in, when
	// it runs in one and the limits can be read; 0 is none.
	CPULimit      float64 `json:"cpu_limit,omitempty"`
	MemLimitBytes int64   `json:"mem_limit_bytes,omitempty"`
	// Docker is what the scripts tell of the Docker that runs the containers (BENCH_DOCKER_*).
	Docker map[string]string `json:"docker,omitempty"`
	// Env holds the limits that the containers were started with (BENCH_CPUS, BENCH_MEM).
	Env map[string]string `json:"env,omitempty"`
}

// ReadHost collects the description of the host.
func ReadHost() Host {
	h := Host{
		Label:     os.Getenv("BENCH_HOST"),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		GoVersion: runtime.Version(),
		GoMaxProc: runtime.GOMAXPROCS(0),
	}
	if h.Label == "" {
		h.Label, _ = os.Hostname()
	}
	if bz, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		h.Kernel = strings.TrimSpace(string(bz))
	}
	if bz, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		if f := strings.Fields(string(bz)); len(f) == 2 && f[0] != "max" {
			quota, _ := strconv.ParseFloat(f[0], 64)
			period, _ := strconv.ParseFloat(f[1], 64)
			if period > 0 {
				h.CPULimit = quota / period
			}
		}
	}
	if bz, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if s := strings.TrimSpace(string(bz)); s != "max" {
			h.MemLimitBytes, _ = strconv.ParseInt(s, 10, 64)
		}
	}

	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		switch {
		case strings.HasPrefix(k, "BENCH_DOCKER_"):
			if h.Docker == nil {
				h.Docker = map[string]string{}
			}
			h.Docker[strings.ToLower(strings.TrimPrefix(k, "BENCH_DOCKER_"))] = v
		case k == "BENCH_CPUS" || k == "BENCH_MEM":
			if h.Env == nil {
				h.Env = map[string]string{}
			}
			h.Env[strings.ToLower(strings.TrimPrefix(k, "BENCH_"))] = v
		}
	}
	return h
}
