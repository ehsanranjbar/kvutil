package metrics

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Usage is what the process has used so far.
type Usage struct {
	UserSec     float64
	SysSec      float64
	MaxRSSBytes int64
}

// ReadUsage reads the CPU time and the peak resident memory of the process.
func ReadUsage() Usage {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return Usage{}
	}
	rss := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024 // kilobytes everywhere but on macOS, where it is bytes
	}
	return Usage{
		UserSec:     float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6,
		SysSec:      float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6,
		MaxRSSBytes: rss,
	}
}

// cgroup is what the control group that the process runs in has used, when it can be read (in
// a Linux container, which includes the containers of Docker Desktop on macOS).
type cgroup struct {
	ok       bool
	cpuUsec  int64
	memBytes int64
	memPeak  int64
}

func readCgroup() cgroup {
	var c cgroup
	stat, err := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return c
	}
	for _, line := range strings.Split(string(stat), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "usage_usec" {
			c.cpuUsec, _ = strconv.ParseInt(f[1], 10, 64)
			c.ok = true
		}
	}
	c.memBytes = readInt("/sys/fs/cgroup/memory.current")
	c.memPeak = readInt("/sys/fs/cgroup/memory.peak")
	return c
}

func readInt(path string) int64 {
	bz, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(string(bz)), 10, 64)
	return n
}

// currentRSS reads the resident memory of the process now, which is only known on Linux.
func currentRSS() int64 {
	bz, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := strings.Fields(string(bz))
	if len(f) < 2 {
		return 0
	}
	pages, _ := strconv.ParseInt(f[1], 10, 64)
	return pages * int64(os.Getpagesize())
}

// Resources is what a measured interval used.
type Resources struct {
	WallSec    float64 `json:"wall_sec"`
	CPUUserSec float64 `json:"cpu_user_sec"`
	CPUSysSec  float64 `json:"cpu_sys_sec"`
	// CPUCores is the CPU time of the process over the wall time: the cores that it kept busy.
	CPUCores float64 `json:"cpu_cores"`
	// MaxRSSBytes is the peak resident memory of the process since it started.
	MaxRSSBytes int64 `json:"max_rss_bytes"`
	// RSSBytes is the resident memory of the process at the end, when the system tells it.
	RSSBytes int64 `json:"rss_bytes,omitempty"`
	// GoSysBytes is the memory that the Go runtime has from the system at the end.
	GoSysBytes uint64 `json:"go_sys_bytes"`
	// Cgroup* are the numbers of the container that the process runs in: its CPU time over the
	// interval, and its memory at the end (the page cache included) and at its peak.
	CgroupCPUCores     float64 `json:"cgroup_cpu_cores,omitempty"`
	CgroupMemBytes     int64   `json:"cgroup_mem_bytes,omitempty"`
	CgroupMemPeakBytes int64   `json:"cgroup_mem_peak_bytes,omitempty"`
}

// Probe measures the resources used between its start and its stop.
type Probe struct {
	start time.Time
	u0    Usage
	cg0   cgroup
}

// StartProbe starts measuring.
func StartProbe() *Probe {
	return &Probe{start: time.Now(), u0: ReadUsage(), cg0: readCgroup()}
}

// Stop returns what was used since the probe started.
func (p *Probe) Stop() Resources {
	wall := time.Since(p.start).Seconds()
	u := ReadUsage()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	r := Resources{
		WallSec:     round3(wall),
		CPUUserSec:  round3(u.UserSec - p.u0.UserSec),
		CPUSysSec:   round3(u.SysSec - p.u0.SysSec),
		MaxRSSBytes: u.MaxRSSBytes,
		RSSBytes:    currentRSS(),
		GoSysBytes:  ms.Sys,
	}
	if wall > 0 {
		r.CPUCores = round3((r.CPUUserSec + r.CPUSysSec) / wall)
	}
	if cg := readCgroup(); cg.ok && p.cg0.ok {
		if wall > 0 {
			r.CgroupCPUCores = round3(float64(cg.cpuUsec-p.cg0.cpuUsec) / 1e6 / wall)
		}
		r.CgroupMemBytes = cg.memBytes
		r.CgroupMemPeakBytes = cg.memPeak
	}
	return r
}
