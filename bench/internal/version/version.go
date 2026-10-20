// Package version says what is being benchmarked.
package version

import (
	"os"
	"runtime/debug"
)

// KvutilCommit is the commit of kvutil that the harness is built with, with "-dirty" after it if
// the tree had changes. The build sets it:
//
//	go build -ldflags "-X github.com/ehsanranjbar/kvutil/bench/internal/version.KvutilCommit=$(git rev-parse HEAD)"
var KvutilCommit = ""

// Kvutil returns the commit of kvutil that is benchmarked: the one the build set, or KVUTIL_COMMIT
// from the environment, or "unknown".
func Kvutil() string {
	if KvutilCommit != "" {
		return KvutilCommit
	}
	if c := os.Getenv("KVUTIL_COMMIT"); c != "" {
		return c
	}
	return "unknown"
}

// Module returns the version of a module that the program is built with.
func Module(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == path {
			if dep.Replace != nil {
				return dep.Replace.Version
			}
			return dep.Version
		}
	}
	return "unknown"
}
