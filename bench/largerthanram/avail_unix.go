//go:build linux || darwin

package main

import (
	"os"
	"strconv"
	"strings"
)

// availableMemory reports the memory this process may actually use, preferring a
// cgroup limit when one is set — that is the number that decides whether a store is
// "larger than RAM" for this run, not the host's physical total.
func availableMemory() int64 {
	// cgroup v2, which is what a container or systemd scope sets.
	for _, p := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(b))
		if s == "max" {
			continue
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 0 && n < 1<<50 {
			return n
		}
	}
	return hostMemory()
}
